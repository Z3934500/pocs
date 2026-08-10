package com.poc.order.service;

import com.poc.contracts.CapturePaymentCommand;
import com.poc.contracts.InventoryReservationResponse;
import com.poc.contracts.InventoryReservationResult;
import com.poc.contracts.OrderStatus;
import com.poc.contracts.PaymentResponse;
import com.poc.contracts.PaymentStatus;
import com.poc.contracts.RefundPaymentCommand;
import com.poc.contracts.ReservationStatus;
import com.poc.contracts.ReserveInventoryCommand;
import com.poc.order.dto.CreateOrderRequest;
import com.poc.order.entity.OrderAggregate;
import com.poc.order.gateway.InventoryGateway;
import com.poc.order.gateway.PaymentGatewayClient;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/**
 * Saga orchestrator for the Order bounded context.
 *
 * <p>The default path is synchronous reserve -> capture -> commit. When
 * {@code inventory.reservation.mode=sqs-fifo}, the order is durably queued by
 * the Order outbox and this service resumes the same Saga when the inventory
 * result consumer receives a response.</p>
 */
@Service
public class OrderWorkflowService {

    private final OrderStateService stateService;
    private final InventoryGateway inventoryGateway;
    private final PaymentGatewayClient paymentGateway;
    private final MeterRegistry meterRegistry;

    @Value("${inventory.reservation.mode:sync}")
    private String reservationMode;

    public OrderWorkflowService(
            OrderStateService stateService,
            InventoryGateway inventoryGateway,
            PaymentGatewayClient paymentGateway,
            MeterRegistry meterRegistry) {
        this.stateService = stateService;
        this.inventoryGateway = inventoryGateway;
        this.paymentGateway = paymentGateway;
        this.meterRegistry = meterRegistry;
    }

    /** Runs reserve -> capture -> commit, or queues the reservation command. */
    public OrderAggregate placeOrder(
            CreateOrderRequest request, String headerIdempotencyKey) {
        if (request == null) {
            throw new IllegalArgumentException("order request is required");
        }

        String orderKey = headerIdempotencyKey == null || headerIdempotencyKey.isBlank()
                ? request.idempotencyKey()
                : headerIdempotencyKey;
        OrderAggregate order = stateService.createPending(request, orderKey);
        if (order.getStatus() != OrderStatus.PENDING) {
            return order;
        }
        if (isFifoReservationMode()) {
            increment("order.saga.reservation_queued");
            return order;
        }

        InventoryReservationResponse reservation;
        try {
            reservation = inventoryGateway.reserve(new ReserveInventoryCommand(
                    order.getOrderId(),
                    order.getSku(),
                    order.getQty(),
                    orderKey + ":inventory",
                    request.reservationTtlMinutes()));
        } catch (RuntimeException exception) {
            increment("order.saga.reservation_failed");
            return stateService.markReservationFailed(
                    order.getOrderId(), exception.getMessage());
        }
        if (reservation == null || reservation.status() != ReservationStatus.RESERVED) {
            return stateService.markReservationFailed(
                    order.getOrderId(), "inventory did not reserve stock");
        }
        return finishAfterReservation(
                order,
                reservation,
                paymentKey(request, order),
                request.providerRef(),
                request.paymentSucceed() == null || request.paymentSucceed());
    }

    /** Resumes the Saga after a queued inventory command has been applied. */
    public OrderAggregate onReservationResult(InventoryReservationResult result) {
        if (result == null || result.orderId() == null || result.orderId().isBlank()) {
            throw new IllegalArgumentException("inventory reservation result is required");
        }
        OrderAggregate order = stateService.get(result.orderId());
        if (order.getStatus() != OrderStatus.RESERVATION_QUEUED) {
            return order;
        }
        if (result.status() != ReservationStatus.RESERVED) {
            increment("order.saga.reservation_failed");
            return stateService.markReservationFailed(order.getOrderId(), result.error());
        }

        InventoryReservationResponse reservation = new InventoryReservationResponse(
                result.orderId(),
                result.reservationId(),
                result.sku(),
                result.qty(),
                result.status());
        return finishAfterReservation(
                order,
                reservation,
                order.getPaymentIdempotencyKey() == null
                        ? order.getOrderId() + ":payment"
                        : order.getPaymentIdempotencyKey(),
                order.getProviderRef(),
                order.isPaymentSucceed());
    }

    /** Cancels a pending order and sends a queued cancellation when necessary. */
    public OrderAggregate cancel(String orderId, String reason) {
        OrderAggregate order = stateService.get(orderId);
        if (order.getStatus() == OrderStatus.RESERVATION_QUEUED) {
            return stateService.cancelQueued(orderId, reason == null ? "cancelled" : reason);
        }
        if (order.getStatus() != OrderStatus.PENDING) {
            return order;
        }
        if (!tryRelease(orderId)) {
            return stateService.markCompensationFailed(orderId, "cancel release failed");
        }
        return stateService.markCancelled(
                orderId, reason == null ? "customer cancelled" : reason);
    }

    public OrderAggregate get(String orderId) {
        return stateService.get(orderId);
    }

    private OrderAggregate finishAfterReservation(
            OrderAggregate order,
            InventoryReservationResponse reservation,
            String paymentKey,
            String providerRef,
            boolean paymentSucceed) {
        PaymentResponse payment;
        try {
            payment = paymentGateway.capture(new CapturePaymentCommand(
                    order.getOrderId(),
                    paymentKey,
                    providerRef,
                    order.getAmountCents(),
                    order.getCurrency(),
                    paymentSucceed));
        } catch (RuntimeException exception) {
            // A timeout is not a payment failure. Resolve the idempotency key first.
            payment = queryPaymentAfterTimeout(order.getOrderId(), paymentKey);
            if (payment == null) {
                increment("order.saga.payment_unknown");
                return stateService.markPaymentUnknown(
                        order.getOrderId(), "payment outcome requires reconciliation");
            }
        }
        if (payment == null || payment.status() != PaymentStatus.CAPTURED) {
            return releaseAfterPaymentFailure(order, "payment failed");
        }

        try {
            InventoryReservationResponse committed = inventoryGateway.commit(order.getOrderId());
            if (committed == null || committed.status() != ReservationStatus.COMMITTED) {
                throw new IllegalStateException("inventory commit was not confirmed");
            }
            increment("order.saga.confirmed");
            return stateService.markConfirmed(
                    order.getOrderId(), reservation.reservationId(), payment.paymentId());
        } catch (RuntimeException commitFailure) {
            boolean refundSucceeded = tryRefund(payment.paymentId(), order.getOrderId());
            boolean releaseSucceeded = tryRelease(order.getOrderId());
            if (refundSucceeded && releaseSucceeded) {
                return stateService.markCompensated(
                        order.getOrderId(),
                        "inventory commit failed; payment refunded and stock released");
            }
            return stateService.markCompensationFailed(
                    order.getOrderId(),
                    "inventory commit failed and compensation requires retry");
        }
    }

    private OrderAggregate releaseAfterPaymentFailure(
            OrderAggregate order, String reason) {
        if (!tryRelease(order.getOrderId())) {
            return stateService.markCompensationFailed(
                    order.getOrderId(),
                    "payment failed but inventory release requires retry");
        }
        return stateService.markPaymentFailed(order.getOrderId(), reason);
    }

    private PaymentResponse queryPaymentAfterTimeout(String orderId, String idempotencyKey) {
        try {
            return paymentGateway.query(orderId, idempotencyKey);
        } catch (RuntimeException queryFailure) {
            increment("order.saga.payment_query_failed");
            return null;
        }
    }

    private boolean tryRelease(String orderId) {
        try {
            inventoryGateway.release(orderId, "order saga compensation");
            return true;
        } catch (RuntimeException exception) {
            increment("order.saga.release_failed");
            return false;
        }
    }

    private boolean tryRefund(String paymentId, String orderId) {
        try {
            return paymentGateway.refund(new RefundPaymentCommand(
                            paymentId, orderId + ":refund"))
                    .status() == PaymentStatus.REFUNDED;
        } catch (RuntimeException exception) {
            increment("order.saga.refund_failed");
            return false;
        }
    }

    private String paymentKey(CreateOrderRequest request, OrderAggregate order) {
        return request.paymentIdempotencyKey() == null
                || request.paymentIdempotencyKey().isBlank()
                ? order.getOrderId() + ":payment"
                : request.paymentIdempotencyKey();
    }

    private boolean isFifoReservationMode() {
        return "sqs-fifo".equalsIgnoreCase(reservationMode);
    }

    private void increment(String metricName) {
        Counter.builder(metricName)
                .tag("service", "order")
                .register(meterRegistry)
                .increment();
        Counter.builder("oms_business_operations")
                .tag("service", "order")
                .tag("operation", metricName)
                .tag("outcome", metricName.contains("failed") || metricName.contains("unknown") ? "failure" : "success")
                .register(meterRegistry)
                .increment();
    }
}
