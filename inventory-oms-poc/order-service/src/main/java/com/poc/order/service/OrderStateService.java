package com.poc.order.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.poc.contracts.InventoryReservationQueueCommand;
import com.poc.contracts.OrderStatus;
import com.poc.contracts.ReserveInventoryCommand;
import com.poc.order.dto.CreateOrderRequest;
import com.poc.order.entity.OrderAggregate;
import com.poc.order.entity.OrderOutboxEvent;
import com.poc.order.entity.OrderSagaStep;
import com.poc.order.repository.OrderOutboxRepository;
import com.poc.order.repository.OrderRepository;
import com.poc.order.repository.OrderSagaStepRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.util.Map;

/** Owns local Order database transactions; it does not call remote services. */
@Service
public class OrderStateService {

    private final OrderRepository orderRepository;
    private final OrderOutboxRepository outboxRepository;
    private final OrderSagaStepRepository sagaRepository;
    private final ObjectMapper objectMapper;

    private final String reservationMode;

    public OrderStateService(
            OrderRepository orderRepository,
            OrderOutboxRepository outboxRepository,
            OrderSagaStepRepository sagaRepository,
            ObjectMapper objectMapper,
            org.springframework.core.env.Environment environment) {
        this.orderRepository = orderRepository;
        this.outboxRepository = outboxRepository;
        this.sagaRepository = sagaRepository;
        this.objectMapper = objectMapper;
        this.reservationMode = environment.getProperty("inventory.reservation.mode", "sync");
    }

    @Transactional
    public OrderAggregate createPending(CreateOrderRequest request, String idempotencyKey) {
        validate(request, idempotencyKey);
        OrderAggregate existing = orderRepository.findByIdempotencyKey(idempotencyKey)
                .orElse(null);
        if (existing != null) {
            ensureSameRequest(existing, request);
            return existing;
        }
        if (orderRepository.existsById(request.orderId())) {
            throw new IllegalStateException("order already exists: " + request.orderId());
        }

        String currency = normalizeCurrency(request.currency());
        OrderAggregate order = new OrderAggregate(
                request.orderId(),
                request.sku(),
                request.qty(),
                request.amountCents(),
                currency,
                idempotencyKey);
        order.setPaymentIdempotencyKey(
                request.paymentIdempotencyKey() == null || request.paymentIdempotencyKey().isBlank()
                        ? request.orderId() + ":payment"
                        : request.paymentIdempotencyKey());
        order.setProviderRef(request.providerRef());
        order.setPaymentSucceed(request.paymentSucceed());
        if (isFifoReservationMode()) {
            order.setStatus(OrderStatus.RESERVATION_QUEUED);
        }
        orderRepository.save(order);
        sagaRepository.save(new OrderSagaStep(
                order.getOrderId(),
                "create_order",
                "COMPLETED",
                isFifoReservationMode() ? "order accepted; inventory reservation queued" : "order accepted"));
        outboxRepository.save(new OrderOutboxEvent(
                order.getOrderId(),
                "order.created",
                "{\"orderId\":\"" + order.getOrderId() + "\"}"));
        if (isFifoReservationMode()) {
            ReserveInventoryCommand reserve = new ReserveInventoryCommand(
                    order.getOrderId(),
                    order.getSku(),
                    order.getQty(),
                    idempotencyKey + ":inventory",
                    request.reservationTtlMinutes());
            outboxRepository.save(new OrderOutboxEvent(
                    order.getOrderId(),
                    "inventory.reservation.requested",
                    serialize(InventoryReservationQueueCommand.reserve(reserve)),
                    order.getSku()));
        }
        return order;
    }

    @Transactional
    public OrderAggregate markReservationFailed(String orderId, String detail) {
        return transition(
                orderId,
                OrderStatus.RESERVATION_FAILED,
                "reserve_inventory",
                "FAILED",
                detail,
                "order.reservation_failed");
    }

    @Transactional
    public OrderAggregate markPaymentFailed(String orderId, String detail) {
        return transition(
                orderId,
                OrderStatus.PAYMENT_FAILED,
                "capture_payment",
                "COMPENSATED",
                detail,
                "order.payment_failed");
    }

    @Transactional
    public OrderAggregate markPaymentUnknown(String orderId, String detail) {
        return transition(
                orderId,
                OrderStatus.PAYMENT_UNKNOWN,
                "capture_payment",
                "UNKNOWN",
                detail,
                "order.payment_unknown");
    }

    @Transactional
    public OrderAggregate markConfirmed(
            String orderId, String reservationId, String paymentId) {
        OrderAggregate order = get(orderId);
        order.setReservationId(reservationId);
        order.setPaymentId(paymentId);
        order.setStatus(OrderStatus.CONFIRMED);
        orderRepository.save(order);
        sagaRepository.save(new OrderSagaStep(
                orderId,
                "confirm_order",
                "COMPLETED",
                "inventory and payment committed"));
        outboxRepository.save(new OrderOutboxEvent(
                orderId,
                "order.confirmed",
                "{\"orderId\":\"" + orderId + "\"}"));
        return order;
    }

    @Transactional
    public OrderAggregate markCompensated(String orderId, String detail) {
        return transition(
                orderId,
                OrderStatus.COMPENSATED,
                "compensate_order",
                "COMPLETED",
                detail,
                "order.compensated");
    }

    @Transactional
    public OrderAggregate markCompensationFailed(String orderId, String detail) {
        return transition(
                orderId,
                OrderStatus.COMPENSATION_FAILED,
                "compensate_order",
                "FAILED",
                detail,
                "order.compensation_failed");
    }

    @Transactional
    public OrderAggregate markCancelled(String orderId, String detail) {
        return transition(
                orderId,
                OrderStatus.CANCELLED,
                "cancel_order",
                "COMPLETED",
                detail,
                "order.cancelled");
    }

    /** Cancels a request that is waiting in the FIFO inventory queue. */
    @Transactional
    public OrderAggregate cancelQueued(String orderId, String detail) {
        OrderAggregate order = get(orderId);
        if (order.getStatus() != OrderStatus.RESERVATION_QUEUED) {
            return order;
        }
        order.setStatus(OrderStatus.CANCELLED);
        orderRepository.save(order);
        sagaRepository.save(new OrderSagaStep(
                orderId, "cancel_order", "COMPLETED", detail == null ? "cancelled" : detail));
        outboxRepository.save(new OrderOutboxEvent(
                orderId,
                "order.cancelled",
                serialize(Map.of("orderId", orderId, "status", "CANCELLED"))));
        outboxRepository.save(new OrderOutboxEvent(
                orderId,
                "inventory.reservation.cancel_requested",
                serialize(InventoryReservationQueueCommand.cancel(orderId, order.getSku())),
                order.getSku()));
        return order;
    }

    @Transactional(readOnly = true)
    public OrderAggregate get(String orderId) {
        return orderRepository.findById(orderId)
                .orElseThrow(() -> new IllegalArgumentException(
                        "order not found: " + orderId));
    }

    private OrderAggregate transition(
            String orderId,
            OrderStatus status,
            String step,
            String stepStatus,
            String detail,
            String eventType) {
        OrderAggregate order = get(orderId);
        order.setStatus(status);
        orderRepository.save(order);
        sagaRepository.save(new OrderSagaStep(orderId, step, stepStatus, detail));
        outboxRepository.save(new OrderOutboxEvent(
                orderId,
                eventType,
                "{\"orderId\":\"" + orderId
                        + "\",\"status\":\"" + status + "\"}"));
        return order;
    }

    private static void validate(CreateOrderRequest request, String key) {
        if (request == null) {
            throw new IllegalArgumentException("order request is required");
        }
        requireText(request.orderId(), "orderId");
        requireText(request.sku(), "sku");
        requireText(key, "idempotencyKey");
        if (request.qty() <= 0
                || request.amountCents() <= 0
                || request.reservationTtlMinutes() <= 0) {
            throw new IllegalArgumentException(
                    "qty, amountCents and reservationTtlMinutes must be positive");
        }
    }

    private static void ensureSameRequest(
            OrderAggregate existing, CreateOrderRequest request) {
        boolean differentOrder = !existing.getOrderId().equals(request.orderId());
        boolean differentSku = !existing.getSku().equals(request.sku());
        boolean differentQuantity = existing.getQty() != request.qty();
        boolean differentAmount = existing.getAmountCents() != request.amountCents();
        boolean differentCurrency = !existing.getCurrency()
                .equals(normalizeCurrency(request.currency()));
        if (differentOrder || differentSku || differentQuantity
                || differentAmount || differentCurrency) {
            throw new IllegalStateException(
                    "order idempotency key was reused with different arguments");
        }
    }

    private static String normalizeCurrency(String currency) {
        String normalized = currency == null || currency.isBlank()
                ? "CNY"
                : currency.trim().toUpperCase();
        if (normalized.length() != 3) {
            throw new IllegalArgumentException("currency must be a 3-letter code");
        }
        return normalized;
    }

    private static void requireText(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " is required");
        }
    }

    private boolean isFifoReservationMode() {
        return "sqs-fifo".equalsIgnoreCase(reservationMode);
    }

    private String serialize(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("could not serialize inventory queue command", exception);
        }
    }
}
