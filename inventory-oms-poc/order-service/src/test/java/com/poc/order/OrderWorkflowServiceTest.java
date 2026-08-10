package com.poc.order;

import com.poc.contracts.*;
import com.poc.order.dto.CreateOrderRequest;
import com.poc.order.entity.OrderAggregate;
import com.poc.order.gateway.*;
import com.poc.order.service.*;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(MockitoExtension.class)
class OrderWorkflowServiceTest {
    @Mock OrderStateService stateService;
    @Mock InventoryGateway inventoryGateway;
    @Mock PaymentGatewayClient paymentGateway;

    @Test void successRunsReserveCaptureCommitAndConfirm() {
        OrderAggregate pending = new OrderAggregate("ORDER-1", "SKU-RED-001", 1, 1000, "CNY", "order-1");
        OrderAggregate confirmed = new OrderAggregate("ORDER-1", "SKU-RED-001", 1, 1000, "CNY", "order-1");
        CreateOrderRequest request = new CreateOrderRequest("ORDER-1", "SKU-RED-001", 1, 1000, "CNY", "order-1", 15, "pay-1", "provider-1", true);
        when(stateService.createPending(request, "order-1")).thenReturn(pending);
        when(inventoryGateway.reserve(any())).thenReturn(new InventoryReservationResponse("ORDER-1", "reservation-1", "SKU-RED-001", 1, ReservationStatus.RESERVED));
        when(paymentGateway.capture(any())).thenReturn(new PaymentResponse("payment-1", "ORDER-1", PaymentStatus.CAPTURED, 1000, "CNY"));
        when(inventoryGateway.commit("ORDER-1")).thenReturn(new InventoryReservationResponse("ORDER-1", "reservation-1", "SKU-RED-001", 1, ReservationStatus.COMMITTED));
        when(stateService.markConfirmed("ORDER-1", "reservation-1", "payment-1")).thenReturn(confirmed);

        assertEquals(confirmed, new OrderWorkflowService(stateService, inventoryGateway, paymentGateway, new SimpleMeterRegistry()).placeOrder(request, null));
        verify(inventoryGateway).commit("ORDER-1"); verify(stateService).markConfirmed("ORDER-1", "reservation-1", "payment-1");
    }

    @Test void failedPaymentReleasesInventoryAndMarksOrder() {
        OrderAggregate pending = new OrderAggregate("ORDER-2", "SKU-RED-001", 1, 1000, "CNY", "order-2");
        CreateOrderRequest request = new CreateOrderRequest("ORDER-2", "SKU-RED-001", 1, 1000, "CNY", "order-2", 15, "pay-2", "provider-2", false);
        when(stateService.createPending(request, "order-2")).thenReturn(pending);
        when(inventoryGateway.reserve(any())).thenReturn(new InventoryReservationResponse("ORDER-2", "reservation-2", "SKU-RED-001", 1, ReservationStatus.RESERVED));
        when(paymentGateway.capture(any())).thenReturn(new PaymentResponse("payment-2", "ORDER-2", PaymentStatus.FAILED, 1000, "CNY"));
        when(inventoryGateway.release("ORDER-2", "order saga compensation")).thenReturn(new InventoryReservationResponse("ORDER-2", "reservation-2", "SKU-RED-001", 1, ReservationStatus.RELEASED));
        when(stateService.markPaymentFailed(eq("ORDER-2"), anyString())).thenReturn(pending);

        new OrderWorkflowService(stateService, inventoryGateway, paymentGateway, new SimpleMeterRegistry()).placeOrder(request, null);
        verify(inventoryGateway).release("ORDER-2", "order saga compensation"); verify(stateService).markPaymentFailed(eq("ORDER-2"), anyString());
        verify(inventoryGateway, never()).commit(anyString());
    }

    @Test void timeoutQueriesPaymentBeforeMarkingUnknown() {
        OrderAggregate pending = new OrderAggregate("ORDER-TIMEOUT", "SKU-RED-001", 1, 1000, "CNY", "order-timeout");
        OrderAggregate confirmed = new OrderAggregate("ORDER-TIMEOUT", "SKU-RED-001", 1, 1000, "CNY", "order-timeout");
        CreateOrderRequest request = new CreateOrderRequest("ORDER-TIMEOUT", "SKU-RED-001", 1, 1000, "CNY", "order-timeout", 15, "pay-timeout", "provider-timeout", true);
        when(stateService.createPending(request, "order-timeout")).thenReturn(pending);
        when(inventoryGateway.reserve(any())).thenReturn(new InventoryReservationResponse("ORDER-TIMEOUT", "reservation-timeout", "SKU-RED-001", 1, ReservationStatus.RESERVED));
        when(paymentGateway.capture(any())).thenThrow(new IllegalStateException("provider timeout"));
        when(paymentGateway.query("ORDER-TIMEOUT", "pay-timeout")).thenReturn(new PaymentResponse("payment-timeout", "ORDER-TIMEOUT", PaymentStatus.CAPTURED, 1000, "CNY"));
        when(inventoryGateway.commit("ORDER-TIMEOUT")).thenReturn(new InventoryReservationResponse("ORDER-TIMEOUT", "reservation-timeout", "SKU-RED-001", 1, ReservationStatus.COMMITTED));
        when(stateService.markConfirmed("ORDER-TIMEOUT", "reservation-timeout", "payment-timeout")).thenReturn(confirmed);

        assertEquals(confirmed, new OrderWorkflowService(stateService, inventoryGateway, paymentGateway, new SimpleMeterRegistry()).placeOrder(request, null));
        verify(paymentGateway).query("ORDER-TIMEOUT", "pay-timeout");
        verify(stateService, never()).markPaymentUnknown(anyString(), anyString());
    }

    @Test void queuedReservationResultResumesPaymentAndCommit() {
        OrderAggregate queued = new OrderAggregate(
                "ORDER-QUEUED", "SKU-RED-001", 1, 1000, "CNY", "order-queued");
        queued.setStatus(OrderStatus.RESERVATION_QUEUED);
        queued.setPaymentIdempotencyKey("pay-queued");
        queued.setProviderRef("provider-queued");
        queued.setPaymentSucceed(true);
        OrderAggregate confirmed = new OrderAggregate(
                "ORDER-QUEUED", "SKU-RED-001", 1, 1000, "CNY", "order-queued");
        InventoryReservationResult result = new InventoryReservationResult(
                "ORDER-QUEUED", "SKU-RED-001", 1, "reservation-queued",
                ReservationStatus.RESERVED, null, "order-queued:inventory");

        when(stateService.get("ORDER-QUEUED")).thenReturn(queued);
        when(paymentGateway.capture(any())).thenReturn(
                new PaymentResponse("payment-queued", "ORDER-QUEUED", PaymentStatus.CAPTURED, 1000, "CNY"));
        when(inventoryGateway.commit("ORDER-QUEUED")).thenReturn(
                new InventoryReservationResponse("ORDER-QUEUED", "reservation-queued", "SKU-RED-001", 1,
                        ReservationStatus.COMMITTED));
        when(stateService.markConfirmed("ORDER-QUEUED", "reservation-queued", "payment-queued"))
                .thenReturn(confirmed);

        assertEquals(confirmed, new OrderWorkflowService(
                stateService, inventoryGateway, paymentGateway, new SimpleMeterRegistry())
                .onReservationResult(result));
        verify(paymentGateway).capture(any());
        verify(inventoryGateway).commit("ORDER-QUEUED");
    }
}
