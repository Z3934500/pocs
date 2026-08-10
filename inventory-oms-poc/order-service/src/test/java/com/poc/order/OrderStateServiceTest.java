package com.poc.order;

import com.poc.contracts.OrderStatus;
import com.poc.order.dto.CreateOrderRequest;
import com.poc.order.entity.OrderAggregate;
import com.poc.order.repository.*;
import com.poc.order.service.OrderStateService;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
class OrderStateServiceTest {
    @Autowired OrderStateService stateService;
    @Autowired OrderRepository orderRepository;
    @Autowired OrderOutboxRepository outboxRepository;
    @Autowired OrderSagaStepRepository sagaRepository;

    @BeforeEach void reset() { outboxRepository.deleteAll(); sagaRepository.deleteAll(); orderRepository.deleteAll(); }

    @Test void orderIdempotencyReturnsExistingAggregate() {
        CreateOrderRequest request = new CreateOrderRequest("ORDER-1", "SKU-RED-001", 2, 1200, "CNY", "order-1", 15, "pay-1", "provider-1", true);
        OrderAggregate first = stateService.createPending(request, "order-1");
        OrderAggregate retry = stateService.createPending(request, "order-1");
        assertEquals(first.getOrderId(), retry.getOrderId()); assertEquals(OrderStatus.PENDING, retry.getStatus()); assertEquals(1, outboxRepository.count());
    }

    @Test void localTransitionWritesSagaAndOutbox() {
        CreateOrderRequest request = new CreateOrderRequest("ORDER-2", "SKU-RED-001", 1, 500, "CNY", "order-2", 15, "pay-2", "provider-2", true);
        stateService.createPending(request, "order-2");
        assertEquals(OrderStatus.CONFIRMED, stateService.markConfirmed("ORDER-2", "reservation-2", "payment-2").getStatus());
        assertTrue(sagaRepository.count() >= 2); assertEquals(2, outboxRepository.count());
    }
}