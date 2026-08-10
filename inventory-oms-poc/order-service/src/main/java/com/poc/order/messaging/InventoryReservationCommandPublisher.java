package com.poc.order.messaging;

import com.poc.order.entity.OrderOutboxEvent;

/** Publishes durable inventory reservation commands to an ordered transport. */
public interface InventoryReservationCommandPublisher {
    void publish(OrderOutboxEvent event);
}
