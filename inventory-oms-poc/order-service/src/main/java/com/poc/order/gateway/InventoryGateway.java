package com.poc.order.gateway;

import com.poc.contracts.*;

/** Port used by the Saga orchestrator; HTTP is only one possible adapter. */
public interface InventoryGateway {
    InventoryReservationResponse reserve(ReserveInventoryCommand command);
    InventoryReservationResponse commit(String orderId);
    InventoryReservationResponse release(String orderId, String reason);
}