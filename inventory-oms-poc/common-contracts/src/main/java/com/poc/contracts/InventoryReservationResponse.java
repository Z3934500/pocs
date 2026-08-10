package com.poc.contracts;

/**
 * Result returned by the Inventory Service after a reservation command.
 */
public record InventoryReservationResponse(
    String orderId,
    String reservationId,
    String sku,
    int qty,
    ReservationStatus status
) {
}