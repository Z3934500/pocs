package com.poc.contracts;

/** Result emitted after the queued inventory command has been applied. */
public record InventoryReservationResult(
        String orderId,
        String sku,
        int qty,
        String reservationId,
        ReservationStatus status,
        String error,
        String idempotencyKey) {

    public static InventoryReservationResult reserved(
            InventoryReservationResponse response, String idempotencyKey) {
        return new InventoryReservationResult(
                response.orderId(), response.sku(), response.qty(),
                response.reservationId(), response.status(), null, idempotencyKey);
    }

    public static InventoryReservationResult failed(
            String orderId, String sku, int qty, String error, String idempotencyKey) {
        return new InventoryReservationResult(
                orderId, sku, qty, null, null,
                error == null || error.isBlank() ? "inventory reservation failed" : error,
                idempotencyKey);
    }
}
