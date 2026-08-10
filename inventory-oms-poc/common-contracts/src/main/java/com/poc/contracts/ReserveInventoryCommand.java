package com.poc.contracts;

/**
 * Command sent by the Order Service to create an inventory reservation.
 */
public record ReserveInventoryCommand(
    String orderId,
    String sku,
    int qty,
    String idempotencyKey,
    int ttlMinutes
) {
}