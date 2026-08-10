package com.poc.contracts;

/**
 * Admission request for the Redis/Lua flash-sale path. The request is placed
 * on a Redis Stream only after the script atomically checks stock and dedupes
 * the user/idempotency key.
 */
public record SeckillReserveCommand(
        String orderId,
        String userId,
        String sku,
        int qty,
        String idempotencyKey) {
}
