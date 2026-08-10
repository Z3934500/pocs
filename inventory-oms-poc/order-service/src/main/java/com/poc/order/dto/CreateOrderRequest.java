package com.poc.order.dto;

/** Public checkout command. Payment fields are included only for this local gateway stub. */
public record CreateOrderRequest(
    String orderId,
    String sku,
    int qty,
    long amountCents,
    String currency,
    String idempotencyKey,
    int reservationTtlMinutes,
    String paymentIdempotencyKey,
    String providerRef,
    Boolean paymentSucceed
) { }