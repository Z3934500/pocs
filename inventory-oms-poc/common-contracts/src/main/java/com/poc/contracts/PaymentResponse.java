package com.poc.contracts;

/**
 * Result returned by the Payment Service.
 */
public record PaymentResponse(
    String paymentId,
    String orderId,
    PaymentStatus status,
    long amountCents,
    String currency
) {
}