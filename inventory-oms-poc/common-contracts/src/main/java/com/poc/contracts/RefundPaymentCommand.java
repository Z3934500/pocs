package com.poc.contracts;

/**
 * Compensating command issued when payment succeeded but a later Saga step failed.
 */
public record RefundPaymentCommand(
    String paymentId,
    String idempotencyKey
) {
}