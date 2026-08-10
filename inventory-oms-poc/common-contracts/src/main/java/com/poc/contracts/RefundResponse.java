package com.poc.contracts;

/**
 * Result returned by the Payment Service after a refund command.
 */
public record RefundResponse(
    String paymentId,
    PaymentStatus status
) {
}