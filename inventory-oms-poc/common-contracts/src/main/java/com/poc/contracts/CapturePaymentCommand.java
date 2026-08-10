package com.poc.contracts;

/**
 * Command sent to the Payment Service. The succeed flag is only a local gateway stub.
 */
public record CapturePaymentCommand(
    String orderId,
    String idempotencyKey,
    String providerRef,
    long amountCents,
    String currency,
    Boolean succeed
) {
    public boolean shouldSucceed() {
        return succeed == null || succeed;
    }
}