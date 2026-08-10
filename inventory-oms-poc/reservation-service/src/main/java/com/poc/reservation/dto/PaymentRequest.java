package com.poc.reservation.dto;

public record PaymentRequest(
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