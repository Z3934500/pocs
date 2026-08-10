package com.poc.contracts;

/**
 * Payment states exchanged between Order and Payment Services.
 */
public enum PaymentStatus {
    CAPTURED,
    FAILED,
    REFUNDED
}