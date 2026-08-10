package com.poc.contracts;

/**
 * Public order states exposed by the Order Service.
 */
public enum OrderStatus {
    PENDING,
    /** Inventory reservation is durably queued for strict per-SKU ordering. */
    RESERVATION_QUEUED,
    CONFIRMED,
    RESERVATION_FAILED,
    PAYMENT_FAILED,
    PAYMENT_UNKNOWN,
    CANCELLED,
    COMPENSATED,
    COMPENSATION_FAILED
}
