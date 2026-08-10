package com.poc.contracts;

/**
 * Inventory reservation states exchanged between Order and Inventory Services.
 */
public enum ReservationStatus {
    RESERVED,
    COMMITTED,
    RELEASED,
    CANCELLED,
    EXPIRED
}