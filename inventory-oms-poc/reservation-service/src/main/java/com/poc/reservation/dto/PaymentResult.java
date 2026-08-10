package com.poc.reservation.dto;

import com.poc.reservation.entity.PaymentTransaction;
import com.poc.reservation.entity.Reservation;

public record PaymentResult(PaymentTransaction payment, Reservation reservation) {
}