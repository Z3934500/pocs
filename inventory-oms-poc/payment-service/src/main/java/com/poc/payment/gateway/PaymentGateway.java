package com.poc.payment.gateway;

import com.poc.contracts.CapturePaymentCommand;

/** Port for the external gateway; production adapter verifies signed callbacks and provider state. */
public interface PaymentGateway {
    boolean capture(CapturePaymentCommand command);
    boolean refund(String paymentId, long amountCents, String currency);
}