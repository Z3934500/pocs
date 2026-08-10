package com.poc.payment.gateway;

import com.poc.contracts.CapturePaymentCommand;
import org.springframework.stereotype.Component;

/** Deterministic local gateway used by the PoC; no card data is handled here. */
@Component
public class StubPaymentGateway implements PaymentGateway {
    @Override public boolean capture(CapturePaymentCommand command) { return command.shouldSucceed(); }
    @Override public boolean refund(String paymentId, long amountCents, String currency) { return true; }
}