package com.poc.order.gateway;

import com.poc.contracts.*;

/** Port used by the Saga orchestrator; production can replace REST with gRPC or async commands. */
public interface PaymentGatewayClient {
    PaymentResponse capture(CapturePaymentCommand command);
    PaymentResponse query(String orderId, String idempotencyKey);
    RefundResponse refund(RefundPaymentCommand command);
}