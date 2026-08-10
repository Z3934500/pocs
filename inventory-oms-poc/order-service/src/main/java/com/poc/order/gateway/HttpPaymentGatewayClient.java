package com.poc.order.gateway;

import com.poc.contracts.CapturePaymentCommand;
import com.poc.contracts.PaymentResponse;
import com.poc.contracts.RefundPaymentCommand;
import com.poc.contracts.RefundResponse;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

/** REST adapter for Payment Service; production adds mTLS, IAM and timeouts. */
@Component
public class HttpPaymentGatewayClient implements PaymentGatewayClient {

    private final RestClient client;

    public HttpPaymentGatewayClient(
            RestClient.Builder builder,
            @Value("${clients.payment.base-url}") String baseUrl) {
        this.client = builder.baseUrl(baseUrl).build();
    }

    @Override
    public PaymentResponse capture(CapturePaymentCommand command) {
        return client.post().uri("/internal/payments").body(command).retrieve().body(PaymentResponse.class);
    }

    @Override
    public PaymentResponse query(String orderId, String idempotencyKey) {
        return client.get().uri(uriBuilder -> uriBuilder.path("/internal/payments/by-order/{orderId}")
                .queryParam("idempotencyKey", idempotencyKey).build(orderId)).retrieve().body(PaymentResponse.class);
    }

    @Override
    public RefundResponse refund(RefundPaymentCommand command) {
        return client.post().uri("/internal/payments/{paymentId}/refund", command.paymentId())
                .body(command).retrieve().body(RefundResponse.class);
    }
}