package com.poc.payment.controller;

import com.poc.contracts.*;
import com.poc.payment.entity.PaymentOutboxEvent;
import com.poc.payment.service.PaymentService;
import java.util.List;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

/** Internal Payment API; public clients must use Order Service, not this controller. */
@RestController
@RequestMapping("/internal/payments")
public class PaymentController {
    private final PaymentService paymentService;
    public PaymentController(PaymentService paymentService) { this.paymentService = paymentService; }
    @PostMapping public PaymentResponse capture(@RequestBody CapturePaymentCommand command) { return paymentService.capture(command); }
    @PostMapping("/{paymentId}/refund") public RefundResponse refund(@PathVariable String paymentId, @RequestBody RefundPaymentCommand command) {
        return paymentService.refund(new RefundPaymentCommand(paymentId, command == null ? null : command.idempotencyKey()));
    }
    @GetMapping("/{paymentId}") public PaymentResponse get(@PathVariable String paymentId) { return paymentService.get(paymentId); }
    @GetMapping("/by-order/{orderId}")
    public PaymentResponse getByOrder(@PathVariable String orderId, @RequestParam String idempotencyKey) {
        return paymentService.findByOrderAndIdempotency(orderId, idempotencyKey)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "payment not found"));
    }
    @GetMapping("/outbox") public List<PaymentOutboxEvent> pendingOutbox() { return paymentService.pendingOutbox(); }
}