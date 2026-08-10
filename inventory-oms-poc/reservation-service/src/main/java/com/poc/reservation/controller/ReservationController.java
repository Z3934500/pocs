package com.poc.reservation.controller;

import com.poc.reservation.dto.EventConsumeResult;
import com.poc.reservation.dto.PaymentRequest;
import com.poc.reservation.dto.PaymentResult;
import com.poc.reservation.entity.InventoryStock;
import com.poc.reservation.entity.LedgerEntry;
import com.poc.reservation.entity.OutboxEvent;
import com.poc.reservation.entity.Reservation;
import com.poc.reservation.service.ReservationService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/reservation")
public class ReservationController {

    private final ReservationService service;

    public ReservationController(ReservationService service) {
        this.service = service;
    }

    @PostMapping("/create")
    public Reservation create(
        @RequestParam String orderId,
        @RequestParam String sku,
        @RequestParam int qty,
        @RequestHeader(value = "Idempotency-Key", required = false) String headerIdempotencyKey,
        @RequestParam(required = false) String idempotencyKey,
        @RequestParam(defaultValue = "15") int ttlMinutes,
        @RequestParam(defaultValue = "0") long orderAmountCents,
        @RequestParam(defaultValue = "CNY") String orderCurrency) {
        String key = headerIdempotencyKey != null && !headerIdempotencyKey.isBlank()
            ? headerIdempotencyKey : idempotencyKey;
        if (key == null || key.isBlank()) {
            key = orderId + ":" + sku;
        }
        return service.create(orderId, sku, qty, key, ttlMinutes, orderAmountCents, orderCurrency);
    }

    @PostMapping("/{orderId}/payment")
    public PaymentResult capturePayment(@PathVariable String orderId, @RequestBody PaymentRequest request) {
        return service.capturePayment(orderId, request);
    }

    @PostMapping("/{orderId}/cancel")
    public Reservation cancel(@PathVariable String orderId,
                              @RequestParam(defaultValue = "customer cancelled") String reason) {
        return service.cancel(orderId, reason);
    }

    @PostMapping("/expire")
    public int expireReservations() {
        return service.expireReservations();
    }

    @GetMapping("/stock")
    public List<InventoryStock> stock() {
        return service.stock();
    }

    @GetMapping("/outbox")
    public List<OutboxEvent> pendingOutbox() {
        return service.pendingOutbox();
    }

    @PostMapping("/outbox/publish")
    public List<OutboxEvent> publishOutbox() {
        return service.publishOutbox();
    }

    @PostMapping("/outbox/{eventId}/consume")
    public EventConsumeResult consumeEvent(@PathVariable String eventId) {
        return service.consumeEvent(eventId);
    }

    @GetMapping("/ledger")
    public List<LedgerEntry> ledger(@RequestParam(required = false) String orderId) {
        return service.ledger(orderId);
    }
}