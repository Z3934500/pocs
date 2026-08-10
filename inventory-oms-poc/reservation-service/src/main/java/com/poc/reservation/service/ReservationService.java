package com.poc.reservation.service;

import com.poc.reservation.dto.EventConsumeResult;
import com.poc.reservation.dto.PaymentRequest;
import com.poc.reservation.dto.PaymentResult;
import com.poc.reservation.entity.InboxEvent;
import com.poc.reservation.entity.InventoryStock;
import com.poc.reservation.entity.LedgerDirection;
import com.poc.reservation.entity.LedgerEntry;
import com.poc.reservation.entity.OutboxEvent;
import com.poc.reservation.entity.PaymentStatus;
import com.poc.reservation.entity.PaymentTransaction;
import com.poc.reservation.entity.Reservation;
import com.poc.reservation.entity.ReservationStatus;
import com.poc.reservation.entity.SagaLog;
import com.poc.reservation.repository.InboxEventRepository;
import com.poc.reservation.repository.InventoryStockRepository;
import com.poc.reservation.repository.LedgerEntryRepository;
import com.poc.reservation.repository.OutboxEventRepository;
import com.poc.reservation.repository.PaymentTransactionRepository;
import com.poc.reservation.repository.ReservationRepository;
import com.poc.reservation.repository.SagaLogRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

@Service
public class ReservationService {

    private static final int DEFAULT_RESERVATION_MINUTES = 15;

    private final ReservationRepository reservationRepository;
    private final InventoryStockRepository stockRepository;
    private final PaymentTransactionRepository paymentRepository;
    private final OutboxEventRepository outboxRepository;
    private final SagaLogRepository sagaLogRepository;
    private final LedgerEntryRepository ledgerRepository;
    private final InboxEventRepository inboxRepository;

    @Value("${oms.reservation.expiration-batch-size:100}")
    private int expirationBatchSize;

    public ReservationService(ReservationRepository reservationRepository,
                              InventoryStockRepository stockRepository,
                              PaymentTransactionRepository paymentRepository,
                              OutboxEventRepository outboxRepository,
                              SagaLogRepository sagaLogRepository,
                              LedgerEntryRepository ledgerRepository,
                              InboxEventRepository inboxRepository) {
        this.reservationRepository = reservationRepository;
        this.stockRepository = stockRepository;
        this.paymentRepository = paymentRepository;
        this.outboxRepository = outboxRepository;
        this.sagaLogRepository = sagaLogRepository;
        this.ledgerRepository = ledgerRepository;
        this.inboxRepository = inboxRepository;
    }

    @Transactional
    public Reservation create(String orderId, String sku, int qty) {
        return create(orderId, sku, qty, orderId + ":" + sku, DEFAULT_RESERVATION_MINUTES, 0, "CNY");
    }

    @Transactional
    public Reservation create(String orderId, String sku, int qty, String idempotencyKey, int ttlMinutes) {
        return create(orderId, sku, qty, idempotencyKey, ttlMinutes, 0, "CNY");
    }

    @Transactional
    public Reservation create(String orderId, String sku, int qty, String idempotencyKey, int ttlMinutes,
                              long orderAmountCents, String orderCurrency) {
        requireText(orderId, "orderId");
        requireText(sku, "sku");
        requireText(idempotencyKey, "idempotencyKey");
        if (qty <= 0) {
            throw new IllegalArgumentException("qty must be positive");
        }
        if (ttlMinutes <= 0) {
            throw new IllegalArgumentException("ttlMinutes must be positive");
        }
        if (orderAmountCents < 0) {
            throw new IllegalArgumentException("orderAmountCents must not be negative");
        }
        String normalizedOrderCurrency = normalizeCurrency(orderCurrency);

        Reservation existing = reservationRepository.findByIdempotencyKey(idempotencyKey).orElse(null);
        if (existing != null) {
            if (!existing.getOrderId().equals(orderId)
                || !existing.getSku().equals(sku)
                || existing.getQty() != qty
                || (orderAmountCents > 0 && existing.getOrderAmountCents() != orderAmountCents)
                || (orderAmountCents > 0 && !existing.getOrderCurrency().equals(normalizedOrderCurrency))) {
                throw new IllegalStateException("idempotency key was reused with a different reservation request");
            }
            return existing;
        }

        Reservation existingOrderReservation = reservationRepository.findByOrderIdForUpdate(orderId).orElse(null);
        if (existingOrderReservation != null) {
            throw new IllegalStateException("order already has a reservation: " + orderId);
        }

        InventoryStock stock = stockRepository.findBySkuForUpdate(sku)
            .orElseThrow(() -> new IllegalArgumentException("unknown sku=" + sku));
        stock.reserve(qty);

        Reservation reservation = new Reservation();
        reservation.setOrderId(orderId);
        reservation.setSku(sku);
        reservation.setQty(qty);
        reservation.setOrderAmountCents(orderAmountCents);
        reservation.setOrderCurrency(normalizedOrderCurrency);
        reservation.setStatus(ReservationStatus.RESERVED);
        reservation.setIdempotencyKey(idempotencyKey);
        reservation.setExpiresAt(LocalDateTime.now().plusMinutes(ttlMinutes));
        reservationRepository.save(reservation);

        recordSaga(orderId, "reserve_inventory", "COMPLETED", "stock row locked and quantity reserved");
        recordOutbox("reservation", orderId, "inventory.reserved",
            "{\"orderId\":\"" + orderId + "\",\"sku\":\"" + sku + "\",\"qty\":" + qty + "}");
        return reservation;
    }

    @Transactional
    public PaymentResult capturePayment(String orderId, PaymentRequest request) {
        requireText(orderId, "orderId");
        if (request == null) {
            throw new IllegalArgumentException("payment request is required");
        }
        String idempotencyKey = request.idempotencyKey();
        requireText(idempotencyKey, "payment.idempotencyKey");
        String currency = normalizeCurrency(request.currency());
        if (request.amountCents() <= 0) {
            throw new IllegalArgumentException("payment.amountCents must be positive");
        }

        PaymentTransaction existing = paymentRepository
            .findByOrderIdAndIdempotencyKey(orderId, idempotencyKey).orElse(null);
        if (existing != null) {
            if (existing.getAmountCents() != request.amountCents()
                || !existing.getCurrency().equals(currency)
                || (request.providerRef() != null && !request.providerRef().isBlank()
                    && !existing.getProviderRef().equals(request.providerRef()))) {
                throw new IllegalStateException("payment idempotency key was reused with a different request");
            }
            Reservation reservation = reservationRepository.findByOrderIdForUpdate(orderId)
                .orElseThrow(() -> new IllegalStateException("reservation not found for order=" + orderId));
            return new PaymentResult(existing, reservation);
        }

        String providerRef = request.providerRef();
        if (providerRef == null || providerRef.isBlank()) {
            providerRef = "PROVIDER-" + UUID.randomUUID();
        }
        if (paymentRepository.findByProviderRef(providerRef).isPresent()) {
            throw new IllegalStateException("providerRef has already been used: " + providerRef);
        }

        Reservation reservation = reservationRepository.findByOrderIdForUpdate(orderId)
            .orElseThrow(() -> new IllegalArgumentException("reservation not found for order=" + orderId));
        if (reservation.getStatus() != ReservationStatus.RESERVED) {
            throw new IllegalStateException("order=" + orderId + " cannot be paid from status=" + reservation.getStatus());
        }
        if (reservation.getOrderAmountCents() > 0
            && (reservation.getOrderAmountCents() != request.amountCents()
                || !reservation.getOrderCurrency().equals(currency))) {
            throw new IllegalStateException("payment amount/currency does not match the order snapshot");
        }

        PaymentStatus paymentStatus = request.shouldSucceed() ? PaymentStatus.CAPTURED : PaymentStatus.FAILED;
        PaymentTransaction payment = new PaymentTransaction(
            orderId, providerRef, idempotencyKey, request.amountCents(), currency, paymentStatus);
        paymentRepository.save(payment);

        InventoryStock stock = stockRepository.findBySkuForUpdate(reservation.getSku())
            .orElseThrow(() -> new IllegalStateException("stock row not found for sku=" + reservation.getSku()));

        if (request.shouldSucceed()) {
            stock.commit(reservation.getQty());
            reservation.setStatus(ReservationStatus.COMMITTED);
            recordSaga(orderId, "capture_payment", "COMPLETED", "payment captured");
            recordSaga(orderId, "commit_inventory", "COMPLETED", "reserved stock moved to sold stock");
            recordLedger(payment, orderId);
            recordOutbox("payment", orderId, "payment.captured",
                "{\"orderId\":\"" + orderId + "\",\"paymentId\":\"" + payment.getPaymentId() + "\",\"providerRef\":\"" + providerRef + "\"}");
            recordOutbox("inventory", orderId, "inventory.committed",
                "{\"orderId\":\"" + orderId + "\",\"sku\":\"" + reservation.getSku() + "\",\"qty\":" + reservation.getQty() + "}");
            recordOutbox("order", orderId, "order.confirmed",
                "{\"orderId\":\"" + orderId + "\",\"status\":\"CONFIRMED\"}");
        } else {
            stock.release(reservation.getQty());
            reservation.setStatus(ReservationStatus.RELEASED);
            recordSaga(orderId, "capture_payment", "COMPENSATED", "payment failed");
            recordSaga(orderId, "release_inventory", "COMPLETED", "reserved stock released");
            recordOutbox("payment", orderId, "payment.failed",
                "{\"orderId\":\"" + orderId + "\",\"paymentId\":\"" + payment.getPaymentId() + "\",\"providerRef\":\"" + providerRef + "\"}");
            recordOutbox("inventory", orderId, "inventory.released",
                "{\"orderId\":\"" + orderId + "\",\"sku\":\"" + reservation.getSku() + "\",\"qty\":" + reservation.getQty() + "}");
        }

        reservationRepository.save(reservation);
        return new PaymentResult(payment, reservation);
    }

    @Transactional
    public Reservation cancel(String orderId, String reason) {
        Reservation reservation = reservationRepository.findByOrderIdForUpdate(orderId)
            .orElseThrow(() -> new IllegalArgumentException("reservation not found for order=" + orderId));
        if (reservation.getStatus() == ReservationStatus.CANCELLED
            || reservation.getStatus() == ReservationStatus.RELEASED
            || reservation.getStatus() == ReservationStatus.EXPIRED) {
            return reservation;
        }
        if (reservation.getStatus() != ReservationStatus.RESERVED) {
            throw new IllegalStateException("cannot cancel reservation from status=" + reservation.getStatus());
        }
        releaseReservation(reservation, ReservationStatus.CANCELLED, reason == null ? "cancelled" : reason);
        return reservation;
    }

    @Transactional
    public int expireReservations() {
        LocalDateTime now = LocalDateTime.now();
        List<Reservation> candidates = reservationRepository
            .findByStatusAndExpiresAtBeforeOrderByExpiresAtAsc(
                ReservationStatus.RESERVED,
                now,
                org.springframework.data.domain.PageRequest.of(0, boundedExpirationBatchSize()));
        int expired = 0;
        for (Reservation candidate : candidates) {
            Reservation reservation = reservationRepository.findByOrderIdForUpdate(candidate.getOrderId()).orElse(null);
            if (reservation != null
                && reservation.getStatus() == ReservationStatus.RESERVED
                && !reservation.getExpiresAt().isAfter(now)) {
                releaseReservation(reservation, ReservationStatus.EXPIRED, "reservation timeout");
                expired++;
            }
        }
        return expired;
    }

    private int boundedExpirationBatchSize() {
        return Math.max(1, Math.min(expirationBatchSize, 1_000));
    }

    @Transactional(readOnly = true)
    public List<InventoryStock> stock() {
        return stockRepository.findAll();
    }

    @Transactional(readOnly = true)
    public List<OutboxEvent> pendingOutbox() {
        return outboxRepository.findTop100ByStatusOrderByCreatedAtAsc(OutboxEvent.PENDING);
    }

    @Transactional
    public List<OutboxEvent> publishOutbox() {
        List<OutboxEvent> events = outboxRepository.findTop100ByStatusOrderByCreatedAtAsc(OutboxEvent.PENDING);
        events.forEach(OutboxEvent::markPublished);
        return events;
    }

    @Transactional
    public EventConsumeResult consumeEvent(String eventId) {
        requireText(eventId, "eventId");
        if (inboxRepository.existsById(eventId)) {
            return new EventConsumeResult(eventId, false, "event was already processed");
        }
        OutboxEvent event = outboxRepository.findById(eventId)
            .orElseThrow(() -> new IllegalArgumentException("outbox event not found: " + eventId));
        inboxRepository.save(new InboxEvent(eventId, event.getEventType()));
        return new EventConsumeResult(eventId, true, "event accepted by Inbox");
    }

    @Transactional(readOnly = true)
    public List<LedgerEntry> ledger(String orderId) {
        if (orderId == null || orderId.isBlank()) {
            return ledgerRepository.findAll();
        }
        return ledgerRepository.findAll().stream()
            .filter(entry -> entry.getOrderId().equals(orderId))
            .toList();
    }

    private void releaseReservation(Reservation reservation, ReservationStatus finalStatus, String reason) {
        InventoryStock stock = stockRepository.findBySkuForUpdate(reservation.getSku())
            .orElseThrow(() -> new IllegalStateException("stock row not found for sku=" + reservation.getSku()));
        stock.release(reservation.getQty());
        reservation.setStatus(finalStatus);
        reservationRepository.save(reservation);
        recordSaga(reservation.getOrderId(), "release_inventory", "COMPLETED", reason);
        recordOutbox("inventory", reservation.getOrderId(), "inventory.released",
            "{\"orderId\":\"" + reservation.getOrderId() + "\",\"sku\":\"" + reservation.getSku() + "\",\"qty\":" + reservation.getQty() + "}");
        if (finalStatus == ReservationStatus.EXPIRED) {
            recordOutbox("reservation", reservation.getOrderId(), "reservation.timeout",
                "{\"orderId\":\"" + reservation.getOrderId() + "\"}");
        } else {
            recordOutbox("reservation", reservation.getOrderId(), "reservation.cancelled",
                "{\"orderId\":\"" + reservation.getOrderId() + "\",\"reason\":\"" + reason + "\"}");
        }
    }

    private void recordLedger(PaymentTransaction payment, String orderId) {
        String transactionId = "LEDGER-" + payment.getPaymentId();
        ledgerRepository.save(new LedgerEntry(
            transactionId + "-DEBIT", transactionId, orderId, payment.getPaymentId(),
            "customer_receivable", LedgerDirection.DEBIT, payment.getAmountCents(), payment.getCurrency()));
        ledgerRepository.save(new LedgerEntry(
            transactionId + "-CREDIT", transactionId, orderId, payment.getPaymentId(),
            "merchant_cash_pending", LedgerDirection.CREDIT, payment.getAmountCents(), payment.getCurrency()));
    }

    private void recordSaga(String orderId, String step, String status, String message) {
        sagaLogRepository.save(new SagaLog(orderId, step, status, message));
    }

    private void recordOutbox(String aggregateType, String aggregateId, String eventType, String payload) {
        outboxRepository.save(new OutboxEvent(aggregateType, aggregateId, eventType, payload));
    }

    private static String normalizeCurrency(String currency) {
        String normalized = currency == null || currency.isBlank() ? "CNY" : currency.trim().toUpperCase();
        if (normalized.length() != 3) {
            throw new IllegalArgumentException("currency must be a 3-letter code");
        }
        return normalized;
    }

    private static void requireText(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " is required");
        }
    }
}
