package com.poc.payment.service;

import com.poc.contracts.CapturePaymentCommand;
import com.poc.contracts.PaymentResponse;
import com.poc.contracts.PaymentStatus;
import com.poc.contracts.RefundPaymentCommand;
import com.poc.contracts.RefundResponse;
import com.poc.payment.entity.LedgerDirection;
import com.poc.payment.entity.LedgerEntry;
import com.poc.payment.entity.PaymentOutboxEvent;
import com.poc.payment.entity.PaymentTransaction;
import com.poc.payment.gateway.PaymentGateway;
import com.poc.payment.repository.LedgerEntryRepository;
import com.poc.payment.repository.PaymentOutboxRepository;
import com.poc.payment.repository.PaymentTransactionRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Application service for the Payment bounded context.
 *
 * <p>The external provider is hidden behind {@link PaymentGateway}. The local
 * transaction owns idempotency, payment state, the append-only Ledger and the
 * Outbox event. A production adapter must not retry an unknown capture without
 * first querying provider status.</p>
 */
@Service
public class PaymentService {

    private final PaymentTransactionRepository paymentRepository;
    private final PaymentOutboxRepository outboxRepository;
    private final LedgerEntryRepository ledgerRepository;
    private final PaymentGateway paymentGateway;
    private final MeterRegistry meterRegistry;

    public PaymentService(
            PaymentTransactionRepository paymentRepository,
            PaymentOutboxRepository outboxRepository,
            LedgerEntryRepository ledgerRepository,
            PaymentGateway paymentGateway,
            MeterRegistry meterRegistry) {
        this.paymentRepository = paymentRepository;
        this.outboxRepository = outboxRepository;
        this.ledgerRepository = ledgerRepository;
        this.paymentGateway = paymentGateway;
        this.meterRegistry = meterRegistry;
    }

    /** Captures once per order and idempotency key, then records a balanced Ledger. */
    @Transactional
    public PaymentResponse capture(CapturePaymentCommand command) {
        validate(command);
        String currency = normalizeCurrency(command.currency());

        PaymentTransaction existing = paymentRepository
                .findByOrderIdAndIdempotencyKey(command.orderId(), command.idempotencyKey())
                .orElse(null);
        if (existing != null) {
            ensureSameRequest(existing, command, currency);
            return response(existing);
        }

        String providerRef = command.providerRef();
        if (providerRef == null || providerRef.isBlank()) {
            providerRef = "PROVIDER-" + UUID.randomUUID();
        }
        if (paymentRepository.findByProviderRef(providerRef).isPresent()) {
            throw new IllegalStateException("providerRef already used");
        }

        boolean captured = paymentGateway.capture(command);
        PaymentTransaction payment = new PaymentTransaction(
                command.orderId(),
                providerRef,
                command.idempotencyKey(),
                command.amountCents(),
                currency,
                captured ? PaymentStatus.CAPTURED : PaymentStatus.FAILED);
        paymentRepository.save(payment);

        if (captured) {
            recordLedger(payment, false);
            recordOutbox(
                    command.orderId(),
                    "payment.captured",
                    "{\"orderId\":\"" + command.orderId()
                            + "\",\"paymentId\":\"" + payment.getPaymentId() + "\"}");
            increment("payment.capture.success");
        } else {
            recordOutbox(
                    command.orderId(),
                    "payment.failed",
                    "{\"orderId\":\"" + command.orderId()
                            + "\",\"providerRef\":\"" + providerRef + "\"}");
            increment("payment.capture.failed");
        }
        return response(payment);
    }

    /** Refunds a captured payment as a compensating Saga action. */
    @Transactional
    public RefundResponse refund(RefundPaymentCommand command) {
        if (command == null || command.paymentId() == null || command.paymentId().isBlank()) {
            throw new IllegalArgumentException("paymentId is required");
        }

        PaymentTransaction payment = findPayment(command.paymentId());
        if (payment.getStatus() == PaymentStatus.REFUNDED) {
            return new RefundResponse(command.paymentId(), PaymentStatus.REFUNDED);
        }
        if (payment.getStatus() != PaymentStatus.CAPTURED) {
            throw new IllegalStateException("only captured payment can be refunded");
        }
        if (!paymentGateway.refund(
                command.paymentId(), payment.getAmountCents(), payment.getCurrency())) {
            throw new IllegalStateException("gateway refund failed");
        }

        payment.setStatus(PaymentStatus.REFUNDED);
        recordLedger(payment, true);
        recordOutbox(
                payment.getOrderId(),
                "payment.refunded",
                "{\"orderId\":\"" + payment.getOrderId()
                        + "\",\"paymentId\":\"" + command.paymentId() + "\"}");
        increment("payment.refund.success");
        return new RefundResponse(command.paymentId(), PaymentStatus.REFUNDED);
    }

    @Transactional(readOnly = true)
    public PaymentResponse get(String paymentId) {
        return response(findPayment(paymentId));
    }

    @Transactional(readOnly = true)
    public List<PaymentOutboxEvent> pendingOutbox() {
        return outboxRepository.findTop100ByStatusOrderByCreatedAtAsc(PaymentOutboxEvent.PENDING);
    }

    /** Used after a timeout to resolve the provider result before a retry or compensation. */
    @Transactional(readOnly = true)
    public Optional<PaymentResponse> findByOrderAndIdempotency(String orderId, String idempotencyKey) {
        return paymentRepository.findByOrderIdAndIdempotencyKey(orderId, idempotencyKey).map(PaymentService::response);
    }
    private PaymentTransaction findPayment(String paymentId) {
        final UUID paymentUuid;
        try {
            paymentUuid = UUID.fromString(paymentId);
        } catch (IllegalArgumentException exception) {
            throw new IllegalArgumentException("paymentId must be a UUID", exception);
        }
        return paymentRepository.findById(paymentUuid)
                .orElseThrow(() -> new IllegalArgumentException("payment not found"));
    }

    private void recordLedger(PaymentTransaction payment, boolean refund) {
        String transactionId = (refund ? "REFUND-" : "CAPTURE-") + payment.getPaymentId();
        LedgerDirection debitDirection = refund ? LedgerDirection.CREDIT : LedgerDirection.DEBIT;
        LedgerDirection creditDirection = refund ? LedgerDirection.DEBIT : LedgerDirection.CREDIT;
        String paymentId = payment.getPaymentId().toString();

        ledgerRepository.save(new LedgerEntry(
                transactionId + "-A",
                transactionId,
                payment.getOrderId(),
                paymentId,
                "customer_receivable",
                debitDirection,
                payment.getAmountCents(),
                payment.getCurrency()));
        ledgerRepository.save(new LedgerEntry(
                transactionId + "-B",
                transactionId,
                payment.getOrderId(),
                paymentId,
                "merchant_cash_pending",
                creditDirection,
                payment.getAmountCents(),
                payment.getCurrency()));
    }

    private void recordOutbox(String aggregateId, String eventType, String payload) {
        outboxRepository.save(new PaymentOutboxEvent(aggregateId, eventType, payload));
    }

    private void increment(String metricName) {
        Counter.builder(metricName)
                .tag("service", "payment")
                .register(meterRegistry)
                .increment();
        Counter.builder("oms_business_operations")
                .tag("service", "payment")
                .tag("operation", metricName)
                .tag("outcome", metricName.contains("failed") || metricName.contains("unknown") ? "failure" : "success")
                .register(meterRegistry)
                .increment();
    }

    private static PaymentResponse response(PaymentTransaction payment) {
        return new PaymentResponse(
                payment.getPaymentId().toString(),
                payment.getOrderId(),
                payment.getStatus(),
                payment.getAmountCents(),
                payment.getCurrency());
    }

    private static void validate(CapturePaymentCommand command) {
        if (command == null) {
            throw new IllegalArgumentException("capture command is required");
        }
        requireText(command.orderId(), "orderId");
        requireText(command.idempotencyKey(), "idempotencyKey");
        if (command.amountCents() <= 0) {
            throw new IllegalArgumentException("amountCents must be positive");
        }
    }

    private static void ensureSameRequest(
            PaymentTransaction existing,
            CapturePaymentCommand command,
            String currency) {
        boolean differentAmount = existing.getAmountCents() != command.amountCents();
        boolean differentCurrency = !existing.getCurrency().equals(currency);
        boolean differentProviderRef = command.providerRef() != null
                && !command.providerRef().isBlank()
                && !existing.getProviderRef().equals(command.providerRef());
        if (differentAmount || differentCurrency || differentProviderRef) {
            throw new IllegalStateException(
                    "payment idempotency key was reused with different arguments");
        }
    }

    private static String normalizeCurrency(String currency) {
        String normalized = currency == null || currency.isBlank()
                ? "CNY"
                : currency.trim().toUpperCase();
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