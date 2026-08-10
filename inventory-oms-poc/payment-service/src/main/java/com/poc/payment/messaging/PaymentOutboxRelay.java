package com.poc.payment.messaging;

import com.poc.contracts.DomainEvent;
import com.poc.payment.entity.PaymentOutboxEvent;
import com.poc.payment.repository.PaymentOutboxRepository;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.PageRequest;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/** Lease-based, at-least-once relay safe for multiple Payment Service replicas. */
@Component
public class PaymentOutboxRelay {
    private final PaymentOutboxRepository outboxRepository;
    private final DomainEventPublisher publisher;
    private final int batchSize;
    private final long leaseMs;
    private final int maxAttempts;
    private final long retryBaseMs;
    private final long retryMaxMs;

    public PaymentOutboxRelay(
            PaymentOutboxRepository outboxRepository,
            DomainEventPublisher publisher,
            @Value("${messaging.outbox.batch-size:100}") int batchSize,
            @Value("${messaging.outbox.lease-ms:30000}") long leaseMs,
            @Value("${messaging.outbox.max-attempts:8}") int maxAttempts,
            @Value("${messaging.outbox.retry-base-ms:1000}") long retryBaseMs,
            @Value("${messaging.outbox.retry-max-ms:60000}") long retryMaxMs) {
        this.outboxRepository = outboxRepository; this.publisher = publisher; this.batchSize = batchSize;
        this.leaseMs = leaseMs; this.maxAttempts = maxAttempts; this.retryBaseMs = retryBaseMs; this.retryMaxMs = retryMaxMs;
    }

    @Scheduled(fixedDelayString = "${messaging.outbox.poll-ms:1000}")
    public void publishPendingEvents() {
        LocalDateTime now = LocalDateTime.now();
        for (ClaimedEvent item : claim(now)) {
            try {
                publisher.publish(toDomainEvent(item.event));
                outboxRepository.markPublished(item.event.getEventId(), PaymentOutboxEvent.IN_FLIGHT,
                        PaymentOutboxEvent.PUBLISHED, item.leaseId, LocalDateTime.now());
            } catch (RuntimeException exception) {
                outboxRepository.markFailed(item.event.getEventId(), PaymentOutboxEvent.IN_FLIGHT,
                        PaymentOutboxEvent.PENDING, PaymentOutboxEvent.DEAD_LETTER, item.leaseId,
                        LocalDateTime.now().plusNanos(retryDelay(item.event.getAttemptCount() + 1) * 1_000_000L),
                        errorMessage(exception), maxAttempts);
            }
        }
    }

    private List<ClaimedEvent> claim(LocalDateTime now) {
        List<ClaimedEvent> claimed = new ArrayList<>();
        List<PaymentOutboxEvent> candidates = outboxRepository.findClaimable(PaymentOutboxEvent.PENDING,
                PaymentOutboxEvent.IN_FLIGHT, now, PageRequest.of(0, batchSize));
        for (PaymentOutboxEvent event : candidates) {
            String leaseId = UUID.randomUUID().toString();
            if (outboxRepository.tryClaim(event.getEventId(), PaymentOutboxEvent.PENDING,
                    PaymentOutboxEvent.IN_FLIGHT, leaseId, now.plusNanos(leaseMs * 1_000_000L), now) == 1) {
                claimed.add(new ClaimedEvent(event, leaseId));
            }
        }
        return claimed;
    }

    private DomainEvent toDomainEvent(PaymentOutboxEvent event) {
        return new DomainEvent(event.getEventId().toString(), "payment", event.getAggregateId(),
                event.getEventType(), event.getPayloadJson(), 1,
                event.getCreatedAt().toInstant(ZoneOffset.UTC), event.getAggregateId());
    }

    private long retryDelay(int attempt) {
        long delay = retryBaseMs;
        for (int i=1; i<attempt && delay<retryMaxMs; i++) {
            delay = Math.min(retryMaxMs, delay > retryMaxMs / 2 ? retryMaxMs : delay * 2);
        }
        return Math.min(retryMaxMs, delay);
    }

    private static String errorMessage(RuntimeException exception) {
        String message = exception.getMessage();
        if (message == null || message.isBlank()) message = exception.getClass().getSimpleName();
        return message.length() <= 2000 ? message : message.substring(0, 2000);
    }

    private record ClaimedEvent(PaymentOutboxEvent event, String leaseId) { }
}