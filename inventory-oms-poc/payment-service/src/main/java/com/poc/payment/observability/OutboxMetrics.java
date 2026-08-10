package com.poc.payment.observability;

import com.poc.payment.entity.PaymentOutboxEvent;
import com.poc.payment.repository.PaymentOutboxRepository;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/** Exposes Outbox depth, delivery state and oldest pending age. */
@Component
public class OutboxMetrics {
    private final PaymentOutboxRepository repository;
    private volatile long depth;
    private volatile long pendingCount;
    private volatile long inFlightCount;
    private volatile long deadLetterCount;
    private volatile double oldestAgeSeconds;

    public OutboxMetrics(PaymentOutboxRepository repository, MeterRegistry registry) {
        this.repository = repository;
        Gauge.builder("oms_outbox_depth", this, OutboxMetrics::getDepth)
                .description("Number of pending or in-flight outbox events")
                .tag("service", "payment")
                .register(registry);
        Gauge.builder("oms_outbox_pending_count", this, OutboxMetrics::getPendingCount)
                .description("Number of pending outbox events")
                .tag("service", "payment")
                .register(registry);
        Gauge.builder("oms_outbox_in_flight_count", this, OutboxMetrics::getInFlightCount)
                .description("Number of in-flight outbox events")
                .tag("service", "payment")
                .register(registry);
        Gauge.builder("oms_outbox_dead_letter_count", this, OutboxMetrics::getDeadLetterCount)
                .description("Number of dead-lettered outbox events")
                .tag("service", "payment")
                .register(registry);
        Gauge.builder("oms_outbox_oldest_age_seconds", this, OutboxMetrics::getOldestAgeSeconds)
                .description("Age of the oldest pending or in-flight outbox event")
                .tag("service", "payment")
                .register(registry);
    }

    @Scheduled(
            initialDelayString = "${management.outbox.metrics.initial-delay-ms:5000}",
            fixedDelayString = "${management.outbox.metrics.refresh-ms:10000}")
    public void refresh() {
        List<String> activeStatuses = List.of(PaymentOutboxEvent.PENDING, PaymentOutboxEvent.IN_FLIGHT);
        depth = repository.countByStatusIn(activeStatuses);
        pendingCount = repository.countByStatusIn(List.of(PaymentOutboxEvent.PENDING));
        inFlightCount = repository.countByStatusIn(List.of(PaymentOutboxEvent.IN_FLIGHT));
        deadLetterCount = repository.countByStatusIn(List.of(PaymentOutboxEvent.DEAD_LETTER));

        LocalDateTime oldest = repository.findOldestCreatedAt(activeStatuses);
        oldestAgeSeconds = oldest == null
                ? 0
                : Math.max(0, Duration.between(oldest, LocalDateTime.now()).toMillis() / 1000.0);
    }

    public long getDepth() {
        return depth;
    }

    public long getPendingCount() {
        return pendingCount;
    }

    public long getInFlightCount() {
        return inFlightCount;
    }

    public long getDeadLetterCount() {
        return deadLetterCount;
    }

    public double getOldestAgeSeconds() {
        return oldestAgeSeconds;
    }
}
