package com.poc.order.messaging;

import com.poc.contracts.DomainEvent;
import com.poc.order.entity.OrderOutboxEvent;
import com.poc.order.repository.OrderOutboxRepository;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.data.domain.PageRequest;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/** Lease-based, at-least-once relay safe for multiple Order Service replicas. */
@Component
public class OrderOutboxRelay {
    private static final List<String> INVENTORY_RESERVATION_EVENTS = List.of(
            "inventory.reservation.requested",
            "inventory.reservation.cancel_requested");
    private final OrderOutboxRepository repository;
    private final DomainEventPublisher publisher;
    private final ObjectProvider<InventoryReservationCommandPublisher> reservationCommandPublisher;
    private final int batchSize;
    private final long leaseMs;
    private final int maxAttempts;
    private final long retryBaseMs;
    private final long retryMaxMs;

    public OrderOutboxRelay(
            OrderOutboxRepository repository,
            DomainEventPublisher publisher,
            ObjectProvider<InventoryReservationCommandPublisher> reservationCommandPublisher,
            @Value("${messaging.outbox.batch-size:100}") int batchSize,
            @Value("${messaging.outbox.lease-ms:30000}") long leaseMs,
            @Value("${messaging.outbox.max-attempts:8}") int maxAttempts,
            @Value("${messaging.outbox.retry-base-ms:1000}") long retryBaseMs,
            @Value("${messaging.outbox.retry-max-ms:60000}") long retryMaxMs) {
        this.repository=repository; this.publisher=publisher;
        this.reservationCommandPublisher=reservationCommandPublisher; this.batchSize=batchSize;
        this.leaseMs=leaseMs; this.maxAttempts=maxAttempts; this.retryBaseMs=retryBaseMs; this.retryMaxMs=retryMaxMs;
    }

    @Scheduled(fixedDelayString="${messaging.outbox.poll-ms:1000}")
    public void publishPendingEvents() {
        LocalDateTime now = LocalDateTime.now();
        for (ClaimedEvent item : claim(now)) {
            try {
                publish(item.event);
                repository.markPublished(item.event.getEventId(), OrderOutboxEvent.IN_FLIGHT,
                        OrderOutboxEvent.PUBLISHED, item.leaseId, LocalDateTime.now());
            } catch (RuntimeException exception) {
                repository.markFailed(item.event.getEventId(), OrderOutboxEvent.IN_FLIGHT,
                        OrderOutboxEvent.PENDING, OrderOutboxEvent.DEAD_LETTER, item.leaseId,
                        LocalDateTime.now().plusNanos(retryDelay(item.event.getAttemptCount() + 1) * 1_000_000L),
                        errorMessage(exception), maxAttempts);
            }
        }
    }

    private void publish(OrderOutboxEvent event) {
        if (event.getEventType().equals("inventory.reservation.requested")
                || event.getEventType().equals("inventory.reservation.cancel_requested")) {
            InventoryReservationCommandPublisher commandPublisher =
                    reservationCommandPublisher.getIfAvailable();
            if (commandPublisher == null) {
                throw new IllegalStateException(
                        "inventory reservation queue publisher is not configured");
            }
            commandPublisher.publish(event);
            return;
        }
        publisher.publish(toDomainEvent(event));
    }

    private boolean isInventoryReservationEvent(OrderOutboxEvent event) {
        return INVENTORY_RESERVATION_EVENTS.contains(event.getEventType());
    }

    private List<ClaimedEvent> claim(LocalDateTime now) {
        List<ClaimedEvent> claimed = new ArrayList<>();
        List<OrderOutboxEvent> candidates = repository.findClaimable(OrderOutboxEvent.PENDING,
                OrderOutboxEvent.IN_FLIGHT, now, PageRequest.of(0, batchSize));
        for (OrderOutboxEvent event : candidates) {
            if (isInventoryReservationEvent(event)
                    && event.getPartitionKey() != null
                    && repository.countEarlierUnpublishedReservationCommands(
                            INVENTORY_RESERVATION_EVENTS,
                            event.getPartitionKey(),
                            event.getCreatedAt(),
                            OrderOutboxEvent.PUBLISHED) > 0) {
                continue;
            }
            String leaseId = UUID.randomUUID().toString();
            if (repository.tryClaim(event.getEventId(), OrderOutboxEvent.PENDING,
                    OrderOutboxEvent.IN_FLIGHT, leaseId, now.plusNanos(leaseMs * 1_000_000L), now) == 1) {
                claimed.add(new ClaimedEvent(event, leaseId));
            }
        }
        return claimed;
    }

    private DomainEvent toDomainEvent(OrderOutboxEvent event) {
        return new DomainEvent(event.getEventId().toString(), "order", event.getAggregateId(),
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

    private record ClaimedEvent(OrderOutboxEvent event, String leaseId) { }
}
