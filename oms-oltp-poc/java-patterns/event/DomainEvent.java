package com.example.oms.event;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.time.Instant;
import java.util.UUID;

/**
 * Transport-neutral domain event.
 *
 * toJson() delegates to Jackson — no hand-built strings.
 * Eliminates an entire class of silent runtime errors: a typo in
 * String.format() produces malformed JSON that only fails at the consumer
 * when Jackson tries to deserialize it. Jackson failures are compile-time
 * (field names, types) or throw clearly at the call site.
 *
 * schemaVersion is a forward-compatibility hook. Consumers route on it:
 *   v1 → direct deserialize to ReservationCreatedV1
 *   v2 → run migration logic, then deserialize to ReservationCreatedV2
 * This is the minimal migration path before a Schema Registry is wired in.
 *
 * partitionKey drives ordering guarantees:
 *   Kafka: kafkaTemplate.send(topic, partitionKey, json)
 *   SQS FIFO: MessageGroupId = partitionKey
 */
public record DomainEvent(
    String  eventId,
    String  aggregateType,
    String  eventType,
    String  aggregateId,
    String  idempotencyKey,
    int     schemaVersion,
    Instant occurredAt,
    String  partitionKey,
    Object  payload
) {
    private static final ObjectMapper MAPPER =
            new ObjectMapper().registerModule(new JavaTimeModule());

    /** Jackson serialisation — throws loudly on failure, never silently. */
    public String toJson() {
        try {
            return MAPPER.writeValueAsString(this);
        } catch (Exception e) {
            throw new IllegalStateException(
                    "DomainEvent serialisation failed for eventId=" + eventId, e);
        }
    }

    public OutboxRecord toOutboxRecord() {
        return new OutboxRecord(eventId, aggregateType, eventType,
                                aggregateId, toJson(), schemaVersion, occurredAt);
    }

    public static DomainEvent forReservation(Reservation r) {
        return new DomainEvent(
            UUID.randomUUID().toString(),
            "Reservation",
            "RESERVATION_CREATED",
            r.getId(),
            r.getIdempotencyKey(),
            1,
            Instant.now(),
            r.getOrderId(),   // partition by orderId → ordering per order guaranteed
            r
        );
    }
}
