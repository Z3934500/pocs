package com.poc.contracts;

import java.time.Instant;

/**
 * Transport-neutral event envelope. Kafka, SQS or another broker can carry this envelope.
 */
public record DomainEvent(
    String eventId,
    String aggregateType,
    String aggregateId,
    String eventType,
    String payloadJson,
    int schemaVersion,
    Instant occurredAt,
    String partitionKey
) {

    /** Backward-compatible constructor for callers that do not set metadata. */
    public DomainEvent(
            String eventId,
            String aggregateType,
            String aggregateId,
            String eventType,
            String payloadJson) {
        this(eventId, aggregateType, aggregateId, eventType, payloadJson,
                1, Instant.now(), aggregateId);
    }

    /** Kafka record key and SQS FIFO MessageGroupId must be stable per aggregate. */
    public String kafkaKey() {
        return partitionKey;
    }

    public String sqsMessageGroupId() {
        return partitionKey;
    }

    /** Broker-side deduplication is only an optimization; Inbox remains authoritative. */
    public String sqsMessageDeduplicationId() {
        return eventId;
    }
}
