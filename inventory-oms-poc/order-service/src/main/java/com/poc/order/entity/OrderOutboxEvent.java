package com.poc.order.entity;

import jakarta.persistence.*;
import java.time.LocalDateTime;
import java.util.UUID;

/** Order-owned durable event with lease/retry state for at-least-once delivery. */
@Entity
@Table(name = "order_outbox_event")
public class OrderOutboxEvent {
    public static final String PENDING = "PENDING";
    public static final String IN_FLIGHT = "IN_FLIGHT";
    public static final String PUBLISHED = "PUBLISHED";
    public static final String DEAD_LETTER = "DEAD_LETTER";
    @Id private UUID eventId;
    @Column(nullable = false) private String aggregateId;
    @Column(nullable = false) private String eventType;
    @Column(nullable = false, length = 4000) private String payloadJson;
    /** Optional ordering scope, populated for FIFO inventory commands. */
    private String partitionKey;
    @Column(nullable = false) private String status;
    @Column(nullable = false, updatable = false) private LocalDateTime createdAt;
    private LocalDateTime publishedAt;
    @Column(nullable = false) private int attemptCount;
    @Column(nullable = false) private LocalDateTime nextAttemptAt;
    private String leaseId;
    private LocalDateTime leaseUntil;
    @Column(length = 2000) private String lastError;
    protected OrderOutboxEvent() { }
    public OrderOutboxEvent(String aggregateId, String eventType, String payloadJson) {
        this.eventId=UUID.randomUUID(); this.aggregateId=aggregateId; this.eventType=eventType;
        this.payloadJson=payloadJson; this.status=PENDING; this.attemptCount=0;
    }
    public OrderOutboxEvent(
            String aggregateId, String eventType, String payloadJson, String partitionKey) {
        this(aggregateId, eventType, payloadJson);
        this.partitionKey = partitionKey;
    }
    @PrePersist void onCreate() { createdAt=LocalDateTime.now(); nextAttemptAt=createdAt; }
    public void markPublished() { status=PUBLISHED; publishedAt=LocalDateTime.now(); }
    public UUID getEventId(){return eventId;} public String getAggregateId(){return aggregateId;}
    public String getEventType(){return eventType;} public String getPayloadJson(){return payloadJson;}
    public String getPartitionKey(){return partitionKey;}
    public String getStatus(){return status;} public LocalDateTime getCreatedAt(){return createdAt;}
    public LocalDateTime getPublishedAt(){return publishedAt;} public int getAttemptCount(){return attemptCount;}
    public LocalDateTime getNextAttemptAt(){return nextAttemptAt;} public String getLeaseId(){return leaseId;}
    public LocalDateTime getLeaseUntil(){return leaseUntil;} public String getLastError(){return lastError;}
}
