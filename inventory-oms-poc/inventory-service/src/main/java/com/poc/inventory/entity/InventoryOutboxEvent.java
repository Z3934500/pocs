package com.poc.inventory.entity;

import jakarta.persistence.*;
import java.time.LocalDateTime;
import java.util.UUID;

/** Durable event written with inventory state; a relay publishes it after commit. */
@Entity
@Table(name = "inventory_outbox_event")
public class InventoryOutboxEvent {
    public static final String PENDING = "PENDING";
    public static final String IN_FLIGHT = "IN_FLIGHT";
    public static final String PUBLISHED = "PUBLISHED";
    public static final String DEAD_LETTER = "DEAD_LETTER";
    @Id private UUID eventId;
    @Column(nullable = false) private String aggregateId;
    @Column(nullable = false) private String eventType;
    @Column(nullable = false, length = 4000) private String payloadJson;
    @Column(nullable = false) private String status;
    @Column(nullable = false, updatable = false) private LocalDateTime createdAt;
    private LocalDateTime publishedAt;
    @Column(nullable = false) private int attemptCount;
    @Column(nullable = false) private LocalDateTime nextAttemptAt;
    private String leaseId;
    private LocalDateTime leaseUntil;
    @Column(length = 2000) private String lastError;
    protected InventoryOutboxEvent() { }
    public InventoryOutboxEvent(String aggregateId, String eventType, String payloadJson) {
        this.eventId = UUID.randomUUID(); this.aggregateId = aggregateId; this.eventType = eventType;
        this.payloadJson = payloadJson; this.status = PENDING; this.attemptCount = 0;
    }
    @PrePersist void onCreate() {
        createdAt = LocalDateTime.now();
        nextAttemptAt = createdAt;
    }
    public void markPublished() { status = PUBLISHED; publishedAt = LocalDateTime.now(); }
    public UUID getEventId() { return eventId; }
    public String getAggregateId() { return aggregateId; }
    public String getEventType() { return eventType; }
    public String getPayloadJson() { return payloadJson; }
    public String getStatus() { return status; }
    public LocalDateTime getCreatedAt() { return createdAt; }
    public LocalDateTime getPublishedAt() { return publishedAt; }
    public int getAttemptCount() { return attemptCount; }
    public LocalDateTime getNextAttemptAt() { return nextAttemptAt; }
    public String getLeaseId() { return leaseId; }
    public LocalDateTime getLeaseUntil() { return leaseUntil; }
    public String getLastError() { return lastError; }
}