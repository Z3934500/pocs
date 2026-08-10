package com.poc.reservation.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.PrePersist;
import jakarta.persistence.Table;

import java.time.LocalDateTime;
import java.util.UUID;

@Entity
@Table(name = "saga_log")
public class SagaLog {

    @Id
    private String logId;

    @Column(nullable = false)
    private String orderId;

    @Column(nullable = false)
    private String stepName;

    @Column(nullable = false)
    private String status;

    @Column(nullable = false, length = 1000)
    private String message;

    @Column(nullable = false, updatable = false)
    private LocalDateTime createdAt;

    protected SagaLog() {
    }

    public SagaLog(String orderId, String stepName, String status, String message) {
        this.logId = UUID.randomUUID().toString();
        this.orderId = orderId;
        this.stepName = stepName;
        this.status = status;
        this.message = message;
    }

    @PrePersist
    void onCreate() {
        createdAt = LocalDateTime.now();
    }

    public String getLogId() { return logId; }
    public String getOrderId() { return orderId; }
    public String getStepName() { return stepName; }
    public String getStatus() { return status; }
    public String getMessage() { return message; }
    public LocalDateTime getCreatedAt() { return createdAt; }
}