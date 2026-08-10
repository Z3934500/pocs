package com.poc.order.entity;

import jakarta.persistence.*;
import java.time.LocalDateTime;
import java.util.UUID;

/** Audit trail for forward and compensating Saga steps. */
@Entity
@Table(name = "order_saga_step")
public class OrderSagaStep {
    @Id private UUID stepId;
    @Column(nullable = false) private String orderId;
    @Column(nullable = false) private String stepName;
    @Column(nullable = false) private String status;
    @Column(nullable = false, length = 1000) private String detail;
    @Column(nullable = false, updatable = false) private LocalDateTime createdAt;
    protected OrderSagaStep() { }
    public OrderSagaStep(String orderId, String stepName, String status, String detail) { this.stepId=UUID.randomUUID(); this.orderId=orderId; this.stepName=stepName; this.status=status; this.detail=detail; }
    @PrePersist void onCreate(){createdAt=LocalDateTime.now();}
    public UUID getStepId(){return stepId;} public String getOrderId(){return orderId;} public String getStepName(){return stepName;} public String getStatus(){return status;} public String getDetail(){return detail;} public LocalDateTime getCreatedAt(){return createdAt;}
}