package com.poc.payment.entity;

import com.poc.contracts.PaymentStatus;
import jakarta.persistence.*;
import java.time.LocalDateTime;
import java.util.UUID;

/** Payment aggregate; providerRef and order/idempotency are database-enforced unique keys. */
@Entity
@Table(name = "payment_transaction", uniqueConstraints = @UniqueConstraint(
    name = "uk_payment_order_idempotency", columnNames = {"order_id", "idempotency_key"}))
public class PaymentTransaction {
    @Id @GeneratedValue(strategy = GenerationType.UUID) private UUID paymentId;
    @Column(name = "order_id", nullable = false, updatable = false) private String orderId;
    @Column(nullable = false, unique = true, updatable = false) private String providerRef;
    @Column(nullable = false, updatable = false) private String idempotencyKey;
    @Column(nullable = false, updatable = false) private long amountCents;
    @Column(nullable = false, length = 3, updatable = false) private String currency;
    @Enumerated(EnumType.STRING) @Column(nullable = false) private PaymentStatus status;
    @Column(nullable = false, updatable = false) private LocalDateTime createdAt;
    @Column(nullable = false) private LocalDateTime updatedAt;
    @Version private Long version;

    protected PaymentTransaction() { }
    public PaymentTransaction(String orderId, String providerRef, String idempotencyKey, long amountCents,
                              String currency, PaymentStatus status) {
        this.orderId = orderId; this.providerRef = providerRef; this.idempotencyKey = idempotencyKey;
        this.amountCents = amountCents; this.currency = currency; this.status = status;
    }
    @PrePersist void onCreate() { LocalDateTime now = LocalDateTime.now(); createdAt = now; updatedAt = now; }
    @PreUpdate void onUpdate() { updatedAt = LocalDateTime.now(); }
    public UUID getPaymentId() { return paymentId; }
    public String getOrderId() { return orderId; }
    public String getProviderRef() { return providerRef; }
    public String getIdempotencyKey() { return idempotencyKey; }
    public long getAmountCents() { return amountCents; }
    public String getCurrency() { return currency; }
    public PaymentStatus getStatus() { return status; }
    public void setStatus(PaymentStatus status) { this.status = status; }
    public LocalDateTime getCreatedAt() { return createdAt; }
    public LocalDateTime getUpdatedAt() { return updatedAt; }
    public Long getVersion() { return version; }
}