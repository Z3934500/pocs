package com.poc.order.entity;

import com.poc.contracts.OrderStatus;
import jakarta.persistence.*;
import java.time.LocalDateTime;

/** Order aggregate; the Order Service owns this table and its state transitions. */
@Entity
@Table(name = "customer_order", uniqueConstraints = @UniqueConstraint(name = "uk_order_idempotency", columnNames = "idempotency_key"))
public class OrderAggregate {
    @Id private String orderId;
    @Column(nullable = false, updatable = false) private String sku;
    @Column(nullable = false, updatable = false) private int qty;
    @Column(nullable = false, updatable = false) private long amountCents;
    @Column(nullable = false, length = 3, updatable = false) private String currency;
    @Enumerated(EnumType.STRING) @Column(nullable = false) private OrderStatus status;
    @Column(nullable = false, updatable = false) private String idempotencyKey;
    private String reservationId;
    private String paymentId;
    /** Persisted so an asynchronous FIFO reservation result can resume Saga payment. */
    private String paymentIdempotencyKey;
    private String providerRef;
    private Boolean paymentSucceed;
    @Column(nullable = false, updatable = false) private LocalDateTime createdAt;
    @Column(nullable = false) private LocalDateTime updatedAt;
    @Version private Long version;

    protected OrderAggregate() { }
    public OrderAggregate(String orderId, String sku, int qty, long amountCents, String currency, String idempotencyKey) {
        this.orderId = orderId; this.sku = sku; this.qty = qty; this.amountCents = amountCents; this.currency = currency;
        this.idempotencyKey = idempotencyKey; this.status = OrderStatus.PENDING;
    }
    @PrePersist void onCreate() { LocalDateTime now = LocalDateTime.now(); createdAt = now; updatedAt = now; }
    @PreUpdate void onUpdate() { updatedAt = LocalDateTime.now(); }
    public String getOrderId() { return orderId; }
    public String getSku() { return sku; }
    public int getQty() { return qty; }
    public long getAmountCents() { return amountCents; }
    public String getCurrency() { return currency; }
    public OrderStatus getStatus() { return status; }
    public void setStatus(OrderStatus status) { this.status = status; }
    public String getIdempotencyKey() { return idempotencyKey; }
    public String getReservationId() { return reservationId; }
    public void setReservationId(String reservationId) { this.reservationId = reservationId; }
    public String getPaymentId() { return paymentId; }
    public void setPaymentId(String paymentId) { this.paymentId = paymentId; }
    public String getPaymentIdempotencyKey() { return paymentIdempotencyKey; }
    public void setPaymentIdempotencyKey(String value) { this.paymentIdempotencyKey = value; }
    public String getProviderRef() { return providerRef; }
    public void setProviderRef(String value) { this.providerRef = value; }
    public boolean isPaymentSucceed() { return paymentSucceed == null || paymentSucceed; }
    public void setPaymentSucceed(Boolean value) { this.paymentSucceed = value; }
    public LocalDateTime getCreatedAt() { return createdAt; }
    public LocalDateTime getUpdatedAt() { return updatedAt; }
    public Long getVersion() { return version; }
}
