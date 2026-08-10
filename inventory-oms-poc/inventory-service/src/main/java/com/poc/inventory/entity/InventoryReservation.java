package com.poc.inventory.entity;

import com.poc.contracts.ReservationStatus;
import jakarta.persistence.*;
import java.time.LocalDateTime;
import java.util.UUID;

/** Inventory-owned reservation; Order Service references it but never writes this table. */
@Entity
@Table(name = "inventory_reservation", uniqueConstraints = {
    @UniqueConstraint(name = "uk_inventory_reservation_order", columnNames = "order_id"),
    @UniqueConstraint(name = "uk_inventory_reservation_idempotency", columnNames = "idempotency_key")
})
public class InventoryReservation {
    @Id @GeneratedValue(strategy = GenerationType.UUID) private UUID reservationId;
    @Column(name = "order_id", nullable = false, updatable = false) private String orderId;
    @Column(nullable = false, updatable = false) private String sku;
    @Column(nullable = false, updatable = false) private int qty;
    @Enumerated(EnumType.STRING) @Column(nullable = false) private ReservationStatus status;
    @Column(nullable = false, updatable = false) private String idempotencyKey;
    @Column(nullable = false) private LocalDateTime expiresAt;
    @Column(nullable = false, updatable = false) private LocalDateTime createdAt;
    @Column(nullable = false) private LocalDateTime updatedAt;
    @Version private Long version;

    protected InventoryReservation() { }
    public InventoryReservation(String orderId, String sku, int qty, String idempotencyKey, LocalDateTime expiresAt) {
        this.orderId = orderId; this.sku = sku; this.qty = qty; this.idempotencyKey = idempotencyKey;
        this.expiresAt = expiresAt; this.status = ReservationStatus.RESERVED;
    }
    @PrePersist void onCreate() { LocalDateTime now = LocalDateTime.now(); createdAt = now; updatedAt = now; }
    @PreUpdate void onUpdate() { updatedAt = LocalDateTime.now(); }
    public UUID getReservationId() { return reservationId; }
    public String getOrderId() { return orderId; }
    public String getSku() { return sku; }
    public int getQty() { return qty; }
    public ReservationStatus getStatus() { return status; }
    public void setStatus(ReservationStatus status) { this.status = status; }
    public String getIdempotencyKey() { return idempotencyKey; }
    public LocalDateTime getExpiresAt() { return expiresAt; }
    public LocalDateTime getCreatedAt() { return createdAt; }
    public LocalDateTime getUpdatedAt() { return updatedAt; }
    public Long getVersion() { return version; }
}