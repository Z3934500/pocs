package com.poc.inventory.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.PrePersist;
import jakarta.persistence.PreUpdate;
import jakarta.persistence.Table;
import jakarta.persistence.Version;
import java.time.LocalDateTime;

/** Inventory aggregate root; quantity mutations happen while this row is locked. */
@Entity
@Table(name = "inventory_stock")
public class InventoryStock {
    @Id private String sku;
    @Column(nullable = false) private int availableQty;
    @Column(nullable = false) private int reservedQty;
    @Column(nullable = false) private int soldQty;
    @Column(nullable = false) private int seckillAllocatedQty;
    @Column(nullable = false) private LocalDateTime updatedAt;
    @Version private Long version;

    protected InventoryStock() { }
    public InventoryStock(String sku, int availableQty) { this.sku = sku; this.availableQty = availableQty; }

    public void reserve(int qty) {
        if (qty <= 0) throw new IllegalArgumentException("qty must be positive");
        if (availableQty < qty) throw new IllegalStateException("insufficient stock for sku=" + sku);
        availableQty -= qty; reservedQty += qty;
    }

    /**
     * Moves a bounded quota out of normal inventory before Redis becomes the
     * high-concurrency admission point. Normal reservations cannot consume
     * this quota while it is waiting in the Redis stream.
     */
    public void allocateSeckillQuota(int qty) {
        if (qty <= 0 || availableQty < qty) {
            throw new IllegalStateException("insufficient normal stock for seckill quota, sku=" + sku);
        }
        if (seckillAllocatedQty > 0) {
            throw new IllegalStateException("seckill quota already open for sku=" + sku);
        }
        availableQty -= qty;
        seckillAllocatedQty += qty;
    }

    /** Converts a successful Redis admission into a DB reservation. */
    public void acceptSeckillReservation(int qty) {
        if (qty <= 0 || seckillAllocatedQty < qty) {
            throw new IllegalStateException("seckill quota mismatch for sku=" + sku);
        }
        seckillAllocatedQty -= qty;
        reservedQty += qty;
    }

    /** Returns an unconsumed Redis admission to normal inventory. */
    public void releaseSeckillQuota(int qty) {
        if (qty <= 0 || seckillAllocatedQty < qty) {
            throw new IllegalStateException("seckill quota mismatch for sku=" + sku);
        }
        seckillAllocatedQty -= qty;
        availableQty += qty;
    }
    public void commit(int qty) {
        if (qty <= 0 || reservedQty < qty) throw new IllegalStateException("reserved stock mismatch for sku=" + sku);
        reservedQty -= qty; soldQty += qty;
    }
    public void release(int qty) {
        if (qty <= 0 || reservedQty < qty) throw new IllegalStateException("reserved stock mismatch for sku=" + sku);
        reservedQty -= qty; availableQty += qty;
    }
    @PrePersist @PreUpdate void touch() { updatedAt = LocalDateTime.now(); }
    public String getSku() { return sku; }
    public int getAvailableQty() { return availableQty; }
    public int getReservedQty() { return reservedQty; }
    public int getSoldQty() { return soldQty; }
    public int getSeckillAllocatedQty() { return seckillAllocatedQty; }
    public LocalDateTime getUpdatedAt() { return updatedAt; }
    public Long getVersion() { return version; }
}
