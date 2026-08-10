package com.poc.reservation.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.PrePersist;
import jakarta.persistence.PreUpdate;
import jakarta.persistence.Table;
import jakarta.persistence.Version;

import java.time.LocalDateTime;

@Entity
@Table(name = "inventory_stock")
public class InventoryStock {

    @Id
    private String sku;

    @Column(nullable = false)
    private int availableQty;

    @Column(nullable = false)
    private int reservedQty;

    @Column(nullable = false)
    private int soldQty;

    @Column(nullable = false)
    private LocalDateTime updatedAt;

    @Version
    private Long version;

    protected InventoryStock() {
    }

    public InventoryStock(String sku, int availableQty) {
        this.sku = sku;
        this.availableQty = availableQty;
        this.updatedAt = LocalDateTime.now();
    }

    public void reserve(int qty) {
        if (qty <= 0 || availableQty < qty) {
            throw new IllegalStateException("insufficient stock for sku=" + sku);
        }
        availableQty -= qty;
        reservedQty += qty;
    }

    public void commit(int qty) {
        if (qty <= 0 || reservedQty < qty) {
            throw new IllegalStateException("reserved stock mismatch for sku=" + sku);
        }
        reservedQty -= qty;
        soldQty += qty;
    }

    public void release(int qty) {
        if (qty <= 0 || reservedQty < qty) {
            throw new IllegalStateException("reserved stock mismatch for sku=" + sku);
        }
        reservedQty -= qty;
        availableQty += qty;
    }

    @PrePersist
    @PreUpdate
    void touch() {
        updatedAt = LocalDateTime.now();
    }

    public String getSku() { return sku; }
    public int getAvailableQty() { return availableQty; }
    public int getReservedQty() { return reservedQty; }
    public int getSoldQty() { return soldQty; }
    public LocalDateTime getUpdatedAt() { return updatedAt; }
    public Long getVersion() { return version; }
}