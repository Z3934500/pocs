package com.poc.reservation.entity;

import jakarta.persistence.*;
import java.time.LocalDateTime;

@Entity
@Table(name = "inventory_reservation")
public class Reservation {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String orderId;
    private String sku;
    private Integer qty;

    @Column(name = "status")
    private String status; // Pending, Confirmed, Canceled

    private LocalDateTime createdAt;

    public Reservation() {
        this.createdAt = LocalDateTime.now();
    }

    // getters & setters ...
}
