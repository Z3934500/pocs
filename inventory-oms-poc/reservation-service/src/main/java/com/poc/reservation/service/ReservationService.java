package com.poc.reservation.service;

import com.poc.reservation.entity.Reservation;
import com.poc.reservation.repository.ReservationRepository;
import org.springframework.stereotype.Service;

@Service
public class ReservationService {

    private final ReservationRepository repo;

    public ReservationService(ReservationRepository repo) {
        this.repo = repo;
    }

    public Reservation create(String orderId, String sku, int qty) {
        Reservation r = new Reservation();
        r.setOrderId(orderId);
        r.setSku(sku);
        r.setQty(qty);
        r.setStatus("Pending");
        return repo.save(r);
    }
}
