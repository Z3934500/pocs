package com.poc.reservation.controller;

import com.poc.reservation.entity.Reservation;
import com.poc.reservation.service.ReservationService;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/reservation")
public class ReservationController {

    private final ReservationService service;

    public ReservationController(ReservationService service) {
        this.service = service;
    }

    @PostMapping("/create")
    public Reservation create(@RequestParam String orderId,
                              @RequestParam String sku,
                              @RequestParam int qty) {
        return service.create(orderId, sku, qty);
    }
}
