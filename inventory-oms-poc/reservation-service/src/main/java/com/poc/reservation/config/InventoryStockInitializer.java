package com.poc.reservation.config;

import com.poc.reservation.entity.InventoryStock;
import com.poc.reservation.repository.InventoryStockRepository;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@Order(0)
public class InventoryStockInitializer implements CommandLineRunner {

    private final InventoryStockRepository repository;

    public InventoryStockInitializer(InventoryStockRepository repository) {
        this.repository = repository;
    }

    @Override
    public void run(String... args) {
        if (repository.count() > 0) {
            return;
        }
        repository.saveAll(List.of(
            new InventoryStock("SKU-RED-001", 120),
            new InventoryStock("SKU-BLK-002", 80),
            new InventoryStock("SKU-BAT-004", 240)
        ));
    }
}