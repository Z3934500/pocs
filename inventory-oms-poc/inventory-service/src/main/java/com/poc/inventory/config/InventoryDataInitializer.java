package com.poc.inventory.config;

import com.poc.inventory.entity.InventoryStock;
import com.poc.inventory.repository.InventoryStockRepository;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.*;
import java.util.List;

/** Seeds deterministic local stock; production stock is loaded through controlled imports. */
@Configuration
public class InventoryDataInitializer {
    @Bean
    CommandLineRunner seedInventory(InventoryStockRepository repository) {
        return args -> { if (repository.count() == 0) repository.saveAll(List.of(
            new InventoryStock("SKU-RED-001", 120), new InventoryStock("SKU-BLK-002", 80), new InventoryStock("SKU-BAT-004", 240)));
        };
    }
}