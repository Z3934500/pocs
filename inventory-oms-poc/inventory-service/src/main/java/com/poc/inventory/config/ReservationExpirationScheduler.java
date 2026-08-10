package com.poc.inventory.config;

import com.poc.inventory.service.InventoryService;
import org.slf4j.*;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/** Releases reservations that exceed their business TTL. */
@Component
public class ReservationExpirationScheduler {
    private static final Logger log = LoggerFactory.getLogger(ReservationExpirationScheduler.class);
    private final InventoryService inventoryService;
    public ReservationExpirationScheduler(InventoryService inventoryService) { this.inventoryService = inventoryService; }
    @Scheduled(fixedDelayString = "${inventory.reservation.expiration-scan-ms:60000}")
    public void releaseExpiredReservations() {
        int count = inventoryService.expireReservations();
        if (count > 0) log.info("Released {} expired inventory reservations", count);
    }
}