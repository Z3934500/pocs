package com.poc.inventory;

import com.poc.contracts.*;
import com.poc.inventory.entity.InventoryOutboxEvent;
import com.poc.inventory.entity.InventoryStock;
import com.poc.inventory.messaging.InventoryOutboxRelay;
import com.poc.inventory.repository.*;
import com.poc.inventory.service.InventoryService;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
class InventoryServiceTest {
    @Autowired InventoryService service;
    @Autowired InventoryStockRepository stockRepository;
    @Autowired InventoryReservationRepository reservationRepository;
    @Autowired InventoryOutboxRepository outboxRepository;
    @Autowired InventoryOutboxRelay outboxRelay;

    @BeforeEach void reset() {
        outboxRepository.deleteAll(); reservationRepository.deleteAll(); stockRepository.deleteAll();
        stockRepository.saveAll(List.of(new InventoryStock("SKU-RED-001", 10), new InventoryStock("SKU-BLK-002", 5)));
    }

    @Test void reservationIsIdempotentAndLocksStock() {
        ReserveInventoryCommand command = new ReserveInventoryCommand("ORDER-1", "SKU-RED-001", 2, "reserve-1", 15);
        InventoryReservationResponse first = service.reserve(command);
        InventoryReservationResponse retry = service.reserve(command);
        InventoryStock stock = stockRepository.findById("SKU-RED-001").orElseThrow();
        assertEquals(first.reservationId(), retry.reservationId()); assertEquals(8, stock.getAvailableQty()); assertEquals(2, stock.getReservedQty()); assertEquals(1, outboxRepository.count());
    }

    @Test void commitMovesReservedToSold() {
        service.reserve(new ReserveInventoryCommand("ORDER-2", "SKU-RED-001", 3, "reserve-2", 15));
        assertEquals(ReservationStatus.COMMITTED, service.commit("ORDER-2").status());
        InventoryStock stock = stockRepository.findById("SKU-RED-001").orElseThrow();
        assertEquals(7, stock.getAvailableQty()); assertEquals(0, stock.getReservedQty()); assertEquals(3, stock.getSoldQty());
    }

    @Test void releaseRestoresAvailableStock() {
        service.reserve(new ReserveInventoryCommand("ORDER-3", "SKU-BLK-002", 2, "reserve-3", 15));
        assertEquals(ReservationStatus.RELEASED, service.release("ORDER-3", "payment failed").status());
        InventoryStock stock = stockRepository.findById("SKU-BLK-002").orElseThrow();
        assertEquals(5, stock.getAvailableQty()); assertEquals(0, stock.getReservedQty());
    }

    @Test void seckillQuotaIsSeparatedFromNormalAvailableStock() {
        assertEquals(4, service.allocateSeckillQuota("SKU-RED-001", 4));

        InventoryStock allocated = stockRepository.findById("SKU-RED-001").orElseThrow();
        assertEquals(6, allocated.getAvailableQty());
        assertEquals(4, allocated.getSeckillAllocatedQty());

        service.reserveFromSeckill(new SeckillReserveCommand(
                "ORDER-SECKILL", "USER-1", "SKU-RED-001", 2, "seckill-1"));

        InventoryStock persisted = stockRepository.findById("SKU-RED-001").orElseThrow();
        assertEquals(6, persisted.getAvailableQty());
        assertEquals(2, persisted.getSeckillAllocatedQty());
        assertEquals(2, persisted.getReservedQty());
    }

    @Test void relayClaimsAndPublishesAfterLocalCommit() {
        service.reserve(new ReserveInventoryCommand("ORDER-RELAY", "SKU-RED-001", 1, "reserve-relay", 15));

        outboxRelay.publishPendingEvents();

        assertEquals(InventoryOutboxEvent.PUBLISHED, outboxRepository.findAll().get(0).getStatus());
        assertEquals(1, outboxRepository.findAll().get(0).getAttemptCount());
    }
}
