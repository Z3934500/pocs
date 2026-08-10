package com.poc.reservation.service;

import com.poc.reservation.dto.EventConsumeResult;
import com.poc.reservation.dto.PaymentRequest;
import com.poc.reservation.dto.PaymentResult;
import com.poc.reservation.entity.InventoryStock;
import com.poc.reservation.entity.LedgerDirection;
import com.poc.reservation.entity.OutboxEvent;
import com.poc.reservation.entity.PaymentStatus;
import com.poc.reservation.entity.Reservation;
import com.poc.reservation.entity.ReservationStatus;
import com.poc.reservation.repository.InboxEventRepository;
import com.poc.reservation.repository.InventoryStockRepository;
import com.poc.reservation.repository.LedgerEntryRepository;
import com.poc.reservation.repository.OutboxEventRepository;
import com.poc.reservation.repository.PaymentTransactionRepository;
import com.poc.reservation.repository.ReservationRepository;
import com.poc.reservation.repository.SagaLogRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
class ReservationServiceTest {

    @Autowired
    private ReservationService service;

    @Autowired
    private ReservationRepository reservationRepository;

    @Autowired
    private InventoryStockRepository stockRepository;

    @Autowired
    private PaymentTransactionRepository paymentRepository;

    @Autowired
    private OutboxEventRepository outboxRepository;

    @Autowired
    private SagaLogRepository sagaLogRepository;

    @Autowired
    private LedgerEntryRepository ledgerRepository;

    @Autowired
    private InboxEventRepository inboxRepository;

    @BeforeEach
    void resetData() {
        inboxRepository.deleteAll();
        ledgerRepository.deleteAll();
        paymentRepository.deleteAll();
        sagaLogRepository.deleteAll();
        outboxRepository.deleteAll();
        reservationRepository.deleteAll();
        stockRepository.deleteAll();
        stockRepository.saveAll(List.of(
            new InventoryStock("SKU-RED-001", 120),
            new InventoryStock("SKU-BLK-002", 80)
        ));
    }

    @Test
    void reservationUsesDatabaseIdempotencyAndUpdatesLockedStock() {
        Reservation first = service.create("ORDER-001", "SKU-RED-001", 2, "idem-001", 15);
        Reservation retry = service.create("ORDER-001", "SKU-RED-001", 2, "idem-001", 15);

        InventoryStock stock = stockRepository.findById("SKU-RED-001").orElseThrow();
        assertEquals(first.getId(), retry.getId());
        assertEquals(ReservationStatus.RESERVED, first.getStatus());
        assertEquals(118, stock.getAvailableQty());
        assertEquals(2, stock.getReservedQty());
        assertEquals(1, outboxRepository.count());
    }

    @Test
    void successfulPaymentCommitsSagaAndWritesBalancedLedger() {
        service.create("ORDER-002", "SKU-RED-001", 3, "idem-002", 15, 3000, "CNY");

        PaymentResult result = service.capturePayment(
            "ORDER-002",
            new PaymentRequest("pay-idem-002", "provider-002", 3000, "CNY", true)
        );
        PaymentResult retry = service.capturePayment(
            "ORDER-002",
            new PaymentRequest("pay-idem-002", "provider-002", 3000, "CNY", true)
        );

        InventoryStock stock = stockRepository.findById("SKU-RED-001").orElseThrow();
        Set<LedgerDirection> directions = ledgerRepository.findAll().stream()
            .map(entry -> entry.getDirection())
            .collect(Collectors.toSet());

        assertEquals(PaymentStatus.CAPTURED, result.payment().getStatus());
        assertEquals(ReservationStatus.COMMITTED, result.reservation().getStatus());
        assertEquals(result.payment().getPaymentId(), retry.payment().getPaymentId());
        assertEquals(117, stock.getAvailableQty());
        assertEquals(0, stock.getReservedQty());
        assertEquals(3, stock.getSoldQty());
        assertEquals(2, ledgerRepository.count());
        assertEquals(Set.of(LedgerDirection.DEBIT, LedgerDirection.CREDIT), directions);
        assertEquals(4, outboxRepository.count());
        assertTrue(sagaLogRepository.count() >= 3);
    }

    @Test
    void failedPaymentRunsCompensationAndDoesNotWriteLedger() {
        service.create("ORDER-003", "SKU-BLK-002", 4, "idem-003", 15, 4000, "CNY");

        PaymentResult result = service.capturePayment(
            "ORDER-003",
            new PaymentRequest("pay-idem-003", "provider-003", 4000, "CNY", false)
        );

        InventoryStock stock = stockRepository.findById("SKU-BLK-002").orElseThrow();
        assertEquals(PaymentStatus.FAILED, result.payment().getStatus());
        assertEquals(ReservationStatus.RELEASED, result.reservation().getStatus());
        assertEquals(80, stock.getAvailableQty());
        assertEquals(0, stock.getReservedQty());
        assertEquals(0, ledgerRepository.count());
        assertTrue(outboxRepository.findAll().stream()
            .map(OutboxEvent::getEventType)
            .anyMatch("payment.failed"::equals));
    }

    @Test
    void paymentAmountMustMatchOrderSnapshot() {
        Reservation reservation = service.create("ORDER-006", "SKU-RED-001", 1, "idem-006", 15, 1000, "CNY");

        assertThrows(IllegalStateException.class, () -> service.capturePayment(
            "ORDER-006",
            new PaymentRequest("pay-idem-006", "provider-006", 900, "CNY", true)
        ));
        assertEquals(ReservationStatus.RESERVED,
            reservationRepository.findById(reservation.getId()).orElseThrow().getStatus());
    }

    @Test
    void expiredReservationReleasesStockThroughCompensationPath() {
        Reservation reservation = service.create("ORDER-005", "SKU-RED-001", 5, "idem-005", 15);
        reservation.setExpiresAt(LocalDateTime.now().minusMinutes(1));
        reservationRepository.save(reservation);

        assertEquals(1, service.expireReservations());
        assertEquals(ReservationStatus.EXPIRED,
            reservationRepository.findById(reservation.getId()).orElseThrow().getStatus());
        InventoryStock stock = stockRepository.findById("SKU-RED-001").orElseThrow();
        assertEquals(120, stock.getAvailableQty());
        assertEquals(0, stock.getReservedQty());
        assertTrue(outboxRepository.findAll().stream()
            .map(OutboxEvent::getEventType)
            .anyMatch("reservation.timeout"::equals));
    }

    @Test
    void outboxConsumerUsesInboxForDuplicateDelivery() {
        service.create("ORDER-004", "SKU-RED-001", 1, "idem-004", 15);
        OutboxEvent event = service.pendingOutbox().get(0);

        EventConsumeResult first = service.consumeEvent(event.getEventId());
        EventConsumeResult second = service.consumeEvent(event.getEventId());

        assertTrue(first.processed());
        assertFalse(second.processed());
        assertEquals(1, inboxRepository.count());
        assertNotNull(first.message());
    }
}