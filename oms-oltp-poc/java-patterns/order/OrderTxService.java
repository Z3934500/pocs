package com.example.oms.order;

import com.example.oms.event.DomainEvent;
import com.example.oms.inventory.InventoryStock;
import com.example.oms.inventory.InventoryStockRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;

/**
 * Inner layer: @Transactional(REQUIRES_NEW) ONLY — no @Retryable.
 *
 * REQUIRES_NEW suspends any ambient transaction and opens a fresh physical
 * transaction. Each call from OrderService.createWithRetry() gets a clean
 * Hibernate session and a clean tx context — no rollback-only flag carried
 * over from a prior failed attempt.
 *
 * OUTBOX vs XADD:
 * The Outbox event is saved in the same transaction as the business data.
 * If any step fails and the transaction rolls back, the outbox row also
 * rolls back. No XADD in Lua; this DB row is the single source of truth.
 * The relay reads the outbox AFTER commit and publishes asynchronously —
 * no blocking I/O inside this transaction (kafkaTemplate.send().get()
 * has been removed for exactly this reason).
 */
@Service
public class OrderTxService {

    private final ReservationRepository    reservationRepo;
    private final InventoryStockRepository stockRepo;
    private final OutboxEventRepository    outboxRepo;

    public OrderTxService(ReservationRepository reservationRepo,
                          InventoryStockRepository stockRepo,
                          OutboxEventRepository outboxRepo) {
        this.reservationRepo = reservationRepo;
        this.stockRepo       = stockRepo;
        this.outboxRepo      = outboxRepo;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public Reservation createInNewTx(CreateOrderRequest req) {

        // 1. Idempotency short-circuit — check BEFORE attempting INSERT.
        //    Returns the existing result transparently; no exception thrown.
        //    Also prevents DataIntegrityViolationException from ever firing
        //    when two requests race with the same idempotencyKey.
        Optional<Reservation> existing =
                reservationRepo.findByIdempotencyKey(req.idempotencyKey());
        if (existing.isPresent()) {
            return existing.get();
        }

        // 2. Pessimistic row lock: SELECT … FOR UPDATE.
        //    Held until this transaction commits — competing threads block
        //    here, not at the INSERT. Combined with stock.reserve(), oversell
        //    is impossible at the DB level regardless of Redis state.
        InventoryStock stock = stockRepo.findBySkuForUpdate(req.sku());
        stock.reserve(req.quantity());  // throws InsufficientStockException if qty > available

        // 3. Persist reservation.
        Reservation reservation = Reservation.builder()
                .orderId(req.orderId())
                .userId(req.userId())
                .sku(req.sku())
                .quantity(req.quantity())
                .idempotencyKey(req.idempotencyKey())
                .build();
        reservationRepo.save(reservation);

        // 4. Outbox event — committed atomically with the reservation above.
        //    No kafkaTemplate.send().get() here; the relay reads this row
        //    after commit and publishes asynchronously. Row lock is released
        //    at commit, not held during Kafka I/O.
        outboxRepo.save(DomainEvent.forReservation(reservation).toOutboxRecord());

        return reservation;
    }
}
