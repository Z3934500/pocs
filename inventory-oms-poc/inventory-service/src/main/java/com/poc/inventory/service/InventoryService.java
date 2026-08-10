package com.poc.inventory.service;

import com.poc.contracts.InventoryReservationResponse;
import com.poc.contracts.ReservationStatus;
import com.poc.contracts.ReserveInventoryCommand;
import com.poc.contracts.SeckillReserveCommand;
import com.poc.inventory.entity.InventoryOutboxEvent;
import com.poc.inventory.entity.InventoryReservation;
import com.poc.inventory.entity.InventoryStock;
import com.poc.inventory.repository.InventoryOutboxRepository;
import com.poc.inventory.repository.InventoryReservationRepository;
import com.poc.inventory.repository.InventoryStockRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.LocalDateTime;
import java.util.List;
import org.springframework.dao.CannotAcquireLockException;
import org.springframework.dao.DeadlockLoserDataAccessException;
import org.springframework.dao.PessimisticLockingFailureException;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Application service for the Inventory bounded context.
 *
 * <p>This class is the only writer of stock and reservation state. A SKU row
 * lock serializes competing reservations, while the local transaction also
 * persists the corresponding Outbox event.</p>
 */
@Service
public class InventoryService {

    private final InventoryStockRepository stockRepository;
    private final InventoryReservationRepository reservationRepository;
    private final InventoryOutboxRepository outboxRepository;
    private final MeterRegistry meterRegistry;

    public InventoryService(
            InventoryStockRepository stockRepository,
            InventoryReservationRepository reservationRepository,
            InventoryOutboxRepository outboxRepository,
            MeterRegistry meterRegistry) {
        this.stockRepository = stockRepository;
        this.reservationRepository = reservationRepository;
        this.outboxRepository = outboxRepository;
        this.meterRegistry = meterRegistry;
    }

    /**
     * Reserves stock in a local transaction protected by a pessimistic SKU row lock.
     * Repeated commands with the same idempotency key return the original reservation.
     */
    @Transactional
    @Retryable(
            retryFor = {
                CannotAcquireLockException.class,
                DeadlockLoserDataAccessException.class,
                PessimisticLockingFailureException.class
            },
            maxAttempts = 3,
            backoff = @Backoff(delay = 50, multiplier = 2.0))
    public InventoryReservationResponse reserve(ReserveInventoryCommand command) {
        validate(command);

        InventoryReservation existing = reservationRepository
                .findByIdempotencyKey(command.idempotencyKey())
                .orElse(null);
        if (existing != null) {
            ensureSameRequest(existing, command);
            return response(existing);
        }

        if (reservationRepository.findByOrderIdForUpdate(command.orderId()).isPresent()) {
            throw new IllegalStateException(
                    "order already has an inventory reservation: " + command.orderId());
        }

        InventoryStock stock = stockRepository.findBySkuForUpdate(command.sku())
                .orElseThrow(() -> new IllegalArgumentException("unknown sku=" + command.sku()));
        stock.reserve(command.qty());

        InventoryReservation reservation = new InventoryReservation(
                command.orderId(),
                command.sku(),
                command.qty(),
                command.idempotencyKey(),
                LocalDateTime.now().plusMinutes(command.ttlMinutes()));
        reservationRepository.save(reservation);
        recordOutbox(
                command.orderId(),
                "inventory.reserved",
                json(command.orderId(), command.sku(), command.qty()));
        increment("inventory.reservation.created");
        return response(reservation);
    }

    /**
     * Removes a bounded quota from normal inventory before the Redis hot path
     * is opened. This prevents normal DB reservations from racing with Redis
     * admissions for the same units.
     */
    @Transactional
    @Retryable(
            retryFor = {
                CannotAcquireLockException.class,
                DeadlockLoserDataAccessException.class,
                PessimisticLockingFailureException.class
            },
            maxAttempts = 3,
            backoff = @Backoff(delay = 50, multiplier = 2.0))
    public int allocateSeckillQuota(String sku, int qty) {
        requireText(sku, "sku");
        if (qty <= 0) {
            throw new IllegalArgumentException("qty must be positive");
        }
        InventoryStock stock = stockRepository.findBySkuForUpdate(sku)
                .orElseThrow(() -> new IllegalArgumentException("unknown sku=" + sku));
        stock.allocateSeckillQuota(qty);
        return stock.getSeckillAllocatedQty();
    }

    /**
     * Persists an admission already atomically accepted by Redis/Lua. The DB
     * path consumes only the pre-allocated seckill quota; it does not decrement
     * normal availableQty a second time.
     */
    @Transactional
    @Retryable(
            retryFor = {
                CannotAcquireLockException.class,
                DeadlockLoserDataAccessException.class,
                PessimisticLockingFailureException.class
            },
            maxAttempts = 3,
            backoff = @Backoff(delay = 50, multiplier = 2.0))
    public InventoryReservationResponse reserveFromSeckill(SeckillReserveCommand command) {
        validateSeckill(command);
        InventoryReservation existing = reservationRepository
                .findByIdempotencyKey(command.idempotencyKey())
                .orElse(null);
        if (existing != null) {
            ensureSameRequest(existing, command.orderId(), command.sku(), command.qty());
            return response(existing);
        }

        if (reservationRepository.findByOrderIdForUpdate(command.orderId()).isPresent()) {
            throw new IllegalStateException(
                    "order already has an inventory reservation: " + command.orderId());
        }

        InventoryStock stock = stockRepository.findBySkuForUpdate(command.sku())
                .orElseThrow(() -> new IllegalArgumentException("unknown sku=" + command.sku()));
        stock.acceptSeckillReservation(command.qty());
        InventoryReservation reservation = new InventoryReservation(
                command.orderId(),
                command.sku(),
                command.qty(),
                command.idempotencyKey(),
                LocalDateTime.now().plusMinutes(15));
        reservationRepository.save(reservation);
        recordOutbox(
                command.orderId(),
                "inventory.reserved",
                json(command.orderId(), command.sku(), command.qty()));
        increment("inventory.seckill.reservation.persisted");
        return response(reservation);
    }

    @Transactional
    @Retryable(
            retryFor = {
                CannotAcquireLockException.class,
                DeadlockLoserDataAccessException.class,
                PessimisticLockingFailureException.class
            },
            maxAttempts = 3,
            backoff = @Backoff(delay = 50, multiplier = 2.0))
    public void releaseSeckillQuota(String sku, int qty) {
        InventoryStock stock = stockRepository.findBySkuForUpdate(sku)
                .orElseThrow(() -> new IllegalArgumentException("unknown sku=" + sku));
        stock.releaseSeckillQuota(qty);
    }

    /** Commits reserved units to sold units after payment capture. */
    @Transactional
    @Retryable(
            retryFor = {
                CannotAcquireLockException.class,
                DeadlockLoserDataAccessException.class,
                PessimisticLockingFailureException.class
            },
            maxAttempts = 3,
            backoff = @Backoff(delay = 50, multiplier = 2.0))
    public InventoryReservationResponse commit(String orderId) {
        InventoryReservation reservation = reservationRepository.findByOrderIdForUpdate(orderId)
                .orElseThrow(() -> new IllegalArgumentException(
                        "reservation not found for order=" + orderId));
        if (reservation.getStatus() == ReservationStatus.COMMITTED) {
            return response(reservation);
        }
        requireStatus(reservation, ReservationStatus.RESERVED);

        InventoryStock stock = stockRepository.findBySkuForUpdate(reservation.getSku())
                .orElseThrow(() -> new IllegalStateException(
                        "stock not found for sku=" + reservation.getSku()));
        stock.commit(reservation.getQty());
        reservation.setStatus(ReservationStatus.COMMITTED);
        recordOutbox(
                orderId,
                "inventory.committed",
                json(orderId, reservation.getSku(), reservation.getQty()));
        increment("inventory.reservation.committed");
        return response(reservation);
    }

    /** Releases reserved units as a Saga compensation action. */
    @Transactional
    @Retryable(
            retryFor = {
                CannotAcquireLockException.class,
                DeadlockLoserDataAccessException.class,
                PessimisticLockingFailureException.class
            },
            maxAttempts = 3,
            backoff = @Backoff(delay = 50, multiplier = 2.0))
    public InventoryReservationResponse release(String orderId, String reason) {
        InventoryReservation reservation = reservationRepository.findByOrderIdForUpdate(orderId)
                .orElseThrow(() -> new IllegalArgumentException(
                        "reservation not found for order=" + orderId));
        if (reservation.getStatus() == ReservationStatus.RELEASED
                || reservation.getStatus() == ReservationStatus.CANCELLED
                || reservation.getStatus() == ReservationStatus.EXPIRED) {
            return response(reservation);
        }
        requireStatus(reservation, ReservationStatus.RESERVED);
        releaseLockedReservation(reservation, ReservationStatus.RELEASED);
        return response(reservation);
    }

    /** Releases reservations that expired before payment completed. */
    @Transactional
    @Retryable(
            retryFor = {
                CannotAcquireLockException.class,
                DeadlockLoserDataAccessException.class,
                PessimisticLockingFailureException.class
            },
            maxAttempts = 3,
            backoff = @Backoff(delay = 50, multiplier = 2.0))
    public int expireReservations() {
        LocalDateTime now = LocalDateTime.now();
        int count = 0;
        for (InventoryReservation candidate : reservationRepository
                .findByStatusAndExpiresAtBefore(ReservationStatus.RESERVED, now)) {
            InventoryReservation current = reservationRepository
                    .findByOrderIdForUpdate(candidate.getOrderId())
                    .orElse(null);
            if (current != null
                    && current.getStatus() == ReservationStatus.RESERVED
                    && !current.getExpiresAt().isAfter(now)) {
                releaseLockedReservation(current, ReservationStatus.EXPIRED);
                count++;
            }
        }
        return count;
    }

    @Transactional(readOnly = true)
    public InventoryStock getStock(String sku) {
        return stockRepository.findById(sku)
                .orElseThrow(() -> new IllegalArgumentException("unknown sku=" + sku));
    }

    @Transactional(readOnly = true)
    public List<InventoryOutboxEvent> pendingOutbox() {
        return outboxRepository.findTop100ByStatusOrderByCreatedAtAsc(InventoryOutboxEvent.PENDING);
    }

    private void releaseLockedReservation(
            InventoryReservation reservation, ReservationStatus status) {
        InventoryStock stock = stockRepository.findBySkuForUpdate(reservation.getSku())
                .orElseThrow(() -> new IllegalStateException(
                        "stock not found for sku=" + reservation.getSku()));
        stock.release(reservation.getQty());
        reservation.setStatus(status);
        recordOutbox(
                reservation.getOrderId(),
                "inventory.released",
                json(reservation.getOrderId(), reservation.getSku(), reservation.getQty()));
        if (status == ReservationStatus.EXPIRED) {
            recordOutbox(
                    reservation.getOrderId(),
                    "reservation.expired",
                    "{\"orderId\":\"" + reservation.getOrderId() + "\"}");
        }
        increment("inventory.reservation.released");
    }

    private void recordOutbox(String aggregateId, String eventType, String payloadJson) {
        outboxRepository.save(new InventoryOutboxEvent(aggregateId, eventType, payloadJson));
    }

    private void increment(String metricName) {
        Counter.builder(metricName)
                .tag("service", "inventory")
                .register(meterRegistry)
                .increment();
    }

    private static InventoryReservationResponse response(InventoryReservation reservation) {
        return new InventoryReservationResponse(
                reservation.getOrderId(),
                reservation.getReservationId().toString(),
                reservation.getSku(),
                reservation.getQty(),
                reservation.getStatus());
    }

    private static void validate(ReserveInventoryCommand command) {
        if (command == null) {
            throw new IllegalArgumentException("reserve command is required");
        }
        requireText(command.orderId(), "orderId");
        requireText(command.sku(), "sku");
        requireText(command.idempotencyKey(), "idempotencyKey");
        if (command.qty() <= 0 || command.ttlMinutes() <= 0) {
            throw new IllegalArgumentException("qty and ttlMinutes must be positive");
        }
    }

    private static void ensureSameRequest(
            InventoryReservation existing, ReserveInventoryCommand command) {
        ensureSameRequest(existing, command.orderId(), command.sku(), command.qty());
    }

    private static void ensureSameRequest(
            InventoryReservation existing, String orderId, String sku, int qty) {
        if (!existing.getOrderId().equals(orderId)
                || !existing.getSku().equals(sku)
                || existing.getQty() != qty) {
            throw new IllegalStateException(
                    "inventory idempotency key was reused with different arguments");
        }
    }

    private static void validateSeckill(SeckillReserveCommand command) {
        if (command == null) {
            throw new IllegalArgumentException("seckill command is required");
        }
        requireText(command.orderId(), "orderId");
        requireText(command.userId(), "userId");
        requireText(command.sku(), "sku");
        requireText(command.idempotencyKey(), "idempotencyKey");
        if (command.qty() <= 0) {
            throw new IllegalArgumentException("qty must be positive");
        }
    }

    private static void requireStatus(
            InventoryReservation reservation, ReservationStatus expected) {
        if (reservation.getStatus() != expected) {
            throw new IllegalStateException(
                    "reservation=" + reservation.getOrderId()
                            + " must be " + expected
                            + " but was " + reservation.getStatus());
        }
    }

    private static String json(String orderId, String sku, int qty) {
        return "{\"orderId\":\"" + orderId
                + "\",\"sku\":\"" + sku
                + "\",\"qty\":" + qty + "}";
    }

    private static void requireText(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " is required");
        }
    }
}
