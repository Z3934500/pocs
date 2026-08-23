package com.example.oms.consumer;

import com.example.oms.event.DomainEvent;
import com.example.oms.order.Reservation;
import com.example.oms.order.ReservationRepository;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * Consumes reservation events from the Outbox relay and persists them.
 *
 * CONSUMER-SIDE DEDUP RULE:
 * Always check the BUSINESS TABLE (order/reservation), not the outbox table.
 * The outbox records producer state. If the relay retries a send after a
 * Kafka ACK timeout, the consumer receives the same message twice. Checking
 * outbox.status would not help — the consumer has no visibility into whether
 * the outbox was already "sent" on the producer side.
 *
 * Correct dedup: look up idempotencyKey in the reservation table.
 * If the row exists, the event was already processed → idempotent return.
 */
@Component
public class InventoryPersistenceConsumer {

    private final ReservationRepository reservationRepo;

    public InventoryPersistenceConsumer(ReservationRepository reservationRepo) {
        this.reservationRepo = reservationRepo;
    }

    @KafkaListener(
        topics  = "${oms.seckill.topic}",
        groupId = "inventory-persistence"
    )
    @Transactional
    public void onEvent(DomainEvent event) {
        // Check BUSINESS TABLE — not the outbox — for deduplication.
        if (reservationRepo.existsByIdempotencyKey(event.idempotencyKey())) {
            return;  // already persisted on a prior delivery — idempotent return
        }

        Reservation reservation = Reservation.fromEvent(event);
        reservationRepo.save(reservation);
    }
}
