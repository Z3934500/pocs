package com.poc.inventory.repository;

import com.poc.contracts.ReservationStatus;
import com.poc.inventory.entity.InventoryReservation;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.*;
import org.springframework.data.repository.query.Param;
import java.time.LocalDateTime;
import java.util.*;

public interface InventoryReservationRepository extends JpaRepository<InventoryReservation, UUID> {
    Optional<InventoryReservation> findByIdempotencyKey(String idempotencyKey);
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select reservation from InventoryReservation reservation where reservation.orderId = :orderId")
    Optional<InventoryReservation> findByOrderIdForUpdate(@Param("orderId") String orderId);
    List<InventoryReservation> findByStatusAndExpiresAtBefore(ReservationStatus status, LocalDateTime now);
}