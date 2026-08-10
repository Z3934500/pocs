package com.poc.reservation.repository;

import com.poc.reservation.entity.Reservation;
import com.poc.reservation.entity.ReservationStatus;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

public interface ReservationRepository extends JpaRepository<Reservation, Long> {

    Optional<Reservation> findByIdempotencyKey(String idempotencyKey);

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select r from Reservation r where r.orderId = :orderId")
    Optional<Reservation> findByOrderIdForUpdate(@Param("orderId") String orderId);

    List<Reservation> findByStatusAndExpiresAtBeforeOrderByExpiresAtAsc(
        ReservationStatus status, LocalDateTime time, Pageable pageable);
}
