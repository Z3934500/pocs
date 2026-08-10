package com.poc.payment.repository;

import com.poc.payment.entity.PaymentOutboxEvent;
import jakarta.transaction.Transactional;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface PaymentOutboxRepository extends JpaRepository<PaymentOutboxEvent, UUID> {
    List<PaymentOutboxEvent> findTop100ByStatusOrderByCreatedAtAsc(String status);
    long countByStatusIn(List<String> statuses);

    @Query("select min(event.createdAt) from PaymentOutboxEvent event where event.status in :statuses")
    LocalDateTime findOldestCreatedAt(@Param("statuses") List<String> statuses);

    @Query("""
        select event from PaymentOutboxEvent event
        where (event.status = :pending and (event.nextAttemptAt is null or event.nextAttemptAt <= :now))
           or (event.status = :inFlight and event.leaseUntil < :now)
        order by event.createdAt asc
        """)
    List<PaymentOutboxEvent> findClaimable(@Param("pending") String pending, @Param("inFlight") String inFlight,
                                           @Param("now") LocalDateTime now, Pageable pageable);
    @Modifying @Transactional
    @Query("""
        update PaymentOutboxEvent event set event.status = :inFlight, event.leaseId = :leaseId,
               event.leaseUntil = :leaseUntil, event.attemptCount = event.attemptCount + 1
         where event.eventId = :eventId and
              ((event.status = :pending and (event.nextAttemptAt is null or event.nextAttemptAt <= :now))
               or (event.status = :inFlight and event.leaseUntil < :now))
        """)
    int tryClaim(@Param("eventId") UUID eventId, @Param("pending") String pending,
                 @Param("inFlight") String inFlight, @Param("leaseId") String leaseId,
                 @Param("leaseUntil") LocalDateTime leaseUntil, @Param("now") LocalDateTime now);
    @Modifying @Transactional
    @Query("""
        update PaymentOutboxEvent event set event.status = :published, event.publishedAt = :publishedAt,
               event.leaseId = null, event.leaseUntil = null, event.lastError = null
         where event.eventId = :eventId and event.status = :inFlight and event.leaseId = :leaseId
        """)
    int markPublished(@Param("eventId") UUID eventId, @Param("inFlight") String inFlight,
                      @Param("published") String published, @Param("leaseId") String leaseId,
                      @Param("publishedAt") LocalDateTime publishedAt);
    @Modifying @Transactional
    @Query("""
        update PaymentOutboxEvent event set event.status = case when event.attemptCount >= :maxAttempts
               then :deadLetter else :pending end, event.nextAttemptAt = :nextAttemptAt,
               event.leaseId = null, event.leaseUntil = null, event.lastError = :lastError
         where event.eventId = :eventId and event.status = :inFlight and event.leaseId = :leaseId
        """)
    int markFailed(@Param("eventId") UUID eventId, @Param("inFlight") String inFlight,
                   @Param("pending") String pending, @Param("deadLetter") String deadLetter,
                   @Param("leaseId") String leaseId, @Param("nextAttemptAt") LocalDateTime nextAttemptAt,
                   @Param("lastError") String lastError, @Param("maxAttempts") int maxAttempts);
}
