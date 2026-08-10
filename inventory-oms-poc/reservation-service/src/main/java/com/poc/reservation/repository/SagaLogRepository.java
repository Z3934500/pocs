package com.poc.reservation.repository;

import com.poc.reservation.entity.SagaLog;
import org.springframework.data.jpa.repository.JpaRepository;

public interface SagaLogRepository extends JpaRepository<SagaLog, String> {
}