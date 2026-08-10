package com.poc.reservation.repository;

import com.poc.reservation.entity.InboxEvent;
import org.springframework.data.jpa.repository.JpaRepository;

public interface InboxEventRepository extends JpaRepository<InboxEvent, String> {
}