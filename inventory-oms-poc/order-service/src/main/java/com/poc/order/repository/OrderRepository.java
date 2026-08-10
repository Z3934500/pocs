package com.poc.order.repository;

import com.poc.order.entity.OrderAggregate;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.Optional;

public interface OrderRepository extends JpaRepository<OrderAggregate, String> {
    Optional<OrderAggregate> findByIdempotencyKey(String idempotencyKey);
}