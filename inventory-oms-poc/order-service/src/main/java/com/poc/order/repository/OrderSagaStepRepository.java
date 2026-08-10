package com.poc.order.repository;

import com.poc.order.entity.OrderSagaStep;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.*;

public interface OrderSagaStepRepository extends JpaRepository<OrderSagaStep, UUID> {
    List<OrderSagaStep> findByOrderIdOrderByCreatedAtAsc(String orderId);
}