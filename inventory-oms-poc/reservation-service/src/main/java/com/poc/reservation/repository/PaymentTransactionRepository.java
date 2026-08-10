package com.poc.reservation.repository;

import com.poc.reservation.entity.PaymentTransaction;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface PaymentTransactionRepository extends JpaRepository<PaymentTransaction, String> {

    Optional<PaymentTransaction> findByOrderIdAndIdempotencyKey(String orderId, String idempotencyKey);

    Optional<PaymentTransaction> findByProviderRef(String providerRef);
}