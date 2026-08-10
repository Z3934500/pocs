package com.poc.payment.repository;

import com.poc.payment.entity.PaymentTransaction;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.*;

public interface PaymentTransactionRepository extends JpaRepository<PaymentTransaction, UUID> {
    Optional<PaymentTransaction> findByOrderIdAndIdempotencyKey(String orderId, String idempotencyKey);
    Optional<PaymentTransaction> findByProviderRef(String providerRef);
}