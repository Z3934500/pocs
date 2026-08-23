package com.example.oms.order;

import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.DeadlockLoserDataAccessException;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;

/**
 * Outer layer: @Retryable ONLY — no @Transactional here.
 *
 * WHY THE SPLIT:
 * Combining @Retryable and @Transactional on the same method is a Spring
 * proxy-order trap. Default AOP order: @Transactional opens first,
 * @Retryable wraps outside. When the first attempt throws, the transaction
 * is marked rollback-only. @Retryable catches the exception but does NOT
 * close the tx context. The second attempt finds a rollback-only transaction
 * and immediately throws TransactionSystemException — business logic never
 * runs. All 3 retries fail with the wrong exception type.
 *
 * FIX: two Spring beans, two layers.
 * - This bean owns retry logic only.
 * - OrderTxService owns the transaction.
 * Each retry crosses a bean boundary → REQUIRES_NEW opens a truly fresh
 * transaction (self-invocation bypass is avoided by design).
 */
@Service
public class OrderService {

    private final OrderTxService txService;

    public OrderService(OrderTxService txService) {
        this.txService = txService;
    }

    @Retryable(
        value   = {DeadlockLoserDataAccessException.class,
                   DataIntegrityViolationException.class},
        maxAttempts = 3,
        backoff = @Backoff(delay = 100, multiplier = 2)
    )
    public Reservation createWithRetry(CreateOrderRequest req) {
        return txService.createInNewTx(req);
    }
}
