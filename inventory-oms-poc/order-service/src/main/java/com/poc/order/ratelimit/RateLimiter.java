package com.poc.order.ratelimit;

import java.time.Instant;

// [kb-land] Source: RATE_LIMIT_LOAD_TEST_PLAN.md + HOTEL_PAYMENT_DEVSECOPS_PLAYBOOK.md
// Pattern: Fixed-bucket rate limiting abstraction — decouple the check algorithm
//          (NoOp / Redis Lua / token-bucket) from controllers and filters.
//          scope  = logical endpoint key  e.g. "create-order", "capture-payment"
//          subject = caller identity        e.g. "user:u123", "ip:1.2.3.4", "apikey:k9"
//          now    = wall-clock instant for testability (pass Instant.now() in production)
public interface RateLimiter {

    /**
     * Evaluate whether the caller identified by {@code subject} is within the
     * configured limit for {@code scope}.
     *
     * <p>Implementations MUST be idempotent w.r.t. the current bucket: calling
     * check twice in the same bucket with the same arguments should increment
     * the counter only once per real request, OR implementations should only
     * be called once per request (preferred — wrap in a filter/interceptor).
     *
     * @param scope   logical throttle key (e.g. "create-order")
     * @param subject caller identity (e.g. "user:u123")
     * @param now     current wall-clock time (injectable for testing)
     * @return decision containing whether the request is allowed and diagnostic counters
     */
    RateLimitDecision check(String scope, String subject, Instant now);
}
