package com.poc.order.ratelimit;

// [kb-land] Source: RATE_LIMIT_LOAD_TEST_PLAN.md + HOTEL_PAYMENT_DEVSECOPS_PLAYBOOK.md
// Pattern: Rate-limit result as a value object — callers inspect `allowed` and attach
//          diagnostic headers (X-RateLimit-Limit / X-RateLimit-Remaining) without coupling
//          to the underlying counter implementation.
public record RateLimitDecision(
        boolean allowed,
        long currentCount,
        long limit,
        String scope,
        String subject
) {
    /** Convenience factory for the shadow / no-op path. */
    public static RateLimitDecision permitAll(String scope, String subject) {
        return new RateLimitDecision(true, 0L, Long.MAX_VALUE, scope, subject);
    }

    public long remaining() {
        return Math.max(0L, limit - currentCount);
    }
}
