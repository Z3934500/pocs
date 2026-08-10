package com.poc.order.ratelimit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.time.Instant;

// [kb-land] Source: RATE_LIMIT_LOAD_TEST_PLAN.md + HOTEL_PAYMENT_DEVSECOPS_PLAYBOOK.md
// Pattern: No-op / passthrough implementation — active when rate-limit.shadow-mode=true
//          (the PoC default).  Logs decisions at DEBUG so load-test reports can validate
//          the call path without blocking any real traffic.
//
//          Production path:  swap in RedisFixedBucketRateLimiter (P1 enhancement).
//          Rollout order:    NoOp → Redis shadow (log only) → Redis enforce.
@Component
@ConditionalOnProperty(name = "rate-limit.shadow-mode", havingValue = "true", matchIfMissing = true)
public class NoOpRateLimiter implements RateLimiter {

    private static final Logger log = LoggerFactory.getLogger(NoOpRateLimiter.class);

    @Override
    public RateLimitDecision check(String scope, String subject, Instant now) {
        RateLimitDecision decision = RateLimitDecision.permitAll(scope, subject);
        log.debug("[rate-limit][shadow] scope={} subject={} allowed=true (no-op)", scope, subject);
        return decision;
    }
}
