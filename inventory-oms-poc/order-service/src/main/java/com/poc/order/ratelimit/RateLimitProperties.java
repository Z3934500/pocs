package com.poc.order.ratelimit;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.HashMap;
import java.util.Map;

// [kb-land] Source: RATE_LIMIT_LOAD_TEST_PLAN.md
// Pattern: Externalise rate-limit policy into application.properties so limits can be
//          tuned per-environment without redeployment (ConfigMap in K8s).
//          shadow-mode=true means check() always returns allowed=true but still logs,
//          enabling safe production rollout before hard enforcement.
@ConfigurationProperties(prefix = "rate-limit")
public class RateLimitProperties {

    /** When true the limiter logs decisions but never blocks requests (shadow mode). */
    private boolean shadowMode = true;

    /**
     * Per-scope policy map keyed by scope name (e.g. "create-order").
     * Each entry specifies max requests and the bucket window in seconds.
     */
    private Map<String, ScopePolicy> policies = new HashMap<>();

    // ---- accessors ----

    public boolean isShadowMode() { return shadowMode; }
    public void setShadowMode(boolean shadowMode) { this.shadowMode = shadowMode; }

    public Map<String, ScopePolicy> getPolicies() { return policies; }
    public void setPolicies(Map<String, ScopePolicy> policies) { this.policies = policies; }

    // ---- nested config ----

    public static class ScopePolicy {

        /** Maximum requests allowed per bucket window. */
        private long maxRequests = 100;

        /** Bucket window size in seconds (fixed-bucket algorithm). */
        private int bucketWindowSeconds = 10;

        public long getMaxRequests() { return maxRequests; }
        public void setMaxRequests(long maxRequests) { this.maxRequests = maxRequests; }

        public int getBucketWindowSeconds() { return bucketWindowSeconds; }
        public void setBucketWindowSeconds(int bucketWindowSeconds) {
            this.bucketWindowSeconds = bucketWindowSeconds;
        }
    }
}
