package com.example.oms.concurrency;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Per-instance local rate limiter using AtomicInteger.
 *
 * SCOPE: single JVM only. Designed to absorb burst spikes BEFORE
 * the request reaches Redis (global quota) or the DB (row lock).
 * Not a substitute for global rate limiting across instances.
 *
 * 3-LAYER CONCURRENCY MODEL:
 *   ① This limiter  — fast-fail locally (nanosecond, no network I/O)
 *   ② Redis Lua DECRBY — global quota (microsecond, cluster-scoped atomic)
 *   ③ DB SELECT FOR UPDATE — absolute safety net (ms, DB row lock)
 *
 * ConcurrentHashMap.compute vs get+put:
 * get() followed by put() is NOT atomic even on ConcurrentHashMap.
 * Two threads can both read the same old value and both write incremented
 * values — one update is lost. compute() locks the key's hash-bucket node
 * for the entire lambda execution, making read-modify-write atomic.
 * Lambda must be pure memory — no I/O, no blocking calls inside.
 *
 * computeIfAbsent is safe here because AtomicInteger itself is thread-safe;
 * once the counter is in the map, incrementAndGet() handles concurrency.
 */
public class LocalRateLimiter {

    // Per-SKU request counters reset every second.
    private final ConcurrentHashMap<String, AtomicInteger> counters =
            new ConcurrentHashMap<>();

    private final int maxPerSecond;

    public LocalRateLimiter(int maxPerSecond) {
        this.maxPerSecond = maxPerSecond;
    }

    /**
     * Returns true if admitted; false if this instance has hit its local cap.
     * computeIfAbsent: safe init (key-absence guard). The AtomicInteger
     * returned is then incremented atomically via CAS — no further locking.
     */
    public boolean tryAcquire(String sku) {
        AtomicInteger counter =
                counters.computeIfAbsent(sku, k -> new AtomicInteger(0));
        return counter.incrementAndGet() <= maxPerSecond;
    }

    /** Called by @Scheduled every second to reset per-SKU windows. */
    public void resetCounters() {
        counters.clear();
    }
}
