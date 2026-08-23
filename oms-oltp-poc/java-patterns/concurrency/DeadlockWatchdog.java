package com.example.oms.concurrency;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;

/**
 * Three-level deadlock observability.
 *
 * ─────────────────────────────────────────────────────────────────────────
 * LEVEL 1 — InnoDB / PostgreSQL DB deadlock  (self-detecting, no watchdog)
 *   The DB engine maintains a wait-for graph. Cycle detected → victim tx
 *   killed immediately → Spring throws DeadlockLoserDataAccessException.
 *   @Retryable handles recovery. This watchdog only COUNTS the events for
 *   SRE alerting (rate spike = architectural problem, not transient jitter).
 *
 *   DB_LOCK_TIMEOUT_MS=3000 is a different thing: it handles non-deadlock
 *   lock WAITS (no cycle, thread A just waiting for thread B). That throws
 *   LockAcquisitionException, not DeadlockLoserDataAccessException.
 *
 * LEVEL 2 — JVM monitor deadlock  (cannot self-recover — must page on-call)
 *   synchronized / ReentrantLock cycles are invisible to the DB engine.
 *   ThreadMXBean.findDeadlockedThreads() scans the JVM lock graph.
 *   A JVM deadlock will never resolve itself — only a restart helps.
 *   Detect early, alert immediately.
 *
 * LEVEL 3 — HikariCP connection leak  (implicit watchdog, zero code needed)
 *   Configure in application.yml:
 *     spring.datasource.hikari.leak-detection-threshold: 3000
 *   Any connection held longer than 3 s prints the acquisition stack trace.
 *   Catches blocking I/O accidentally re-introduced inside @Transactional
 *   (e.g. kafkaTemplate.send().get()) without any custom code.
 * ─────────────────────────────────────────────────────────────────────────
 */
@Component
public class DeadlockWatchdog {

    private static final Logger log =
            LoggerFactory.getLogger(DeadlockWatchdog.class);

    private static final ThreadMXBean THREAD_MX =
            ManagementFactory.getThreadMXBean();

    /** Micrometer counter — feed into Prometheus / CloudWatch. */
    private final Counter dbDeadlockCounter;

    public DeadlockWatchdog(MeterRegistry registry) {
        this.dbDeadlockCounter = Counter.builder("oms.db.deadlock.total")
                .description("DB deadlock victim events caught by @Retryable")
                .tag("service", "seckill")
                .register(registry);
    }

    // ── Level 2: JVM monitor deadlock scan ───────────────────────────────

    /**
     * Polls every 5 s. JVM deadlocks are rare but fatal — they never
     * self-resolve. Log the full thread chain and fire an alert immediately.
     *
     * findDeadlockedThreads() covers both Object monitors (synchronized)
     * and java.util.concurrent locks (ReentrantLock, etc).
     */
    @Scheduled(fixedDelay = 5_000)
    public void detectJvmDeadlocks() {
        long[] deadlocked = THREAD_MX.findDeadlockedThreads();
        if (deadlocked == null) return;          // healthy — fast path

        ThreadInfo[] infos =
                THREAD_MX.getThreadInfo(deadlocked, /* maxDepth */ 20);
        StringBuilder sb = new StringBuilder(
                "⚠️  JVM DEADLOCK — process must be restarted:\n");
        for (ThreadInfo ti : infos) {
            sb.append("  Thread[").append(ti.getThreadName())
              .append("] blocked on ").append(ti.getLockName())
              .append(", held by [").append(ti.getLockOwnerName())
              .append("]\n");
        }
        log.error(sb.toString());
        // TODO: fire PagerDuty / SNS P1 alert — this is always a code bug.
    }

    // ── Level 1: DB deadlock counter (called from @Recover / advice) ─────

    /**
     * Increment whenever @Retryable catches DeadlockLoserDataAccessException.
     * Wire into your @Recover method or an AOP aspect.
     *
     * Prometheus alert rule example:
     *   rate(oms_db_deadlock_total[1m]) > 5  →  P2 page
     *   (sustained spike = lock-ordering bug or missing index)
     */
    public void recordDbDeadlock(String context) {
        dbDeadlockCounter.increment();
        log.warn("DB deadlock victim — context={}", context);
    }
}
