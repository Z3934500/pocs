package com.example.oms.concurrency;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Graceful shutdown coordinator demonstrating two volatile patterns.
 *
 * PATTERN 1 — volatile boolean (single-writer / multi-reader):
 * One thread writes (Actuator endpoint, OS signal handler).
 * Many threads read (scheduled tasks, pollers).
 * volatile guarantees: the write is immediately visible to all CPU cores;
 * no thread reads a stale cached value. boolean assignment is atomic by JLS,
 * so volatile alone is sufficient — no synchronized block needed.
 *
 * PATTERN 2 — AtomicBoolean.compareAndSet (multiple-writer, exactly one wins):
 * Multiple Admin nodes could simultaneously trigger shutdown.
 * volatile would still "work" but two writes could both succeed.
 * compareAndSet(true, false) is a single atomic CAS operation:
 * exactly ONE caller transitions true→false; all others see false→false
 * and know shutdown was already claimed.
 *
 * WRONG CHOICE EXAMPLE:
 *   volatile int counter; counter++;   // NOT atomic — 3-step read+add+write
 *   AtomicInteger.incrementAndGet();    // correct for read-modify-write
 * volatile is only correct for single-assignment visibility, not for
 * any read-modify-write operation.
 */
@Component
public class GracefulShutdown {

    private static final Logger log = LoggerFactory.getLogger(GracefulShutdown.class);

    // Pattern 1: volatile — zero overhead, correct for single-writer scenario
    private volatile boolean running = true;

    // Pattern 2: AtomicBoolean — exclusive one-time trigger across threads/nodes
    private final AtomicBoolean shutdownClaimed = new AtomicBoolean(false);

    /** Actuator /shutdown or SIGTERM handler — single writer. */
    public void initiateShutdown() {
        this.running = false;   // volatile write — all threads see this immediately
        log.info("Graceful shutdown initiated — task polling will stop");
    }

    /**
     * For multi-node environments where exactly one Admin node must own shutdown.
     * Returns true only for the thread/node that successfully transitions false→true.
     */
    public boolean claimShutdown() {
        if (shutdownClaimed.compareAndSet(false, true)) {
            initiateShutdown();
            log.info("Shutdown ownership claimed by this node");
            return true;
        }
        log.info("Shutdown already claimed — no-op");
        return false;
    }

    @Scheduled(fixedDelay = 100)
    public void pollOutboxRelay() {
        if (!running) return;   // volatile read — never stale, no lock needed
        // ... scan outbox table, publish pending events to Kafka/SQS
    }
}
