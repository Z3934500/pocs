# java-patterns — OMS Seckill Concurrency Reference

Java code examples derived from the OMS Pre-Prod architecture review.
Each file isolates one pattern and explains **why** the naive approach fails.

## File Map

| File | Pattern | Pre-Prod Issue |
|---|---|---|
| `order/OrderService.java` | `@Retryable` outer layer only | Trap 1: proxy order |
| `order/OrderTxService.java` | `@Transactional(REQUIRES_NEW)` inner layer | Trap 1: proxy order |
| `compensation/rollback_stock.lua` | Idempotent Lua compensation | Trap 2: over-compensation |
| `consumer/InventoryPersistenceConsumer.java` | `@KafkaListener` with business-table dedup | Trap 3: duplicate consume |
| `event/DomainEvent.java` | Record + Jackson `toJson()` | payloadJson hand-built strings |
| `concurrency/LocalRateLimiter.java` | `AtomicInteger` + `ConcurrentHashMap.compute` | JVM-level rate limiting |
| `concurrency/GracefulShutdown.java` | `volatile` vs `AtomicBoolean.compareAndSet` | volatile ≠ atomicity |

---

## Pattern 1 — @Retryable + @Transactional Two-Layer Split

**The trap**: both on the same method → first failure marks tx `rollback-only`
→ second retry gets `TransactionSystemException` immediately → all retries wasted.

**The fix**: `OrderService` owns `@Retryable`; `OrderTxService` owns
`@Transactional(REQUIRES_NEW)`. Each retry crosses a bean boundary and
opens a fresh transaction.

```
OrderService.createWithRetry()          ← @Retryable, no @Transactional
  └─→ OrderTxService.createInNewTx()   ← @Transactional(REQUIRES_NEW), no @Retryable
        1. findByIdempotencyKey()       ← idempotent short-circuit
        2. findBySkuForUpdate()         ← SELECT FOR UPDATE (row lock)
        3. reservationRepo.save()
        4. outboxRepo.save()            ← atomic with step 3, no XADD
```

## Pattern 2 — Compensation Lua Idempotency

**The trap**: network-retry calls `INCRBY` twice → stock exceeds original value → oversell.

**The fix**: `SISMEMBER` check before any mutation.
Returns `ALREADY_ROLLED_BACK` if user is no longer in the dedup set.

## Pattern 3 — Consumer-Side Dedup

**The trap**: checking `outbox.status` at the consumer — that's producer state.
Relay retries produce duplicate Kafka messages regardless of outbox state.

**The fix**: `reservationRepo.existsByIdempotencyKey()` — check the business table.

## Pattern 4 — volatile vs AtomicBoolean

| Need | Tool |
|---|---|
| Single writer, many readers (shutdown flag) | `volatile boolean` |
| Multiple writers, exactly one must win | `AtomicBoolean.compareAndSet` |
| Read-modify-write counter | `AtomicInteger.incrementAndGet()` |
| Per-key atomic deduction (local) | `ConcurrentHashMap.compute()` |
| Per-key atomic deduction (cluster) | Redis Lua `DECRBY` |
