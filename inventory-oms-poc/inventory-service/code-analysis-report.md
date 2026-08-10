# Code Analysis Report — inventory-service
> Generated: 2026-08-03 | Source: inventory-oms-poc/inventory-service | Files analysed: 31

---

## 0. Inventory

| Path | Language | Layer |
|------|----------|-------|
| `InventoryServiceApplication.java` | Java 17 | Script/Entry-point |
| `entity/InventoryStock.java` | Java 17 | Model/Entity |
| `entity/InventoryReservation.java` | Java 17 | Model/Entity |
| `entity/InventoryOutboxEvent.java` | Java 17 | Model/Entity |
| `repository/InventoryStockRepository.java` | Java 17 | Repository/DAO |
| `repository/InventoryReservationRepository.java` | Java 17 | Repository/DAO |
| `repository/InventoryOutboxRepository.java` | Java 17 | Repository/DAO |
| `service/InventoryService.java` | Java 17 | Service |
| `controller/InventoryController.java` | Java 17 | Controller |
| `controller/ApiExceptionHandler.java` | Java 17 | Controller |
| `messaging/DomainEventPublisher.java` | Java 17 | Infrastructure |
| `messaging/LoggingDomainEventPublisher.java` | Java 17 | Infrastructure |
| `messaging/KafkaDomainEventPublisher.java` | Java 17 | Infrastructure |
| `messaging/SqsDomainEventPublisher.java` | Java 17 | Infrastructure |
| `messaging/InventoryOutboxRelay.java` | Java 17 | Infrastructure |
| `messaging/SqsInventoryReservationCommandConsumer.java` | Java 17 | Infrastructure |
| `messaging/SqsMessagingConfiguration.java` | Java 17 | Config |
| `seckill/SeckillAdmission.java` | Java 17 | Infrastructure |
| `seckill/SeckillAdmissionResponse.java` | Java 17 | Model/Entity |
| `seckill/DisabledSeckillAdmission.java` | Java 17 | Infrastructure |
| `seckill/RedisSeckillAdmission.java` | Java 17 | Infrastructure |
| `seckill/RedisSeckillStreamConsumer.java` | Java 17 | Infrastructure |
| `observability/OutboxMetrics.java` | Java 17 | Infrastructure |
| `observability/KafkaConsumerLagMetrics.java` | Java 17 | Infrastructure |
| `observability/SqsQueueMetrics.java` | Java 17 | Infrastructure |
| `observability/RequestTraceLoggingFilter.java` | Java 17 | Infrastructure |
| `config/InventoryDataInitializer.java` | Java 17 | Config |
| `config/InventorySchedulingConfig.java` | Java 17 | Config |
| `config/ReservationExpirationScheduler.java` | Java 17 | Infrastructure |
| `resources/application.properties` | Properties | Config |
| `resources/db/migration/V1__create_tables.sql` | SQL (PostgreSQL) | Infrastructure |

**Build system:** Maven (`pom.xml`), Spring Boot Maven Plugin  
**Framework:** Spring Boot 3, Spring Data JPA, Spring Kafka, Spring Data Redis, Spring Retry, AWS SDK v2 SQS  
**DB (local):** H2 file-mode (PostgreSQL compatibility mode) | **DB (prod):** Aurora PostgreSQL 15  
**Schema management:** Flyway (disabled in PoC, enabled in production via `FLYWAY_ENABLED=true`)

---

## 1. Architecture Map (File Tree)

```
src/main/java/com/poc/inventory/
├── InventoryServiceApplication.java         [Entry-point]
├── controller/
│   ├── InventoryController.java             [Controller]
│   └── ApiExceptionHandler.java             [Controller — global error handler]
├── service/
│   └── InventoryService.java                [Service — sole writer of stock state]
├── entity/
│   ├── InventoryStock.java                  [Model/Entity — aggregate root]
│   InventoryReservation.java                [Model/Entity]
│   └── InventoryOutboxEvent.java            [Model/Entity — transactional outbox]
├── repository/
│   ├── InventoryStockRepository.java        [Repository/DAO]
│   ├── InventoryReservationRepository.java  [Repository/DAO]
│   └── InventoryOutboxRepository.java       [Repository/DAO]
├── messaging/
│   ├── DomainEventPublisher.java            [Infrastructure — port interface]
│   ├── LoggingDomainEventPublisher.java     [Infrastructure — local adapter]
│   ├── KafkaDomainEventPublisher.java       [Infrastructure — Kafka adapter]
│   ├── SqsDomainEventPublisher.java         [Infrastructure — SQS adapter]
│   ├── InventoryOutboxRelay.java            [Infrastructure — outbox poller]
│   ├── SqsInventoryReservationCommandConsumer.java [Infrastructure — SQS consumer]
│   └── SqsMessagingConfiguration.java      [Config — SqsAsyncClient bean]
├── seckill/
│   ├── SeckillAdmission.java                [Infrastructure — port interface]
│   ├── SeckillAdmissionResponse.java        [Model/Entity — response DTO]
│   ├── DisabledSeckillAdmission.java        [Infrastructure — no-op adapter]
│   ├── RedisSeckillAdmission.java           [Infrastructure — Redis/Lua adapter]
│   └── RedisSeckillStreamConsumer.java      [Infrastructure — Redis Stream consumer]
├── observability/
│   ├── OutboxMetrics.java                   [Infrastructure — Micrometer gauges]
│   ├── KafkaConsumerLagMetrics.java         [Infrastructure — Kafka lag gauge]
│   ├── SqsQueueMetrics.java                 [Infrastructure — SQS depth gauge]
│   └── RequestTraceLoggingFilter.java       [Infrastructure — MDC trace filter]
└── config/
    ├── InventoryDataInitializer.java         [Config — test seed data]
    ├── InventorySchedulingConfig.java        [Config — @EnableScheduling]
    └── ReservationExpirationScheduler.java   [Infrastructure — TTL expiry job]

src/main/resources/
├── application.properties
└── db/migration/V1__create_tables.sql
```

---

## 2. Layer Diagram

```
HTTP Request (POST /internal/inventory/*)
         │
         ▼
 [InventoryController]
    @RestController  @RequestMapping("/internal/inventory")
         │   │
         │   └──► [SeckillAdmission] (interface)
         │              │
         │         ┌────┴────────────────────────┐
         │         ▼                             ▼
         │   [DisabledSeckillAdmission]  [RedisSeckillAdmission]
         │                                       │ Redis/Lua ADMIT_SCRIPT
         │                                       ▼
         │                               Redis Stream (XADD)
         │                                       │
         │                               [RedisSeckillStreamConsumer]
         │                                 @Scheduled poll
         │                                       │
         ▼                                       ▼
 [InventoryService]  ◄──────────────────────────►
    @Service (sole writer)
         │  pessimistic lock on SKU row
         ▼
 ┌──────────────────────────────────┐
 │  DB (H2/Aurora PostgreSQL)       │
 │  ├── inventory_stock             │
 │  ├── inventory_reservation       │
 │  └── inventory_outbox_event      │
 └────────────────┬─────────────────┘
                  │  (Outbox written in same TX)
                  ▼
 [InventoryOutboxRelay]  @Scheduled poll (1s)
    lease → publish → markPublished / markFailed
                  │
                  ▼
 [DomainEventPublisher] (port)
   ┌─────────────┼──────────────┐
   ▼             ▼              ▼
[Logging]    [Kafka/MSK]   [SQS/SNS]
  (PoC)      @Conditional   @Conditional

SQS Async mode (optional):
[SqsInventoryReservationCommandConsumer] @Scheduled poll
    reads FIFO command queue → InventoryService.reserve()
    → publishes result to result queue

Schedulers (background):
[ReservationExpirationScheduler] @Scheduled(60s) → InventoryService.expireReservations()
[OutboxMetrics]                  @Scheduled(10s) → Micrometer gauges
```


---

## 3. Cross-Cutting Concerns

| Concern | Implementation |
|---------|---------------|
| **Pessimistic locking** | `@Lock(PESSIMISTIC_WRITE)` on `findBySkuForUpdate` and `findByOrderIdForUpdate` — serialises all concurrent SKU mutations |
| **Optimistic locking** | `@Version Long version` on `InventoryStock` and `InventoryReservation` — guards against lost-update on non-lock paths |
| **Transactions** | `@Transactional` on every `InventoryService` write method; Outbox row written in same TX ensuring atomicity |
| **Retry** | `@Retryable(maxAttempts=3, backoff=50ms×2)` on all service write methods for deadlock / lock-wait failures |
| **Idempotency** | `uk_inventory_reservation_idempotency` DB unique constraint; `findByIdempotencyKey` check returns existing result on replay |
| **Outbox pattern** | Every state change writes an `InventoryOutboxEvent` row in the same transaction; `InventoryOutboxRelay` delivers it out-of-transaction via a lease mechanism |
| **Lease / at-least-once** | `tryClaim` CAS update sets `leaseId + leaseUntil`; expired leases allow redelivery after crash |
| **Exponential back-off (Outbox)** | `retryDelay(attempt)` doubles delay up to `retryMaxMs`; after `maxAttempts` marks `DEAD_LETTER` |
| **Error handling** | `ApiExceptionHandler` maps `IllegalArgumentException→400`, `IllegalStateException→409` as RFC 9457 ProblemDetail |
| **Observability** | MDC `traceId/spanId` via `RequestTraceLoggingFilter`; OTEL tracing via `micrometer-tracing-bridge-otel`; Prometheus metrics at `/actuator/prometheus` |
| **Seckill Redis dedup** | Lua script atomically checks order dedup hash + user set + quota counter; appends to Redis Stream only on success |
| **Transport seam** | `DomainEventPublisher` and `SeckillAdmission` are interfaces; concrete adapter selected by `@ConditionalOnProperty(messaging.transport)` and `seckill.redis.enabled` |
| **Connection pool** | HikariCP; prod: `DB_POOL_MAX` pods × replicas ≤ RDS Proxy `MaxConnectionsPercent` |
| **Lock timeout** | `jakarta.persistence.lock.timeout=${DB_LOCK_TIMEOUT_MS:3000}` prevents indefinite wait under hot-SKU contention |

---

## 4. Key Data Models

### `inventory_stock` (aggregate root)
| Field | Type | Note |
|-------|------|------|
| `sku` | VARCHAR(64) PK | Partition key; row locked during mutations |
| `available_qty` | INT ≥ 0 | Decremented on reserve; incremented on release |
| `reserved_qty` | INT ≥ 0 | Incremented on reserve; decremented on commit/release |
| `sold_qty` | INT ≥ 0 | Incremented on commit |
| `seckill_allocated_qty` | INT ≥ 0 | Quota moved out of `available_qty` before Redis opens |
| `version` | BIGINT | Optimistic lock |

Invariant: `available_qty + reserved_qty + sold_qty + seckill_allocated_qty = original total`

### `inventory_reservation`
| Field | Type | Note |
|-------|------|------|
| `reservation_id` | UUID PK | `gen_random_uuid()` |
| `order_id` | VARCHAR(64) UNIQUE | One reservation per order |
| `idempotency_key` | VARCHAR(128) UNIQUE | Replay guard |
| `status` | VARCHAR(32) | `RESERVED → COMMITTED \| RELEASED \| CANCELLED \| EXPIRED` |
| `expires_at` | TIMESTAMP | TTL set at creation; expiry scanner releases overdue rows |

### `inventory_outbox_event`
| Field | Type | Note |
|-------|------|------|
| `event_id` | UUID PK | Random UUID |
| `status` | VARCHAR(32) | `PENDING → IN_FLIGHT → PUBLISHED \| DEAD_LETTER` |
| `lease_id` | VARCHAR(64) | Set by relay to prevent concurrent publish |
| `lease_until` | TIMESTAMP | Expired lease allows redelivery |
| `attempt_count` | INT | Incremented on each claim; triggers DLQ after `maxAttempts` |
| `next_attempt_at` | TIMESTAMP | Exponential back-off target |

---

## 5. Design Patterns

| Pattern | Where |
|---------|-------|
| **Transactional Outbox** | `InventoryOutboxEvent` + `InventoryOutboxRelay` — atomically writes event with state; relay delivers out-of-TX |
| **Saga compensation** | `release()` and `cancelIfPresent()` are explicit compensation actions callable by the Order Saga |
| **Pessimistic row lock** | `findBySkuForUpdate` serialises concurrent SKU mutations at DB level |
| **Idempotent Consumer** | `findByIdempotencyKey` + DB unique constraint makes every command exactly-once visible |
| **Repository** | Spring Data `JpaRepository` per aggregate; custom queries for lock/expiry |
| **Strategy / Adapter** | `DomainEventPublisher` and `SeckillAdmission` interfaces with swappable implementations (Logging / Kafka / SQS / Redis) |
| **Lease-based at-least-once** | `tryClaim` CAS + `leaseUntil` allows redelivery after crash while preventing concurrent double-publish |
| **Redis/Lua atomic admission** | Single Lua script atomically checks dedup + quota + appends stream — no distributed lock needed |
| **Two-phase seckill** | DB quota pre-allocated (`allocateSeckillQuota`) before Redis opens; Redis stream consumer reconciles to DB |


---

## 6. File-by-File Analysis

---

### 6.1 `InventoryServiceApplication.java` — [Script/Entry-point]

#### A. Syntax Profile
- Java 17 / Spring Boot 3 · `@SpringBootApplication` (meta-annotation = `@Configuration + @EnableAutoConfiguration + @ComponentScan`)
- Imports: `SpringApplication`, `SpringBootApplication`

#### B. Structural Skeleton
```java
@SpringBootApplication
public class InventoryServiceApplication {    // entry-point class — no instance fields
    public static void main(String[] args)    // static void: JVM entry point
}
```
Design: single-purpose bootstrap; no business logic here.

#### C. Algorithm Profile
| Method | Pseudocode |
|--------|-----------|
| `main(args)` | Delegate to `SpringApplication.run(InventoryServiceApplication.class, args)` — bootstraps IoC container, starts embedded Tomcat, triggers all `@Component` scanning |

#### D. Execution Pipeline
| Stage | Detail |
|-------|--------|
| Edit | Developer writes `@SpringBootApplication` class |
| Compile | `javac` → `.class`; Maven `spring-boot-maven-plugin` repackages into fat JAR |
| Link/Inject | Spring context loads all auto-configurations declared in `spring.factories`/`AutoConfiguration.imports` |
| Load | JVM loads class; `SpringApplication.run()` creates `ApplicationContext`, wires all beans |
| Execute | `main()` called by JVM → Spring context started → Tomcat listens on `:8082` |

---

### 6.2 `entity/InventoryStock.java` — [Model/Entity]

#### A. Syntax Profile
- Java 17 / Jakarta Persistence 3 · `@Entity`, `@Table`, `@Id`, `@Column`, `@Version`, `@PrePersist`, `@PreUpdate`
- No generics; no lambdas; manual getters; domain logic inside entity (rich model, not anemic)

#### B. Structural Skeleton
```java
@Entity @Table(name = "inventory_stock")
public class InventoryStock {
    @Id private String sku                  // PK — VARCHAR(64)
    @Column private int availableQty        // instance field — decremented on reserve
    @Column private int reservedQty         // instance field — incremented on reserve
    @Column private int soldQty             // instance field — incremented on commit
    @Column private int seckillAllocatedQty // instance field — seckill quota carved out
    @Column private LocalDateTime updatedAt // instance field — auto-set by @PreUpdate
    @Version private Long version           // optimistic lock

    protected InventoryStock()              // JPA no-arg ctor (protected)
    public InventoryStock(String, int)      // domain ctor
    public void reserve(int)                // instance void: mutates availableQty + reservedQty
    public void allocateSeckillQuota(int)   // instance void: carves seckill quota
    public void acceptSeckillReservation(int) // instance void: converts Redis admission to DB reservation
    public void releaseSeckillQuota(int)    // instance void: returns unconsumed quota
    public void commit(int)                 // instance void: reserved → sold
    public void release(int)                // instance void: reserved → available
    @PrePersist @PreUpdate void touch()     // lifecycle callback: sets updatedAt
    // getters only — no setters except via domain methods
}
```


#### C. Algorithm Profile
| Method | Pseudocode |
|--------|-----------|
| `reserve(qty)` | Guard qty>0 AND availableQty≥qty → availableQty -= qty; reservedQty += qty |
| `allocateSeckillQuota(qty)` | Guard qty>0 AND availableQty≥qty AND seckillAllocatedQty==0 → availableQty -= qty; seckillAllocatedQty += qty |
| `acceptSeckillReservation(qty)` | Guard qty>0 AND seckillAllocatedQty≥qty → seckillAllocatedQty -= qty; reservedQty += qty |
| `releaseSeckillQuota(qty)` | Guard qty>0 AND seckillAllocatedQty≥qty → seckillAllocatedQty -= qty; availableQty += qty |
| `commit(qty)` | Guard qty>0 AND reservedQty≥qty → reservedQty -= qty; soldQty += qty |
| `release(qty)` | Guard qty>0 AND reservedQty≥qty → reservedQty -= qty; availableQty += qty |

Invariant enforced: `available + reserved + sold + seckillAllocated = original total`

#### D. Execution Pipeline
| Stage | Detail |
|-------|--------|
| Edit | Developer defines domain mutation methods with guards |
| Compile | `javac`; JPA bytecode enhancement by Hibernate (lazy loading proxies) |
| Link/Inject | `@Entity` registered with Hibernate `SessionFactory`; no Spring beans injected |
| Load | Hibernate maps table `inventory_stock` → entity; `@Version` enables optimistic locking |
| Execute | Called inside `InventoryService` methods while row is pessimistically locked; `@PreUpdate` fires on flush |

---

### 6.3 `entity/InventoryReservation.java` — [Model/Entity]

#### B. Structural Skeleton
```java
@Entity @Table(name="inventory_reservation",
    uniqueConstraints = {uk_order, uk_idempotency})
public class InventoryReservation {
    @Id @GeneratedValue(UUID) private UUID reservationId
    @Column(updatable=false) private String orderId       // immutable after creation
    @Column(updatable=false) private String sku           // immutable
    @Column(updatable=false) private int qty              // immutable
    @Enumerated(STRING) private ReservationStatus status  // mutable: RESERVED→COMMITTED|RELEASED|EXPIRED
    @Column(updatable=false) private String idempotencyKey // immutable
    private LocalDateTime expiresAt                       // business TTL
    @Version private Long version                         // optimistic lock
    public void setStatus(ReservationStatus)              // sole mutable method
}
```

#### C. Algorithm Profile
- Construction: sets `status = RESERVED`, records `orderId/sku/qty/idempotencyKey/expiresAt`
- Mutation path: only `setStatus()` — all transitions driven by `InventoryService`
- Lifecycle: `RESERVED → COMMITTED` (payment success) | `RESERVED → RELEASED` (saga compensation) | `RESERVED → EXPIRED` (TTL scan)

---

### 6.4 `entity/InventoryOutboxEvent.java` — [Model/Entity]

#### B. Structural Skeleton
```java
@Entity @Table(name="inventory_outbox_event")
public class InventoryOutboxEvent {
    public static final String PENDING = "PENDING"     // status constant
    public static final String IN_FLIGHT = "IN_FLIGHT" // status constant
    public static final String PUBLISHED = "PUBLISHED" // status constant
    public static final String DEAD_LETTER = "DEAD_LETTER" // status constant
    @Id private UUID eventId
    private String aggregateId, eventType, payloadJson
    private String status, leaseId, lastError
    private LocalDateTime createdAt, publishedAt, nextAttemptAt, leaseUntil
    private int attemptCount
    public InventoryOutboxEvent(String aggregateId, String eventType, String payloadJson) // ctor
    @PrePersist void onCreate()     // sets createdAt + nextAttemptAt = now
    public void markPublished()     // status = PUBLISHED, publishedAt = now
}
```

#### C. Algorithm Profile
| Method | Pseudocode |
|--------|-----------|
| `ctor` | eventId = UUID.random(); status = PENDING; attemptCount = 0 |
| `onCreate` | createdAt = nextAttemptAt = now() |
| `markPublished` | status = PUBLISHED; publishedAt = now() |

Status transitions driven externally by `InventoryOutboxRelay` via `@Modifying @Query` bulk updates.


---

### 6.5 `repository/InventoryStockRepository.java` — [Repository/DAO]

#### A. Syntax Profile
- Spring Data JPA interface · `@Lock(PESSIMISTIC_WRITE)` · `@Query` JPQL · `@Param`
- Extends `JpaRepository<InventoryStock, String>` — PK type is `String` (sku)

#### B. Structural Skeleton
```java
public interface InventoryStockRepository extends JpaRepository<InventoryStock, String> {
    @Lock(PESSIMISTIC_WRITE)
    @Query("select stock from InventoryStock stock where stock.sku = :sku")
    Optional<InventoryStock> findBySkuForUpdate(@Param("sku") String)
    // JpaRepository provides: findById, save, findAll, count, delete…
}
```

#### C. Algorithm Profile
| Method | Pseudocode |
|--------|-----------|
| `findBySkuForUpdate(sku)` | `SELECT … FROM inventory_stock WHERE sku=? FOR UPDATE` — acquires row lock for the duration of the caller's TX |

#### D. Execution Pipeline
| Stage | Detail |
|-------|--------|
| Compile | Interface; no bytecode generated by compiler — Spring Data generates proxy at runtime |
| Link/Inject | Spring Data JPA generates `SimpleJpaRepository` proxy; injects `EntityManager` |
| Load | Bean registered as `InventoryStockRepository` in Spring context |
| Execute | `findBySkuForUpdate` translated to JPQL → SQL `SELECT … FOR UPDATE` |

---

### 6.6 `repository/InventoryReservationRepository.java` — [Repository/DAO]

#### B. Structural Skeleton
```java
public interface InventoryReservationRepository extends JpaRepository<InventoryReservation, UUID> {
    Optional<InventoryReservation> findByIdempotencyKey(String)        // derived query: WHERE idempotency_key=?
    @Lock(PESSIMISTIC_WRITE) @Query(…)
    Optional<InventoryReservation> findByOrderIdForUpdate(String)      // SELECT … WHERE order_id=? FOR UPDATE
    List<InventoryReservation> findByStatusAndExpiresAtBefore(ReservationStatus, LocalDateTime) // expiry scan
}
```

#### C. Algorithm Profile
| Method | Pseudocode |
|--------|-----------|
| `findByIdempotencyKey` | `SELECT … WHERE idempotency_key=?` — idempotency replay guard |
| `findByOrderIdForUpdate` | `SELECT … WHERE order_id=? FOR UPDATE` — prevents double-reserve for same order |
| `findByStatusAndExpiresAtBefore` | `SELECT … WHERE status=? AND expires_at < ?` — expiry scanner input |

---

### 6.7 `repository/InventoryOutboxRepository.java` — [Repository/DAO]

#### A. Syntax Profile
- Spring Data JPA · `@Modifying`, `@Transactional`, `@Query` with JPQL text-block strings (Java 15+)
- `Pageable` parameter for batch limiting; `@Param` named params; bulk UPDATE queries

#### B. Structural Skeleton
```java
public interface InventoryOutboxRepository extends JpaRepository<InventoryOutboxEvent, UUID> {
    List<InventoryOutboxEvent> findTop100ByStatusOrderByCreatedAtAsc(String status)
    long countByStatusIn(List<String> statuses)
    LocalDateTime findOldestCreatedAt(@Param("statuses") List<String>)  // for age metric
    List<InventoryOutboxEvent> findClaimable(String pending, String inFlight, LocalDateTime now, Pageable) // candidates for relay
    @Modifying @Transactional
    int tryClaim(UUID, String, String, String, LocalDateTime, LocalDateTime)  // CAS: PENDING→IN_FLIGHT with lease
    @Modifying @Transactional
    int markPublished(UUID, String, String, String, LocalDateTime)            // IN_FLIGHT→PUBLISHED (lease-guarded)
    @Modifying @Transactional
    int markFailed(UUID, String, String, String, String, LocalDateTime, String, int) // IN_FLIGHT→PENDING|DEAD_LETTER
}
```

#### C. Algorithm Profile
| Method | Pseudocode |
|--------|-----------|
| `findClaimable` | `WHERE (status=PENDING AND nextAttemptAt≤now) OR (status=IN_FLIGHT AND leaseUntil<now) ORDER BY createdAt ASC LIMIT batchSize` |
| `tryClaim` | `UPDATE … SET status=IN_FLIGHT, leaseId=?, leaseUntil=?, attemptCount+=1 WHERE eventId=? AND (PENDING+due OR IN_FLIGHT+expired)` — returns 1 if claimed, 0 if race lost |
| `markPublished` | `UPDATE … SET status=PUBLISHED WHERE eventId=? AND status=IN_FLIGHT AND leaseId=?` — lease-guarded |
| `markFailed` | `UPDATE … SET status=(IF attemptCount≥maxAttempts THEN DEAD_LETTER ELSE PENDING), nextAttemptAt=backoff, leaseId=null WHERE eventId=? AND leaseId=?` |


---

### 6.8 `service/InventoryService.java` — [Service]

#### A. Syntax Profile
- Java 17 · `@Service`, `@Transactional`, `@Retryable` + `@Backoff` (Spring Retry), `@Transactional(readOnly=true)`
- Constructor injection (no field injection); `Counter.builder` fluent API (Micrometer); `Optional.orElseThrow`; record access via method references

#### B. Structural Skeleton
```java
@Service
public class InventoryService {
    private final InventoryStockRepository stockRepository          // injected
    private final InventoryReservationRepository reservationRepository // injected
    private final InventoryOutboxRepository outboxRepository        // injected
    private final MeterRegistry meterRegistry                       // injected

    @Transactional @Retryable(…)
    public InventoryReservationResponse reserve(ReserveInventoryCommand)
    @Transactional @Retryable(…)
    public int allocateSeckillQuota(String sku, int qty)
    @Transactional @Retryable(…)
    public InventoryReservationResponse reserveFromSeckill(SeckillReserveCommand)
    @Transactional @Retryable(…)
    public void releaseSeckillQuota(String sku, int qty)
    @Transactional @Retryable(…)
    public InventoryReservationResponse commit(String orderId)
    @Transactional @Retryable(…)
    public InventoryReservationResponse release(String orderId, String reason)
    @Transactional @Retryable(…)
    public int expireReservations()
    @Transactional(readOnly=true)
    public InventoryStock getStock(String sku)
    @Transactional(readOnly=true)
    public List<InventoryOutboxEvent> pendingOutbox()

    private void releaseLockedReservation(InventoryReservation, ReservationStatus) // shared release logic
    private void recordOutbox(String aggregateId, String eventType, String payloadJson)
    private void increment(String metricName)
    private static InventoryReservationResponse response(InventoryReservation)
    private static void validate(ReserveInventoryCommand)
    private static void ensureSameRequest(InventoryReservation, …)
    private static void validateSeckill(SeckillReserveCommand)
    private static void requireStatus(InventoryReservation, ReservationStatus)
    private static String json(String orderId, String sku, int qty)   // manual JSON serialisation
    private static void requireText(String value, String field)
}
```

#### C. Algorithm Profile
| Method | Pseudocode |
|--------|-----------|
| `reserve(cmd)` | 1.validate → 2.find by idempotencyKey (if exists: check same args, return) → 3.findByOrderIdForUpdate (if exists: throw dup) → 4.findBySkuForUpdate → 5.stock.reserve(qty) → 6.save reservation → 7.recordOutbox("inventory.reserved") → 8.increment metric → 9.return response |
| `commit(orderId)` | 1.findByOrderIdForUpdate → 2.if COMMITTED: return (idempotent) → 3.requireStatus(RESERVED) → 4.findBySkuForUpdate → 5.stock.commit(qty) → 6.status=COMMITTED → 7.recordOutbox("inventory.committed") |
| `release(orderId, reason)` | 1.findByOrderIdForUpdate → 2.if already terminal: return (idempotent) → 3.requireStatus(RESERVED) → 4.releaseLockedReservation(RELEASED) |
| `expireReservations()` | 1.findByStatusAndExpiresAtBefore(RESERVED, now) → 2.for each: re-lock row → 3.if still RESERVED AND expired: releaseLockedReservation(EXPIRED) → 4.return count |
| `releaseLockedReservation` | findBySkuForUpdate → stock.release(qty) → status=target → recordOutbox("inventory.released") → if EXPIRED: also recordOutbox("reservation.expired") |
| `recordOutbox` | save(new InventoryOutboxEvent(aggregateId, eventType, payloadJson)) — written in same TX |

Retry policy on all writes: `{CannotAcquireLockException, DeadlockLoserDataAccessException, PessimisticLockingFailureException}` → max 3 attempts, backoff 50ms×2


---

### 6.9 `controller/InventoryController.java` — [Controller]

#### B. Structural Skeleton
```java
@RestController @RequestMapping("/internal/inventory")
public class InventoryController {
    private final InventoryService inventoryService        // injected
    private final SeckillAdmission seckillAdmission        // injected (interface)

    @PostMapping("/reservations") public InventoryReservationResponse reserve(@RequestBody ReserveInventoryCommand)
    @PostMapping("/reservations/{orderId}/commit") public InventoryReservationResponse commit(@PathVariable String)
    @PostMapping("/reservations/{orderId}/release") public InventoryReservationResponse release(@PathVariable String, @RequestParam String reason)
    @GetMapping("/stock/{sku}") public InventoryStock stock(@PathVariable String)
    @GetMapping("/outbox") public List<InventoryOutboxEvent> pendingOutbox()
    @PostMapping("/seckill/quota/{sku}") public SeckillAdmissionResponse allocateSeckillQuota(@PathVariable String, @RequestParam int qty)
    @PostMapping("/seckill/reservations") public SeckillAdmissionResponse seckillReserve(@RequestBody SeckillReserveCommand)
}
```
All methods are thin delegates — no business logic. HTTP→Service translation only.

---

### 6.10 `controller/ApiExceptionHandler.java` — [Controller]

#### B. Structural Skeleton
```java
@RestControllerAdvice
public class ApiExceptionHandler {
    @ExceptionHandler(IllegalArgumentException.class) ProblemDetail badRequest(ex)  // → HTTP 400
    @ExceptionHandler(IllegalStateException.class) ProblemDetail conflict(ex)        // → HTTP 409
    private static ProblemDetail problem(HttpStatus, RuntimeException)               // shared builder
}
```
Maps domain exceptions to RFC 9457 ProblemDetail responses.

---

### 6.11 `messaging/DomainEventPublisher.java` — [Infrastructure]

Single-method port interface: `void publish(DomainEvent event)`. Three implementations selected by `@ConditionalOnProperty(messaging.transport)`:
- `logging` (default/PoC) → `LoggingDomainEventPublisher`
- `kafka` → `KafkaDomainEventPublisher`
- `sqs` → `SqsDomainEventPublisher`

---

### 6.12 `messaging/InventoryOutboxRelay.java` — [Infrastructure]

#### A. Syntax Profile
- `@Component`, `@Scheduled(fixedDelayString)`, `@Value` constructor injection
- `record ClaimedEvent(InventoryOutboxEvent, String leaseId)` — Java 16 record as private inner type
- `PageRequest.of(0, batchSize)` for batch size limit; UUID for lease IDs

#### B. Structural Skeleton
```java
@Component
public class InventoryOutboxRelay {
    private final InventoryOutboxRepository outboxRepository  // injected
    private final DomainEventPublisher eventPublisher         // injected (port)
    private final int batchSize                               // @Value
    private final long leaseMs, retryBaseMs, retryMaxMs       // @Value
    private final int maxAttempts                             // @Value

    @Scheduled public void publishPendingEvents()             // void: poll loop
    private List<ClaimedEvent> claim(LocalDateTime now)       // private: CAS batch claim
    private DomainEvent toDomainEvent(InventoryOutboxEvent)   // private: entity→contract
    private long retryDelay(int attempt)                      // private long: exponential backoff
    private static String errorMessage(RuntimeException)      // private static String: truncated error
    private record ClaimedEvent(InventoryOutboxEvent, String leaseId) {}
}
```

#### C. Algorithm Profile
| Method | Pseudocode |
|--------|-----------|
| `publishPendingEvents` | 1.claim(now) → 2.for each claimed: try publish → markPublished; catch: markFailed(backoff, DLQ if maxAttempts) |
| `claim(now)` | 1.findClaimable(PENDING, IN_FLIGHT, now, batchSize) → 2.for each: tryClaim(leaseId, leaseUntil=now+leaseMs) → if updated==1: add to claimed |
| `retryDelay(attempt)` | Doubles `retryBaseMs` for each attempt, caps at `retryMaxMs`: `delay = min(retryMaxMs, delay*2)` |


---

### 6.13 `seckill/RedisSeckillAdmission.java` — [Infrastructure]

#### A. Syntax Profile
- `@Component`, `@ConditionalOnProperty(seckill.redis.enabled=true)`
- `DefaultRedisScript<String>` with embedded Lua script (text block)
- `redisTemplate.execute(script, keys, args)` — atomic Lua execution
- Static nested helpers; `switch` expression (Java 14+)

#### B. Structural Skeleton
```java
@Component @ConditionalOnProperty(seckill.redis.enabled=true)
public class RedisSeckillAdmission implements SeckillAdmission {
    private static final DefaultRedisScript<String> ADMIT_SCRIPT  // static final: Lua script
    private final StringRedisTemplate redisTemplate                // injected
    private final InventoryService inventoryService                // injected
    private final MeterRegistry meterRegistry                      // injected
    private final long dedupTtlSeconds, streamMaxLength            // @Value via env

    public SeckillAdmissionResponse initializeQuota(String sku, int qty) // quota allocation
    public SeckillAdmissionResponse reserve(SeckillReserveCommand)        // Lua admission
    private static String key(String type, String sku)                    // private static String: Redis key builder
    private static void validate(SeckillReserveCommand)                   // private static void: guard
    private static boolean blank(String)                                  // private static boolean: null/blank check
    private void increment(String outcome)                                 // private void: Micrometer counter
    private static String admissionOutcome(String result)                 // private static String: result→label
}
```

#### C. Algorithm Profile
| Method | Pseudocode |
|--------|-----------|
| `initializeQuota(sku, qty)` | 1.validate → 2.inventoryService.allocateSeckillQuota (DB TX) → 3.delete stale Redis keys → 4.SET stock:{sku}=qty; on Redis timeout: if key absent → releaseSeckillQuota (rollback) |
| `reserve(cmd)` | 1.validate → 2.execute Lua ADMIT_SCRIPT → 3.parse result: ACCEPTED → return accepted; DUPLICATE/SOLD_OUT/NOT_INITIALIZED → return rejected |
| **Lua ADMIT_SCRIPT** | 1.HGET requests:{sku} idempotencyKey → if exists: return DUPLICATE → 2.SISMEMBER users:{sku} userId → if member: return DUPLICATE_USER → 3.GET stock:{sku} → if nil: NOT_INITIALIZED; if <qty: SOLD_OUT → 4.DECRBY stock qty → SADD users userId → HSET requests idempotencyKey orderId → XADD stream:{sku} {orderId,userId,sku,qty,idempotencyKey} → EXPIRE → return ACCEPTED:streamId |

---

### 6.14 Remaining Files (Summary)

| File | Layer | Key Points |
|------|-------|-----------|
| `seckill/RedisSeckillStreamConsumer.java` | Infrastructure | `@Scheduled` poll; reads Redis Stream per SKU; calls `reserveFromSeckill`; XACK on success; DLQ after `maxDeliveryAttempts` via attempt hash; releases quota on DLQ |
| `seckill/DisabledSeckillAdmission.java` | Infrastructure | No-op: throws `IllegalStateException` on both methods; active when `seckill.redis.enabled=false` (default) |
| `seckill/SeckillAdmissionResponse.java` | Model/Entity | Java record: `(String status, String detail)`; static factories `accepted()` / `rejected()` |
| `messaging/KafkaDomainEventPublisher.java` | Infrastructure | `@ConditionalOnProperty(messaging.transport=kafka)`; `kafkaTemplate.send(topic, key, json).get()` — blocking for ack |
| `messaging/SqsDomainEventPublisher.java` | Infrastructure | `@ConditionalOnProperty(messaging.transport=sqs)`; `SqsAsyncClient.sendMessage().get()`; adds `MessageGroupId` + `DeduplicationId` if FIFO |
| `messaging/SqsInventoryReservationCommandConsumer.java` | Infrastructure | `@ConditionalOnProperty(inventory.reservation.mode=sqs-fifo)`; polls FIFO command queue; calls `reserve/cancelIfPresent`; sends result to result queue; does NOT delete on transient error (SQS retries) |
| `messaging/SqsMessagingConfiguration.java` | Config | `@Bean SqsAsyncClient` with region + optional endpoint override; `destroyMethod="close"` |
| `observability/OutboxMetrics.java` | Infrastructure | `@Scheduled(10s)`: polls `countByStatusIn` for depth/pending/inFlight/deadLetter + oldest age; registers Micrometer `Gauge`s |
| `observability/KafkaConsumerLagMetrics.java` | Infrastructure | `@ConditionalOnProperty(management.kafka.lag.enabled=true)`; uses Kafka `AdminClient` to compute `endOffset - committedOffset` per partition; exposes `oms_kafka_consumer_lag` gauge |
| `observability/SqsQueueMetrics.java` | Infrastructure | `@ConditionalOnProperty(messaging.transport=sqs)`; polls `GetQueueAttributes` for visible/inFlight/delayed; registers per-queue Micrometer gauges |
| `observability/RequestTraceLoggingFilter.java` | Infrastructure | `OncePerRequestFilter`; logs `method path status traceId spanId durationMs` after response; reads MDC for trace context |
| `config/InventoryDataInitializer.java` | Config | `CommandLineRunner` bean; seeds 3 SKUs if table empty; local/test only |
| `config/InventorySchedulingConfig.java` | Config | `@EnableScheduling @EnableRetry`; `ThreadPoolTaskScheduler` with 2 threads, graceful shutdown (15s) |
| `config/ReservationExpirationScheduler.java` | Infrastructure | `@Scheduled(60s)`; calls `inventoryService.expireReservations()`; logs count |

---


## 7. AI Reconstruction Prompt

```text
PROJECT: inventory-service
PURPOSE: Inventory bounded context for an OMS PoC. Sole writer of stock, reservation, and outbox state.
         Supports normal reservations (pessimistic DB lock) and flash-sale (Redis/Lua → Redis Stream → DB).

TECH STACK:
  Language:  Java 17
  Framework: Spring Boot 3 (Web, Data JPA, Data Redis, Kafka, Actuator, Retry)
  Build:     Maven (spring-boot-maven-plugin fat JAR)
  DB (local): H2 file-mode (PostgreSQL compatibility)
  DB (prod):  Aurora PostgreSQL 15 via RDS Proxy
  Schema:    Flyway (disabled in PoC; enabled via FLYWAY_ENABLED=true)
  Messaging: pluggable — logging (default) | kafka | sqs (via messaging.transport property)
  Metrics:   Micrometer + Prometheus (/actuator/prometheus)
  Tracing:   OTEL via micrometer-tracing-bridge-otel (disabled by default)

RUNTIME PREREQUISITES:
  JDK 17+
  Maven 3.8+
  No external services needed for PoC (H2 + logging transport)
  Production env vars: DB_PROXY_ENDPOINT, DB_NAME, DB_USERNAME, DB_PASSWORD, DB_DDL_AUTO=validate,
    FLYWAY_ENABLED=true, KAFKA_BOOTSTRAP_SERVERS or SQS_QUEUE_URL, OTEL_EXPORTER_OTLP_ENDPOINT
  Port: 8082

BUILD & RUN:
  mvn clean package -DskipTests
  java -jar target/inventory-service-1.0.0.jar
  # Or with Maven:
  mvn spring-boot:run

VERIFY:
  curl http://localhost:8082/actuator/health          → {"status":"UP"}
  curl http://localhost:8082/internal/inventory/stock/SKU-RED-001 → JSON with availableQty=120

FILE MANIFEST (src/main/java/com/poc/inventory/):
  InventoryServiceApplication.java
  entity/InventoryStock.java
  entity/InventoryReservation.java
  entity/InventoryOutboxEvent.java
  repository/InventoryStockRepository.java
  repository/InventoryReservationRepository.java
  repository/InventoryOutboxRepository.java
  service/InventoryService.java
  controller/InventoryController.java
  controller/ApiExceptionHandler.java
  messaging/DomainEventPublisher.java
  messaging/LoggingDomainEventPublisher.java
  messaging/KafkaDomainEventPublisher.java
  messaging/SqsDomainEventPublisher.java
  messaging/InventoryOutboxRelay.java
  messaging/SqsInventoryReservationCommandConsumer.java
  messaging/SqsMessagingConfiguration.java
  seckill/SeckillAdmission.java
  seckill/SeckillAdmissionResponse.java
  seckill/DisabledSeckillAdmission.java
  seckill/RedisSeckillAdmission.java
  seckill/RedisSeckillStreamConsumer.java
  observability/OutboxMetrics.java
  observability/KafkaConsumerLagMetrics.java
  observability/SqsQueueMetrics.java
  observability/RequestTraceLoggingFilter.java
  config/InventoryDataInitializer.java
  config/InventorySchedulingConfig.java
  config/ReservationExpirationScheduler.java
  resources/application.properties
  resources/db/migration/V1__create_tables.sql

=== PER-FILE SPECIFICATIONS ===

-- InventoryServiceApplication.java [Entry-point]
@SpringBootApplication
public class InventoryServiceApplication {
    public static void main(String[] args) { SpringApplication.run(InventoryServiceApplication.class, args); }
}

-- entity/InventoryStock.java [Model/Entity — aggregate root]
@Entity @Table(name="inventory_stock")
Fields: @Id String sku | int availableQty | int reservedQty | int soldQty | int seckillAllocatedQty
        | LocalDateTime updatedAt | @Version Long version
Methods:
  void reserve(int qty)              — guard qty>0 AND available>=qty; available-=qty; reserved+=qty
  void allocateSeckillQuota(int qty) — guard qty>0 AND available>=qty AND seckillAllocated==0;
                                       available-=qty; seckillAllocated+=qty
  void acceptSeckillReservation(int) — seckillAllocated-=qty; reserved+=qty
  void releaseSeckillQuota(int)      — seckillAllocated-=qty; available+=qty
  void commit(int qty)               — reserved-=qty; sold+=qty
  void release(int qty)              — reserved-=qty; available+=qty
  @PrePersist @PreUpdate void touch() — updatedAt = LocalDateTime.now()
Invariant: available + reserved + sold + seckillAllocated = original total (enforced by guards)

-- entity/InventoryReservation.java [Model/Entity]
@Entity @Table(name="inventory_reservation",
  uniqueConstraints={uk_inventory_reservation_order(order_id), uk_inventory_reservation_idempotency(idempotency_key)})
Fields: @Id @GeneratedValue(UUID) UUID reservationId | @Column(updatable=false) String orderId,sku
        | @Column(updatable=false) int qty | @Enumerated(STRING) ReservationStatus status
        | @Column(updatable=false) String idempotencyKey | LocalDateTime expiresAt
        | LocalDateTime createdAt, updatedAt | @Version Long version
Status lifecycle: RESERVED → COMMITTED | RELEASED | CANCELLED | EXPIRED
Only mutable field: status (via setStatus())

-- entity/InventoryOutboxEvent.java [Model/Entity]
@Entity @Table(name="inventory_outbox_event")
Constants: PENDING, IN_FLIGHT, PUBLISHED, DEAD_LETTER
Fields: @Id UUID eventId | String aggregateId, eventType, payloadJson, status, leaseId, lastError
        | LocalDateTime createdAt, publishedAt, nextAttemptAt, leaseUntil | int attemptCount
Constructor: sets eventId=UUID.random(), status=PENDING, attemptCount=0
@PrePersist: createdAt = nextAttemptAt = now()
void markPublished(): status=PUBLISHED, publishedAt=now()
Note: status transitions (IN_FLIGHT, PUBLISHED, DEAD_LETTER) driven by bulk @Modifying queries in repo

-- repository/InventoryStockRepository.java [Repository/DAO]
extends JpaRepository<InventoryStock, String>
@Lock(PESSIMISTIC_WRITE) @Query("select s from InventoryStock s where s.sku=:sku")
Optional<InventoryStock> findBySkuForUpdate(@Param("sku") String sku)
→ SELECT … FROM inventory_stock WHERE sku=? FOR UPDATE (holds row lock for TX duration)

-- repository/InventoryReservationRepository.java [Repository/DAO]
extends JpaRepository<InventoryReservation, UUID>
Optional<InventoryReservation> findByIdempotencyKey(String) → WHERE idempotency_key=?
@Lock(PESSIMISTIC_WRITE) @Query Optional<InventoryReservation> findByOrderIdForUpdate(String) → FOR UPDATE
List<InventoryReservation> findByStatusAndExpiresAtBefore(ReservationStatus, LocalDateTime) → expiry scan

-- repository/InventoryOutboxRepository.java [Repository/DAO]
extends JpaRepository<InventoryOutboxEvent, UUID>
List findTop100ByStatusOrderByCreatedAtAsc(String status)
long countByStatusIn(List<String>)
LocalDateTime findOldestCreatedAt(@Param List<String> statuses)
List<InventoryOutboxEvent> findClaimable(pending, inFlight, now, Pageable)
  → WHERE (status=PENDING AND nextAttemptAt<=now) OR (status=IN_FLIGHT AND leaseUntil<now) ORDER BY createdAt
@Modifying int tryClaim(eventId, pending, inFlight, leaseId, leaseUntil, now)
  → UPDATE SET status=IN_FLIGHT, leaseId=?, leaseUntil=?, attemptCount+=1
     WHERE eventId=? AND ((PENDING+due) OR (IN_FLIGHT+expired)) — CAS, returns 1 if claimed
@Modifying int markPublished(eventId, inFlight, published, leaseId, publishedAt)
  → UPDATE SET status=PUBLISHED WHERE eventId=? AND status=IN_FLIGHT AND leaseId=?
@Modifying int markFailed(eventId, inFlight, pending, deadLetter, leaseId, nextAttemptAt, lastError, maxAttempts)
  → UPDATE SET status=IF(attemptCount>=maxAttempts, DEAD_LETTER, PENDING), nextAttemptAt=backoff, leaseId=null

-- service/InventoryService.java [Service — sole writer]
@Service; all write methods: @Transactional @Retryable(CannotAcquireLockEx|DeadlockLoserEx|PessimisticLockingEx,
  maxAttempts=3, backoff=@Backoff(delay=50, multiplier=2.0))
Constructor injection: InventoryStockRepository, InventoryReservationRepository,
  InventoryOutboxRepository, MeterRegistry

InventoryReservationResponse reserve(ReserveInventoryCommand):
  1. validate(command) — null/blank/qty>0/ttlMinutes>0
  2. findByIdempotencyKey → if exists: ensureSameRequest, return response (idempotent)
  3. findByOrderIdForUpdate → if exists: throw IllegalStateException("order already has reservation")
  4. findBySkuForUpdate → stock.reserve(qty)
  5. new InventoryReservation(orderId, sku, qty, idempotencyKey, now+ttlMinutes)
  6. reservationRepository.save(reservation)
  7. recordOutbox(orderId, "inventory.reserved", json)
  8. increment("inventory.reservation.created"); return response

InventoryReservationResponse commit(String orderId):
  1. findByOrderIdForUpdate → if COMMITTED: return (idempotent)
  2. requireStatus(RESERVED)
  3. findBySkuForUpdate → stock.commit(qty)
  4. reservation.setStatus(COMMITTED); recordOutbox("inventory.committed")

InventoryReservationResponse release(String orderId, String reason):
  1. findByOrderIdForUpdate → if already terminal (RELEASED|CANCELLED|EXPIRED): return (idempotent)
  2. requireStatus(RESERVED) → releaseLockedReservation(RELEASED)

int expireReservations():
  1. findByStatusAndExpiresAtBefore(RESERVED, now)
  2. for each: re-lock with findByOrderIdForUpdate
  3. if still RESERVED AND expiresAt<=now: releaseLockedReservation(EXPIRED)
  4. return count expired

void releaseLockedReservation(reservation, status):
  findBySkuForUpdate → stock.release(qty) → reservation.setStatus(status)
  → recordOutbox("inventory.released") → if EXPIRED: also recordOutbox("reservation.expired")

void recordOutbox(aggregateId, eventType, payloadJson):
  outboxRepository.save(new InventoryOutboxEvent(aggregateId, eventType, payloadJson))
  — written in same @Transactional

-- controller/InventoryController.java [Controller]
@RestController @RequestMapping("/internal/inventory")
POST /reservations → reserve(@RequestBody ReserveInventoryCommand)
POST /reservations/{orderId}/commit → commit(@PathVariable)
POST /reservations/{orderId}/release → release(@PathVariable, @RequestParam reason)
GET  /stock/{sku} → getStock(@PathVariable)
GET  /outbox → pendingOutbox()
POST /seckill/quota/{sku} → seckillAdmission.initializeQuota(sku, qty)
POST /seckill/reservations → seckillAdmission.reserve(@RequestBody SeckillReserveCommand)
All methods are thin delegates; no business logic.

-- controller/ApiExceptionHandler.java [Controller]
@RestControllerAdvice
IllegalArgumentException → HTTP 400 ProblemDetail
IllegalStateException → HTTP 409 ProblemDetail
Uses ProblemDetail.forStatusAndDetail(status, ex.getMessage())

-- messaging/DomainEventPublisher.java [Infrastructure — port]
interface: void publish(DomainEvent event)
Selected by @ConditionalOnProperty("messaging.transport"):
  "logging" (matchIfMissing=true) → LoggingDomainEventPublisher: log.info only
  "kafka" → KafkaDomainEventPublisher: kafkaTemplate.send(topic, event.kafkaKey(), json).get() blocking
  "sqs"   → SqsDomainEventPublisher: sqsClient.sendMessage(…).get(); FIFO: add MessageGroupId + DeduplicationId

-- messaging/InventoryOutboxRelay.java [Infrastructure — Outbox poller]
@Component; @Scheduled(fixedDelayString="${messaging.outbox.poll-ms:1000}")
Config via @Value: batchSize=100, leaseMs=30000, maxAttempts=8, retryBaseMs=1000, retryMaxMs=60000

void publishPendingEvents():
  1. claim(now) → List<ClaimedEvent>
  2. for each: eventPublisher.publish(toDomainEvent(event))
     → on success: markPublished(eventId, IN_FLIGHT, PUBLISHED, leaseId, now)
     → on RuntimeException: markFailed(…, nextAttemptAt=now+retryDelay, lastError, maxAttempts)

List<ClaimedEvent> claim(now):
  1. findClaimable(PENDING, IN_FLIGHT, now, PageRequest(0, batchSize))
  2. for each: tryClaim(eventId, leaseId=UUID.random(), leaseUntil=now+leaseMs, now)
  3. if tryClaim returned 1: add ClaimedEvent(event, leaseId) to result

long retryDelay(attempt): doubles retryBaseMs per attempt, capped at retryMaxMs

-- seckill/SeckillAdmission.java [Infrastructure — port]
interface:
  SeckillAdmissionResponse initializeQuota(String sku, int qty)
  SeckillAdmissionResponse reserve(SeckillReserveCommand command)
Implementations (ConditionalOnProperty seckill.redis.enabled):
  false (default) → DisabledSeckillAdmission: throws IllegalStateException on both methods
  true → RedisSeckillAdmission (see below)

-- seckill/RedisSeckillAdmission.java [Infrastructure — Redis/Lua admission]
@Component @ConditionalOnProperty(seckill.redis.enabled=true)
Redis key scheme: oms:seckill:{type}:{sku} where type ∈ {stock, users, requests, stream, attempts, dlq}

SeckillAdmissionResponse initializeQuota(sku, qty):
  1. inventoryService.allocateSeckillQuota(sku, qty) — DB TX carves quota from available_qty
  2. DELETE keys: users:{sku}, requests:{sku}, stream:{sku} (clear stale state)
  3. SET stock:{sku} = qty
  4. On Redis timeout: if stock key absent → releaseSeckillQuota (rollback DB quota)

SeckillAdmissionResponse reserve(SeckillReserveCommand):
  Execute Lua ADMIT_SCRIPT atomically:
  KEYS: [stock:{sku}, users:{sku}, requests:{sku}, stream:{sku}]
  ARGS: [orderId, userId, sku, qty, idempotencyKey, dedupTtlSeconds, streamMaxLength]
  Lua logic:
    1. HGET requests idempotencyKey → if exists: return "DUPLICATE:{orderId}"
    2. SISMEMBER users userId → if member: return "DUPLICATE_USER"
    3. GET stock → if nil: return "NOT_INITIALIZED"; if <qty: return "SOLD_OUT"
    4. DECRBY stock qty
    5. SADD users userId; HSET requests idempotencyKey orderId
    6. XADD stream MAXLEN ~ streamMaxLength * {orderId,userId,sku,qty,idempotencyKey}
    7. EXPIRE users dedupTtl; EXPIRE requests dedupTtl
    8. return "ACCEPTED:{streamId}"
  Parse result: starts with "ACCEPTED:" → return accepted(streamId); else → return rejected(result)

-- seckill/RedisSeckillStreamConsumer.java [Infrastructure — Redis Stream consumer]
@Component @ConditionalOnProperty(seckill.redis.enabled=true)
@Scheduled(fixedDelayString="${seckill.redis.consumer-poll-ms:100}")
For each configured SKU:
  1. ensureGroup(streamKey) — XGROUP CREATE if not exists (ignore BUSYGROUP)
  2. Read from offset "0-0" (re-process pending) then ">" (new messages)
  3. For each record: inventoryService.reserveFromSeckill(command from stream fields)
     → XACK on success; delete from attempts hash
  4. On failure: increment attempts hash; if attempts>=maxDeliveryAttempts:
     → copy to DLQ stream, releaseSeckillQuota, XACK and delete from attempts
DLQ key: oms:seckill:dlq:{sku}; TTL=dlqTtlSeconds; max length=dlqMaxLength

-- config/InventorySchedulingConfig.java [Config]
@Configuration @EnableScheduling @EnableRetry
@Bean ThreadPoolTaskScheduler: poolSize=2, threadNamePrefix="inventory-scheduler-",
  waitForTasksToCompleteOnShutdown=true, awaitTerminationSeconds=15

-- config/InventoryDataInitializer.java [Config]
@Bean CommandLineRunner: if repository.count()==0 → seed 3 SKUs:
  SKU-RED-001(120), SKU-BLK-002(80), SKU-BAT-004(240)

-- config/ReservationExpirationScheduler.java [Infrastructure]
@Scheduled(fixedDelayString="${inventory.reservation.expiration-scan-ms:60000}")
void releaseExpiredReservations(): inventoryService.expireReservations(); log count

-- observability/RequestTraceLoggingFilter.java [Infrastructure]
extends OncePerRequestFilter
doFilterInternal: record start nanos → chain.doFilter → log method/path/status/traceId/spanId/durationMs
Reads MDC "traceId" and "spanId" (set by micrometer-tracing-bridge-otel)

=== DATABASE SCHEMA (V1__create_tables.sql) ===
inventory_stock: PK=sku, available_qty>=0, reserved_qty>=0, sold_qty>=0, seckill_allocated_qty>=0, version
inventory_reservation: PK=reservation_id(UUID), UNIQUE order_id, UNIQUE idempotency_key, status VARCHAR(32),
  expires_at, INDEX(sku, status, expires_at) WHERE status='RESERVED'
inventory_outbox_event: PK=event_id(UUID), status, lease_id, lease_until, attempt_count, next_attempt_at,
  INDEX(status, next_attempt_at) WHERE status IN ('PENDING','IN_FLIGHT')

=== KEY CONSTRAINTS / GOTCHAS ===
1. Lock ordering: always acquire stock lock AFTER reservation lock to avoid deadlock.
   @Retryable handles the rare cases that still deadlock.
2. Seckill two-phase: DB quota MUST be allocated before Redis SET — if SET fails,
   roll back quota only if Redis key is absent (ambiguous timeout case).
3. Outbox lease expiry intentionally allows redelivery — consumers must be idempotent.
4. `expireReservations` re-locks each row individually — use small `expiration-scan-ms` to limit batch size.
5. H2 in PostgreSQL mode does not support all Aurora-specific syntax — run DDL tests against real Postgres.
6. RedisSeckillStreamConsumer reads from "0-0" first on every poll to reprocess unacknowledged messages
   from previous crashes before consuming new messages with ">".
```








