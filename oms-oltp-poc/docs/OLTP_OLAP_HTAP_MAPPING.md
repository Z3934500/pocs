# OLTP, OLAP and HTAP Mapping

## One-line View

OLTP is the system that runs the business transaction. OLAP is the system that explains what happened. HTAP narrows the gap, but production systems still protect the write path from analytical scans.

## Comparison

| Dimension | OLTP: OMS order system | OLAP: OEE and CCE platforms |
| --- | --- | --- |
| Goal | Create, update and finalize live orders | Analyze history and support decisions |
| Typical action | Place order, reserve stock, capture payment, cancel order | OEE trend, downtime Pareto, customer features, campaign eligibility |
| Latency target | Milliseconds to low hundreds of milliseconds | Seconds to minutes are often acceptable |
| Data state | Current mutable state | Historical, cleaned and curated data |
| Consistency | ACID and strong state transitions | Reproducible batch/stream outputs and lineage |
| Storage model | Row-store, 3NF, transactional indexes | Column-store/lakehouse, star or wide feature tables |
| Write pattern | Many small concurrent writes | Large scans, joins, aggregations and model jobs |
| Time handling | Current valid state and transaction timestamps | Historical timelines, snapshots, event history and slowly changing dimensions |

## Spring Boot Original And Python Companion

Keep `../../inventory-oms-poc` as the Spring Boot original. It is useful when explaining how the OMS would be organized in a Java enterprise microservice and DDD stack: bounded context, controller, application service, domain service, aggregate/entity, repository, domain event, Maven module and Spring runtime.

Keep `oms-oltp-poc` as the Python companion. It is useful when explaining OLTP vs OLAP beside the two Python analytical PoCs, because all three can be run and inspected with the same local Python/FastAPI style.

| Question | Spring Boot original | Python companion |
| --- | --- | --- |
| Is it replacing the other? | No. Keep it as the Java service reference. | No. Use it as a compact comparison and demo reference. |
| Best discussion | Enterprise OMS service implementation and DDD modeling | Data architecture and OLTP/OLAP boundary |
| Runtime style | Spring Boot, Maven, Java packages, DDD layers | FastAPI, SQLite, pytest |
| Production mapping | Natural fit for Java microservice teams | Maps concepts to MySQL/PostgreSQL, Redis, Kafka and CDC without Java setup overhead |
| Discussion angle | Shows framework familiarity, microservice boundaries, aggregates, repositories and domain events | Shows transaction design, Saga, Outbox and analytical handoff clearly |

## DDD Mapping

| DDD concept | OMS example |
| --- | --- |
| Bounded context | Order, Inventory, Payment, Fulfillment, Notification |
| Aggregate root | `Order` controls order status and order items |
| Entity / value object | `OrderItem`, reservation quantity, payment reference |
| Repository | Order repository, reservation repository, inventory repository |
| Domain service | Inventory reservation, payment capture decision, timeout release |
| Domain event | `order.created`, `inventory.reserved`, `payment.captured`, `inventory.released` |
| Integration pattern | Outbox publishes domain events to Kafka / downstream OLAP |

This is why the Spring Boot microservice version is worth keeping: it can show the DDD shape explicitly in Java packages and service boundaries, while the Python version keeps the same business flow compact for architecture demos.

## How the Inventory Diagram Maps to This PoC

| Diagram block | Local PoC implementation | Production mapping |
| --- | --- | --- |
| Client / app | `POST /api/orders` | Checkout API / mobile / POS |
| Redis reservation | conditional stock update in `sku_inventory` | Redis + Lua hot-stock reservation |
| Database system of record | SQLite tables | MySQL or PostgreSQL |
| Outbox pattern | `outbox_events` table | Debezium / Kafka Connect / transactional outbox |
| Kafka event bus | `POST /api/outbox/publish` marks events published | Kafka topics such as `inventory.reserved` and `order.confirmed` |
| Saga happy path | `capture_payment(... succeed=True)` | Payment service, inventory service, fulfillment service |
| Compensation path | cancel, failed payment and timeout release stock | Saga orchestrator / choreography |
| Timeout handler | `POST /api/reservations/expire` | scheduled worker / delayed queue |
| Reconciliation | inventory movement and status history tables | periodic ERP/WMS reconciliation jobs |

## Time And Change Capture For OLAP

OLTP systems often overwrite the current state because operational correctness depends on the latest truth. OLAP systems need to preserve the timeline because analysis depends on what was true at a particular point in time.

| Pattern | What it captures | OMS example | OLAP use |
| --- | --- | --- | --- |
| Event history | Every business change as an immutable fact | `order.created`, `inventory.reserved`, `payment.captured` | Funnel analysis, cycle-time analysis, compensation monitoring |
| Status history | Lifecycle transitions for one aggregate | `RESERVED -> CONFIRMED -> SHIPPED` | SLA tracking, stuck-order detection, operational trend reports |
| Periodic snapshot | State at a regular point in time | daily available/reserved/sold stock | Inventory turnover, stockout risk, end-of-day reporting |
| SCD Type 1 | Latest dimension attributes only | current SKU name or current customer phone | Simple lookup when history is not required |
| SCD Type 2 | Versioned dimension attributes with effective dates | customer segment at order time, SKU category at sale time | Correct historical reporting after customer/product attributes change |

For example, if a customer moves from `Retail` to `VIP`, OLTP only needs the latest customer segment for current service behavior. OLAP may need both versions so last quarter's orders still report under the segment that was true at order time.

## Data Contracts, Metadata And Idempotency

The OMS event stream is not only integration plumbing. It is also the contract boundary between operational systems and analytical systems.

| Layer | Responsibility | OMS / OLAP example |
| --- | --- | --- |
| Data contract | Defines the producer-consumer promise | Event schema, field meaning, required fields, time semantics, quality rules, owner, freshness SLA |
| Metadata governance | Records and controls the contract | Catalog entry, table owner, lineage, access policy, audit history, schema version |
| Delta / lakehouse table state | Makes curated data reliable to read and reproduce | ACID writes, schema enforcement, table history, time travel, change feed |
| ETL / ELT processing | Executes the contract in the right order | Normalize raw payloads, validate quality, deduplicate, merge, build SCD dimensions and publish snapshots |

Unity Catalog and Delta Lake are strong infrastructure pieces for this layer because they provide governed metadata, access control, lineage, auditability, schema enforcement/evolution and reliable versioned tables. They are not the business contract by themselves. The team still has to define what `event_time`, `source_updated_at`, `available_stock`, `customer_segment`, `effective_from` and the freshness SLA mean.

Time is the reason OLAP processing cares so much about sequence. A late event, duplicate event or out-of-order update can change the historical answer. The pipeline therefore needs explicit ordering and replay rules: which timestamp is business time, which timestamp is ingestion time, which source wins during conflicts, and how late-arriving data is corrected.

Data format is the other half of the contract. OLAP jobs need predictable shape because the same data is repeatedly scanned, joined and aggregated. Stable field names, types, nullability, units, timezone rules, enum values and nested payload structure decide whether downstream models are reproducible.

For OLTP, idempotency usually means retry-safe commands. For OLAP, idempotency means replay-safe history. In other words, OLTP identity is often the current record or command. OLAP identity is often the record version in time.

| OLAP pattern | Idempotent key idea | Example |
| --- | --- | --- |
| Immutable event facts | `event_id`, or `business_key + event_type + event_time` | One `inventory.reserved` event is loaded once even if Kafka redelivers it |
| CDC merge | `source_pk + source_updated_at + sequence_number` | Later update for the same order wins deterministically |
| Periodic snapshot | `entity_key + snapshot_date` | One inventory balance row per SKU per day |
| SCD Type 2 dimension | `natural_key + valid_from`, with a surrogate dimension key | Customer segment version valid at order time |
| Record comparison | `record_hash` over meaningful attributes | Detect whether a dimension version actually changed |
| Batch replay | `batch_id` or source file identity plus row key | Reprocessing the same batch does not duplicate facts |

This is the practical bridge from OMS to OEE / CCE: OMS emits trustworthy operational events, CDC/Kafka preserves the sequence, Bronze keeps raw history, Silver enforces contracts and normalizes format, and Gold turns that versioned history into dashboards, features and forecasts.

## Event Contract

The local Outbox emits these event types:

```text
order.created
inventory.reserved
payment.captured
payment.failed
inventory.committed
inventory.released
order.confirmed
order.cancelled
order.timeout
shipment.created
```

Downstream analytical systems should consume events as immutable facts. They should not update the OMS row-store directly.

## Suggested Data Flow Across the Three PoCs

```text
OMS OLTP
  orders / payments / inventory movements / outbox events
        |
        v
Kafka or CDC
        |
        v
Bronze raw events
        |
        v
Silver standardized order, customer and stock facts
        |
        v
Gold marts and features
  OEE operational dashboards
  CCE campaign and customer features
  inventory turnover and stockout analytics
```

## Architecture Talking Point

Use OMS when the need is to ensure business transactions stay correct under concurrency. Use OEE/CCE when the discussion moves to historical analysis, warehouse modeling, feature engineering and operational dashboards. Then connect them with Outbox + CDC + Kafka to show the full enterprise data lifecycle.
