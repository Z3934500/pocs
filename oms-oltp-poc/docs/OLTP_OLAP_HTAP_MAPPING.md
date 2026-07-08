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