# Relevant Data Terminology

This note keeps the Data-direction background terms out of the main portfolio README while preserving the architecture explanation used during walkthroughs.

It covers OLTP vs OLAP, modeling choices, sharding boundaries, change capture, data contracts, lakehouse governance, HTAP and the Java/Python delivery split across the PoCs.

## OLTP vs OLAP

| Dimension | OLTP: operational system | OLAP: analytical system |
| --- | --- | --- |
| Core purpose | Add, update and delete live business records | Query, aggregate and analyze business history |
| Typical examples | Bank withdrawal, ecommerce checkout, hotel booking | Quarterly report, user profile analysis, inventory turnover forecast |
| Response target | Milliseconds, high concurrency, strict state transitions | Seconds or minutes, complex scans over large data |
| Data state | Current mutable data | Historical, cleaned and curated data |
| Time semantics | Latest valid state, such as current stock or order status | State over time, including snapshots, event history and slowly changing dimensions |
| Consistency | ACID, idempotency and transaction boundaries | Reproducible lineage, data quality and governance |
| Storage model | Row-store such as MySQL or PostgreSQL | Column-store or lakehouse such as ClickHouse, Doris, Delta or Parquet |
| Data modeling | 3NF, less redundancy, safer writes | Star / snowflake / wide tables, more redundancy, faster reads |

## Modeling Guide

OLTP usually uses 3NF because write correctness matters most. For example, `oms-oltp-poc` keeps `orders`, `order_items`, `payments`, `sku_inventory` and `inventory_reservations` separate. This reduces duplicated facts and makes updates safer inside one transaction.

OLAP usually uses star or snowflake models because query speed and business readability matter most:

- Star model: one central fact table, such as `fact_order_line`, connected to dimensions such as `dim_customer`, `dim_sku`, `dim_date` and `dim_channel`. It is simple and fast for BI.
- Snowflake model: dimensions are normalized further, such as `dim_sku -> dim_category -> dim_brand`. It reduces dimension duplication but adds joins.
- Gold / feature tables: for dashboards or machine learning, data can be denormalized even more, such as daily OEE metrics or customer campaign features.

## Sharding vs Warehouse Modeling

OLTP sharding and OLAP warehouse modeling solve different layers of the system. Sharding or table-splitting is a physical routing strategy for transactional systems: it protects write throughput, point lookup latency and single-table capacity. A shard key, such as customer ID, order ID or phone hash, tells the application where the current record lives.

Warehouse modeling is a logical analytical model. Fact tables, dimension tables, star schemas and snowflake schemas describe how historical business events should be queried, aggregated and replayed. OLAP partitions, such as `business_date` or `ingestion_date`, are designed for scan pruning, backfill and cost control, not for OLTP request routing.

The two are upstream and downstream, not substitutes:

```text
OLTP sharded tables
  -> CDC / Outbox / ETL / ELT
  -> Bronze / ODS raw history
  -> Silver / DWD cleaned facts and dimensions
  -> Gold / DM feature tables, dashboards and ML datasets
```

A MySQL shard key should not automatically become a warehouse partition key. Likewise, a warehouse star schema should not be forced back into OLTP tables if it creates cross-shard joins or slow transactional writes.

## Time And Change Capture

A key OLAP concern is not only what the value is now, but how it changed over time. OLTP tables often update the latest state in place: an order moves from `RESERVED` to `CONFIRMED`, a customer changes segment, or a SKU moves from available stock to reserved stock. OLAP needs to preserve those transitions so analysts can answer questions such as what the customer segment was at order time, how long inventory stayed reserved, or what stock looked like at the end of each day.

This is where event history, CDC, snapshots and slowly changing dimensions matter:

- Event history captures facts as they happen, such as `order.created`, `inventory.reserved`, `payment.captured` and `inventory.released`.
- Periodic snapshots capture state at a point in time, such as daily inventory balance or daily customer feature values.
- SCD Type 1 overwrites dimension attributes when history is not needed.
- SCD Type 2 preserves dimension versions with effective dates, so historical facts can join to the correct dimension version.

So OLTP protects the current truth; OLAP preserves the timeline of truth.

## Data Contracts And Lakehouse Governance

The metadata story can be summarized as three layers:

- Data contract defines the producer-consumer promise: schema, field meaning, time semantics, quality rules, ownership and freshness expectations.
- Unity Catalog and Delta Lake provide lakehouse governance and reliable table state: catalog metadata, access control, lineage, audit, schema enforcement, table history, ACID writes and change capture capabilities.
- ETL / ELT executes the contract: normalize raw payloads, validate quality rules, deduplicate events, merge changes, build SCD dimensions, publish snapshots and make Gold tables reproducible.

The important distinction is that Unity Catalog and Delta Lake provide strong infrastructure for metadata and table governance, but the business data contract still has to be designed by the team. A table can be governed and versioned, but the team still needs to define what `event_time`, `effective_from`, `customer_segment`, `available_stock` and freshness SLA actually mean.

For OLTP, idempotency usually prevents duplicate commands, such as retrying the same checkout request or payment capture. For OLAP, idempotency prevents duplicate or incorrect historical facts. That usually means using a stable `event_id` when available, or a version-aware key such as `business_key + event_time`, `business_key + source_updated_at`, `business_key + effective_from`, plus optional `sequence_number`, `batch_id` or `record_hash`. In OLTP, identity is often the record. In OLAP, identity is often the record version in time.

The runnable version of this idea is `data-governance-poc`: it turns the contract into SRE-style checks for schema drift, payload shape, freshness, pending lag, timestamp deviation, duplicate semantic events and inventory reconciliation.

## HTAP Note

Modern databases increasingly advertise HTAP, meaning one engine can support both transactional and analytical workloads. In real production architecture, teams still usually separate hot writes from heavy reads through read replicas, CDC, Kafka, materialized views, lakehouse tables or dedicated OLAP stores. The reason is practical: checkout traffic and large dashboard scans should not slow each other down.

## Spring Boot Original vs Python Companion

The original `inventory-oms-poc` should be kept as the Spring Boot / Java reference. It is closer to a production enterprise microservice stack, especially when the discussion is about Spring Boot controllers, services, repositories, Maven modules, DDD tactical patterns and a Java team delivery model.

The new `oms-oltp-poc` is a Python companion version, not a replacement. It exists to make the same OLTP ideas easy to compare with the two Python data-platform PoCs:

| Version | Best for | Why keep it |
| --- | --- | --- |
| `inventory-oms-poc` | Java / Spring Boot OMS microservice and DDD discussion | Shows the enterprise service shape: controller, application service, domain service, aggregate/entity, repository, domain event, Outbox, Maven modules and Spring ecosystem fit |
| `oms-oltp-poc` | Data-platform architecture discussion beside OEE and CCE | Uses the same Python + FastAPI style as the OLAP PoCs, so the OLTP vs OLAP contrast is easier to run and explain locally |

The two versions should be compared, not collapsed into one. Spring Boot is the stronger reference for production Java service implementation and DDD-style microservice boundaries. Python is the stronger reference for compact architecture explanation, local tests and side-by-side comparison with OEE / CCE.

## Microservices And DDD

Microservices and DDD fit this OMS story well because each service can align to a bounded context: Order, Inventory, Payment, Fulfillment and Notification. Inside the Order context, `Order` can be the aggregate root, `OrderItem` belongs inside the aggregate, repositories persist aggregates, domain services enforce cross-aggregate rules, and domain events such as `order.created`, `inventory.reserved` and `order.confirmed` become the clean handoff to Kafka / Outbox.