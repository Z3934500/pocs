# OMS OLTP PoC

Order-management OLTP proof of concept for high-concurrency inventory reservation, payment confirmation, Saga compensation and Outbox event handoff.

This sits beside the existing OLAP/data-platform PoCs:

| PoC | Mode | Core story |
| --- | --- | --- |
| `oms-oltp-poc` | OLTP | Process live orders, reserve inventory, capture payment, compensate failures and emit reliable events |
| `oee-data-platform` | OLAP | Analyze historical equipment events through Bronze -> Silver -> Gold analytical layers |
| `cce-feature-platform` | OLAP plus online serving | Build customer features, segmentation and campaign eligibility from historical and CDC-fed data |

It also sits beside the original Spring Boot inventory OMS reference at `../inventory-oms-poc`. Keep that project as the Java / Spring Boot microservice and DDD version. This Python version is a companion for fast local demos and side-by-side comparison with the Python OLAP PoCs.

## What This Shows

- 3NF-style transactional model: customers, orders, order items, payments and inventory reservations
- ACID order placement: reserve stock and create an order in one SQLite transaction
- Oversell protection with conditional inventory updates
- Idempotency key for repeated checkout requests
- Saga paths for commit and compensation
- Outbox table as the durable handoff to Kafka, CDC, warehouse ingestion or downstream services
- Timeout handler for expired reservations
- FastAPI service and small transaction console

## Architecture

```text
Client / Checkout
        |
        v
FastAPI OMS OLTP service
        |
        v
SQLite row-store tables
  customers / orders / order_items / payments
  sku_inventory / inventory_reservations
  outbox_events / saga_log
        |
        v
Outbox publisher
        |
        v
Kafka / CDC / downstream OLAP systems
        |
        +--> OEE / CCE style Bronze -> Silver -> Gold analytics
```

## Core Flow

```text
Place order
  -> reserve inventory in the transaction
  -> write order + order_items + reservation
  -> append order.created and inventory.reserved outbox events

Payment success
  -> reserved_stock decreases
  -> sold_stock increases
  -> order becomes CONFIRMED
  -> append payment.captured, inventory.committed, order.confirmed

Payment failure / cancel / timeout
  -> reserved_stock decreases
  -> available_stock increases
  -> order becomes PAYMENT_FAILED or CANCELLED
  -> append inventory.released and cancellation/timeout events
```

## Local Run

From this directory:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
python -m pip install -e .
$env:PYTHONPATH="src"
python -m oms_oltp.demo
python -m uvicorn oms_oltp.api:app --host 127.0.0.1 --port 8030
```

Open:

```text
http://127.0.0.1:8030
```

Useful APIs:

```text
POST /api/demo/reset
POST /api/orders
POST /api/orders/{order_id}/payment
POST /api/orders/{order_id}/cancel
POST /api/orders/{order_id}/ship
POST /api/reservations/expire
POST /api/outbox/publish
GET  /api/summary
GET  /api/inventory
GET  /api/orders
GET  /api/outbox
GET  /api/lineage
```

## OLTP vs OLAP Mapping

OLTP is the system that does the work. This PoC is the source-of-record transaction side: low-latency writes, high concurrency, current state, strict consistency and operational status transitions.

OLAP is the system that reads the history. The existing OEE and CCE projects show the analytical side: batch/stream ingestion, denormalized Gold outputs, historical scans, feature computation and dashboards.

In a production version, OMS would typically run on MySQL or PostgreSQL, use Redis + Lua for hot inventory reservation, publish Outbox events to Kafka, and feed analytical stores such as ClickHouse, Doris, Databricks or a lakehouse. This keeps checkout latency and analytical scans from interfering with each other while still allowing HTAP-style near-real-time reporting through CDC.

## Spring Boot Original vs Python Companion

| Version | Best for | Notes |
| --- | --- | --- |
| `../inventory-oms-poc` | Java / Spring Boot microservice and DDD discussion | Keeps the original enterprise-style controller, application service, domain service, aggregate/entity, repository, domain event and Maven project shape |
| `oms-oltp-poc` | OLTP vs OLAP architecture discussion beside OEE and CCE | Uses Python + FastAPI + SQLite so the transaction logic, tests and Outbox flow are compact and easy to inspect |

The Python version does not replace the Spring Boot version. The useful story is the comparison: Spring Boot shows how OMS fits a Java enterprise delivery stack and a DDD-style microservice model; Python shows the same OLTP concepts in the same language and runtime style as the OEE and CCE analytical PoCs.

### DDD Fit

The OMS domain naturally maps to DDD. Order, Inventory, Payment and Fulfillment can be separate bounded contexts or service boundaries. `Order` is a good aggregate root, `OrderItem` belongs inside the aggregate, repositories persist aggregate state, domain services handle rules that cross aggregates, and domain events feed Saga / Outbox integration.

## Design Narrative

The inventory flow chart uses Redis, Kafka and Saga to protect checkout under high concurrency. This local PoC implements the same business contract in a compact way: SQLite represents the row-store OLTP database, conditional `UPDATE ... WHERE available_stock >= qty` represents oversell protection, `outbox_events` represents Kafka handoff, and the service methods model the happy path plus compensation path.

The point is the boundary: the OLTP service owns current order and stock truth. OLAP platforms consume committed history and build views for reporting, forecasts, OEE dashboards, customer features and campaign decisions.