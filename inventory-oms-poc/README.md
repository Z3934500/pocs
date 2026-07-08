# Inventory OMS PoC

Spring Boot / Java reference for high-concurrency inventory reservation, Kafka-ready event handoff, Saga compensation and reconciliation.

This is the original OMS microservice-style PoC. Keep it beside `../oms-oltp-poc`: this project shows the Java / Spring Boot / DDD service shape, while the Python companion keeps the same OLTP ideas compact for side-by-side comparison with the OEE and CCE data-platform PoCs.

## Architecture Diagram

<img src="docs/images/high-concurrency-inventory-system-design.jpg" alt="High concurrency inventory system design with Redis, Kafka and Saga" width="100%">

## What This Shows

- Inventory reservation service using a Spring Boot-style controller / service / repository layout
- DDD-friendly OMS boundaries: Order, Inventory, Payment, Fulfillment and Notification
- High-concurrency stock reservation pattern with Redis + Lua in the target architecture
- Database as system of record for reservations, orders and Outbox events
- Kafka-ready event flow for `inventory.reserved`, `inventory.committed`, `inventory.released` and timeout events
- Saga happy path and compensation path for payment failure, cancellation or reservation timeout
- Reconciliation job for inventory consistency checks against ERP / WMS / finance systems

## Local Structure

```text
inventory-oms-poc/
  README.md
  run.sh
  docker-compose.yml
  Makefile
  reservation-service/
    pom.xml
    src/main/java/com/poc/reservation/
      ReservationApplication.java
      controller/ReservationController.java
      service/ReservationService.java
      repository/ReservationRepository.java
      entity/Reservation.java
  reconciliation-job/
    pom.xml
    src/main/java/com/poc/recon/ReconciliationJob.java
```

## Design Narrative

For OLTP, the inventory service owns the current stock truth. The hot path reserves stock quickly, persists the reservation, and emits durable events through an Outbox/Kafka-style handoff. Downstream services then create orders, capture payments, update fulfillment status and notify users.

The same events can later feed OLAP systems. That is the bridge to the other PoCs in this repository: OMS produces operational facts, while OEE and CCE consume historical data for dashboards, features and decision support.