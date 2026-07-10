# Delivery Portfolio PoC

This repository supports the delivery portfolio behind my website. It is organized as two practical consulting/delivery directions: **Data** and **Automation**.

## Portfolio Directions

| Direction | What it proves | Representative PoCs |
| --- | --- | --- |
| Data | OLTP/OLAP boundaries, CDC, data contracts, medallion modeling, feature platforms and MLOps | `oms-oltp-poc`, `inventory-oms-poc`, `data-governance-poc`, `oee-data-platform`, `cce-feature-platform` |
| Automation | Enterprise knowledge operations, GenAI/RAG, CI/CD, VPC delivery patterns and workflow automation | [Z3934500/KB](https://github.com/Z3934500/KB), `knowledge-cockpit` |
| Presenter / Website | Demo control, narrative handoff and deployable public-facing explanation | `knowledge-cockpit`, `your own domain` deployment notes |

The Data direction explains how business systems produce reliable analytical and ML-ready data. The Automation direction explains how cloud-native delivery, AI and CI/CD reduce repeated manual work and package enterprise knowledge into operated workflows.

In simple Data terms:

- OLTP is the system that does the work: place order, reserve stock, capture payment, cancel booking.
- OLAP is the system that looks at the numbers: sales trend, OEE dashboard, customer features, inventory turnover forecast.

In simple Automation terms:

- RAG is the system that searches governed knowledge first, then asks the LLM to answer with evidence.
- CI/CD is the system that turns app, infrastructure, prompt and evaluation changes into controlled releases.

## Projects

| PoC | Mode | Focus area | Core story |
| --- | --- | --- | --- |
| `inventory-oms-poc` | OLTP | Spring Boot OMS microservice, DDD, inventory reservation | Original Java service reference for controller/service/repository layering, bounded contexts, domain events and Saga handoff; includes the [Redis + Kafka + Saga architecture diagram](inventory-oms-poc/README.md#architecture-diagram) |
| `oms-oltp-poc` | OLTP | Order management, inventory reservation, Saga, Outbox | Live order processing with ACID stock reservation, payment commit, compensation and event handoff |
| `data-governance-poc` | Data governance / Data SRE | Executable contracts, freshness, drift and reconciliation checks | Monitors the OMS Outbox and inventory state so downstream OLAP tables and ML features are trustworthy |
| `oee-data-platform` | OLAP | Industrial equipment analytics, medallion data platform | Multi-site OEE ingestion, schema standardization, data quality, anomaly detection and dashboard |
| `cce-feature-platform` | OLAP + online serving | Databricks ETL, customer features, real-time feature store | Customer Campaign Engine / CDP style platform with identity resolution, segmentation, eligibility, API consumption and [big-data EMR/Delta extension](cce-feature-platform/docs/BIG_DATA_EMR_DELTA_EXTENSION.md) |
| [Z3934500/KB](https://github.com/Z3934500/KB) | GenAI automation | Enterprise RAG knowledge base | Standalone repo for the lightweight S3 + Bedrock path, medium vector DB path, fine-tuning/private-runtime path, VPC diagrams and Jenkins/GitLab deployment trade-offs |
| `knowledge-cockpit` | Presenter / website | Demo control panel and AI KB shell | Installable PWA, phone remote control, presenter notes, repo Q&A and deployment path for your own domain |

## Data Relevant Topics

The longer Data-direction explanation now lives in [Relevant Data Terminology](docs/RELEVANT_DATA_TERMINOLOGY.md). 

<details>
<summary><strong>Data Relevant Topics</strong></summary>

- OLTP vs OLAP: operational writes and state transitions vs analytical history and aggregate reads.
- Modeling guide: 3NF for transactional correctness; star, snowflake and Gold tables for analytics and ML features.
- Sharding vs warehouse modeling: OLTP routing keys are not automatically OLAP partition keys.
- Time and change capture: CDC, event history, snapshots and SCD preserve how facts evolve.
- Data contracts and lakehouse governance: business promises sit above Unity Catalog, Delta Lake and ETL execution.
- HTAP and delivery boundaries: when to separate hot writes from heavy reads, and when to use the Spring Boot or Python PoC.

</details>

## Project Selection Guide

Use `oms-oltp-poc` when discussing live transactions, ACID consistency, high-concurrency inventory reservation, idempotency, Saga compensation and Outbox/Kafka handoff.

Use `inventory-oms-poc` when discussing how this OMS would look in a Java / Spring Boot enterprise service layout, especially DDD bounded contexts, aggregates, repositories, domain services and domain events.

Use `data-governance-poc` when discussing executable data contracts, data quality monitoring, data SRE, freshness checks, timestamp deviation, duplicate detection and reconciliation between OLTP facts and OLAP-ready history.

Use `oee-data-platform` for industrial equipment, mining, manufacturing or operations analytics discussions.

Use `cce-feature-platform` for customer data platform topics: Bronze -> Silver -> Gold modeling, identity resolution, feature serving and downstream campaign activation. For real-time CDC, Kafka, Redis and EKS sizing, see `cce-feature-platform/docs/REALTIME_FEATURE_PLATFORM_480K.md`. For Spark synthetic data, EMR/Delta, S3 layout and Airflow orchestration, see `cce-feature-platform/docs/BIG_DATA_EMR_DELTA_EXTENSION.md`. For the LLM/vector DB extension over existing MLOps, see `cce-feature-platform/docs/AI_VECTOR_DB_EXTENSION.md`.

Use [Z3934500/KB](https://github.com/Z3934500/KB) when discussing enterprise knowledge-base automation: lightweight S3 + Bedrock Knowledge Bases, medium RAG with explicit vector DB, fine-tuning and private runtime trade-offs, VPC design, and Jenkins/GitLab deployment options.

## Presenter Knowledge Cockpit

Use [`knowledge-cockpit`](knowledge-cockpit/README.md) as a live demo control panel for this repository. It provides an installable PWA shell, searchable terminology cards, prepared Q&A, a demo script, presenter notes, phone remote-control sessions, repo evidence links and AI knowledge-base answers through `server.py`.

Local run:

```powershell
python -m http.server 8088
```

Then open:

```text
http://localhost:8088/knowledge-cockpit/
```

Deployment steps are in [`knowledge-cockpit/DEPLOYMENT.md`](knowledge-cockpit/DEPLOYMENT.md).

All PoCs are intentionally compact. They prioritize runnable architecture and engineering patterns over pretending to be full production implementations.

