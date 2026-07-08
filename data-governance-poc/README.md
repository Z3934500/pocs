# Data Governance PoC

Executable data contract and data observability checks for the OMS event stream.

This project makes the "data contract" idea concrete. Instead of treating it as a document only, it runs production-style data SRE checks against the OMS OLTP database and its Outbox events.

## What This Shows

- Schema drift detection for key source tables
- Event payload contract checks for required fields
- Freshness monitoring for the latest Outbox event
- Pending-event lag monitoring for stalled publishers
- Publish-delay monitoring using `created_at` vs `published_at`
- Duplicate semantic event detection
- Timestamp parsing and deviation checks
- Inventory reconciliation from movement facts back to stock state
- JSON and Prometheus-style output for dashboards or alerts

## How It Fits

```text
OMS OLTP
  orders / inventory / outbox_events
        |
        v
Data Governance PoC
  schema checks
  event contract checks
  freshness and lag checks
  reconciliation checks
        |
        v
Alerts / dashboards / Airflow SLA / data quality gates
        |
        v
OLAP tables, dashboards, features and ML models
```

In a larger platform, the same ideas would run in Airflow, Spark, Flink, dbt, Great Expectations, Deequ, Delta Live Tables expectations, ClickHouse/Doris SQL checks, or a dedicated observability tool. The important part is the operating model: data contracts become executable checks with alerts, not just static documentation.

## Local Run

From this directory:

```powershell
python -m pip install -e .
python -m data_governance.demo
```

To check an existing OMS database:

```powershell
python -m data_governance.monitor `
  --db ..\oms-oltp-poc\data\oms_oltp.sqlite `
  --contract contracts\oms_event_contract.json
```

JSON output:

```powershell
python -m data_governance.monitor `
  --db ..\oms-oltp-poc\data\oms_oltp.sqlite `
  --contract contracts\oms_event_contract.json `
  --format json
```

Prometheus-style output:

```powershell
python -m data_governance.monitor `
  --db ..\oms-oltp-poc\data\oms_oltp.sqlite `
  --contract contracts\oms_event_contract.json `
  --format prometheus
```

## Checks

| Check | What it catches | Why it matters |
| --- | --- | --- |
| Schema contract | Missing columns in source tables | Prevents silent downstream breakage |
| Event payload contract | Missing required payload fields | Keeps Bronze/Silver ingestion stable |
| Unknown event type | New event emitted without contract update | Forces producer-consumer alignment |
| Freshness | No recent events | Detects stopped ingestion or source outage |
| Pending lag | Outbox events stuck in `PENDING` | Detects Kafka/CDC publisher delays |
| Publish delay | Large `published_at - created_at` gap | Detects delayed handoff to downstream systems |
| Duplicate semantic event | Same event meaning appears multiple times | Protects OLAP idempotency |
| Timestamp validity | Bad timestamp format | Protects event-time windows and SCD logic |
| Clock skew | Event timestamps unexpectedly in the future | Detects source clock drift or wrong timestamp normalization |
| Inventory reconciliation | Movement facts do not match stock state | Catches data drift between operational state and analytical facts |

## Why This Is Data Governance

Governance is not only catalog ownership or policy text. In day-to-day engineering, governance also means:

- the data has a known owner;
- the schema is stable or evolves intentionally;
- timestamps have clear semantics;
- freshness is monitored;
- duplicates and late data are handled deterministically;
- source-of-record totals reconcile with derived facts;
- downstream tables and ML features can be replayed safely.

This is the practical bridge from OLTP to OLAP: the OMS does the business work, and data governance makes sure the history is trustworthy enough for dashboards, feature engineering, anomaly detection, recommendation systems and risk models.