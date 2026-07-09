# Operations, Maturity And Cost Notes

This note describes the gap between the target CCE architecture and a less mature real-world environment. The real-time and big-data designs are useful target states, but production rollout depends on source-system quality, ownership, data contracts, observability and cost controls.

## Target State vs Reality

| Area | Target state in this repo | Common real-world constraint | Practical response |
| --- | --- | --- | --- |
| OLTP / OLAP boundary | Transactional systems emit CDC or Outbox events; analytical jobs consume history | API logic, batch jobs and reporting tables may be tightly coupled | Add an anti-corruption layer and publish stable event/data contracts before rebuilding everything |
| Unique keys | Events use `event_id` or version-aware keys | Pipelines may still rely on source PK or composite PK even when records represent versions over time | Use `event_id`, `source_updated_at`, `source_sequence`, `business_date` and `record_hash` for replay-safe processing |
| Field semantics | Fields such as customer segment, discount, product type and policy status have clear meaning | Source fields can be overloaded or semantically wrong; premium-financing fields may not mean what downstream teams assume | Define semantic contracts, add validation checks and keep rejected rows with reasons |
| Source capability | Source system can expose the event or calculation needed downstream | OLTP database may not support a discount rule, historical version or calculation required by analytics | Move derived analytical logic into Silver/Gold or a service layer; avoid forcing analytical semantics into OLTP tables |
| Batch / online split | Big-data jobs build baseline features; stream jobs apply low-latency increments | Batch refresh, API lookup and operational writes may share the same database or tables | Separate write path, offline feature path and online serving path incrementally |
| Data drift | Timestamp deviation, volume changes and schema drift are monitored | Source payloads may shift without notification; late and out-of-order data may be normal | Add schema, freshness, volume, timestamp and reconciliation checks after each critical job |
| Ownership | Each dataset and service has a named owner and SLA | Ownership may be split across application, data, vendor and operations teams | Track owner, consumer, SLA, alert route and recovery runbook for each critical dataset |

## Real-Time And Big-Data Relationship

The two paths are complementary:

```text
Big-data path
  S3 / Delta / Spark / Databricks / EMR / Airflow
  -> trusted historical baseline features, segments, scores and anomaly outputs

Real-time path
  RDS CDC / Debezium / MSK / stream job / Redis
  -> low-latency incremental feature updates

Serving path
  Redis / Feature API
  -> combined feature view for campaign decisioning
```

The big-data path is optimized for correctness, replay and full-history computation. The real-time path is optimized for latency, ordering and incremental updates. Redis and the Feature API are the convergence point.

## Operations Dimensions

Operational capacity is not only CPU. For this feature platform, the main dimensions are compute, memory, network and storage.

| Dimension | Where it appears | Failure signal | Useful metric |
| --- | --- | --- | --- |
| Compute | Spark executors, API pods, stream workers | Slow jobs, CPU throttling, high p95 latency | CPU utilization, throttling, Spark task time |
| Memory | Redis, RocksDB state, Spark executor memory | OOM, Redis evictions, executor failures | Redis used memory, evictions, JVM memory, pod restarts |
| Network | CDC to MSK, stream job to Redis, API to Redis | Lag, timeout, retries, packet drops | Kafka bytes in/out, consumer lag, p95 network latency, retry count |
| Storage | S3/Delta, MSK EBS, RocksDB PVC, logs | Small-file slowdown, I/O wait, full disk | S3 request rate, file count, EBS IOPS, PVC usage, compaction backlog |

## Layered Troubleshooting

Do not collapse every database problem into "high concurrency". Request rate, data volume, query shape and downstream lag are different failure modes.

| Question | What it means | Typical fix |
| --- | --- | --- |
| Is QPS high? | Many requests arrive at the same time | Cache hot reads, scale API pods, tune connection pools, add replicas or shard OLTP writes |
| Is one request slow? | A single query scans too much data or waits on IO | Add indexes, choose the right shard key, prune partitions, compact files or reduce shuffle |
| Are connections piling up? | Slow requests occupy DB or service connections until queues form | Fix the slow query first, then tune pool sizes and timeouts |
| Is the pipeline late? | CDC, Kafka, Spark or Airflow is not keeping up | Check consumer lag, broker throughput, Spark stages, retries and data-quality failures |

Partitioning also means different things at different layers:

| Layer | Mechanism | Main purpose | Typical key |
| --- | --- | --- | --- |
| OLTP physical scaling | Sharding or table-splitting | Spread writes, point lookups and storage across databases or tables | Stable routing key such as `customer_id`, `order_id` or phone hash |
| OLTP local pruning | Database table partition | Reduce scan range inside one table or one database | Range or hash key that matches the transactional query |
| Kafka / MSK | Topic partitions | Preserve per-key order and increase consumer parallelism | `unified_customer_key`, order ID or another event ordering key |
| OLAP logical layout | Lakehouse or warehouse partition | Prune historical scans, control replay scope and reduce batch cost | `business_date`, `ingestion_date` or event date |
| Delta / S3 file layout | File sizing, compaction and clustering | Avoid small files, reduce scanned bytes and speed up commits | Target file size plus clustering keys such as customer, policy or date |

These choices are related, but they are not interchangeable. An OLTP shard key is a request-routing rule; an OLAP partition key is an analytical scan and replay rule.

## IO Impact And Troubleshooting

IO often decides whether a large feature pipeline finishes in minutes or hours. CPU-heavy Spark code still waits when source disks, shuffle spill, Kafka broker storage or S3 object operations become the bottleneck.

Illustrative 1 TB sequential read example:

| Storage type | Approx sequential read throughput | Approx time for 1 TB | Random IO profile |
| --- | ---: | ---: | --- |
| HDD | 100 MB/s | ~2.8 hours | ~100 IOPS |
| Enterprise SSD | 500 MB/s | ~34 minutes | ~50K IOPS |
| NVMe SSD | 3 GB/s | ~5.7 minutes | ~500K IOPS |
| S3 Standard | Depends on object size, prefix layout and parallelism | Depends on request pattern | Watch request rate, throttling and 5xx errors |

The exact numbers vary by cloud, instance type, file size, parallelism and storage configuration. The design point is stable: a pipeline that looks compute-bound can actually be blocked by read/write throughput, random IO, small files or request throttling.

Feature-platform IO chain:

| Stage | IO pattern | Bottleneck signal | What to watch |
| --- | --- | --- | --- |
| CDC source | Many small transactions and binlog reads | Source lag, disk wait, replication delay | `iostat -x 1`, read/write IOPS, DB replication lag |
| Kafka / MSK | Sequential append to broker logs | Broker disk utilization, under-replicated partitions, producer latency | broker bytes in/out, disk throughput, average write size, consumer lag |
| Spark shuffle | Stage-to-stage spill, read and write | Long shuffle stages, executor spill, skewed partitions | Spark UI shuffle read/write, spill bytes, task skew, executor disk usage |
| Delta Lake / S3 | Parquet writes plus transaction log updates | Small-file buildup, slow commits, S3 throttling | file count, average file size, commit duration, S3 5xx/throttling metrics |
| Feature API | Mostly memory and network reads | Redis latency, API p95, network timeout | Redis p95 latency, cache hit rate, API p95, network retries |

Troubleshooting order:

1. Check whether the job is compute-bound or waiting on IO: CPU utilization, executor wait time, disk wait and shuffle spill.
2. Check partitioning and file layout: too many tiny files can make S3/Delta slow even when total data volume is moderate.
3. Check shuffle size and skew: one hot customer, product or date partition can dominate the batch window.
4. Check broker and source lag: CDC may be delayed before Spark sees the data.
5. Check serving path separately: Redis and Feature API latency are usually memory/network issues, not disk IO issues.

## Cost Model

The cost model has fixed and variable parts.

| Cost area | Fixed or variable | Driver | Control lever |
| --- | --- | --- | --- |
| EKS control plane / base nodes | Mostly fixed | Cluster count, baseline node groups | Reuse clusters, right-size node groups, use namespaces and quotas |
| Feature API | Variable | QPS, p95 latency target, min/max replicas | HPA thresholds, requests/limits, cache hit rate |
| Redis / ElastiCache | Mostly fixed per node | Active users, feature size per user, replication, memory fragmentation | Store hot features only, compress payloads, TTL stale keys, monitor evictions |
| MSK | Mostly fixed per broker | Broker count, EBS size, retention, partitions | Use right-sized broker count, retention policies, topic partition planning |
| Stream job | Variable | Event rate, state size, checkpoint/rebalance cost | Partition by customer key, tune RocksDB state, monitor lag |
| Databricks / EMR | Variable | Input size, shuffle, job duration, executor size | Partition pruning, compaction, Spot/task nodes, scheduled jobs, incremental processing |
| S3 / Delta | Variable | Raw data, table versions, retention, small files | Lifecycle policies, vacuum/retention, compaction, partition strategy |
| Observability | Variable | Metric cardinality, log volume, trace sampling | Sampling, log retention, dashboard cardinality control |
| MWAA / Airflow | Mostly fixed | Environment size, DAG count, scheduler load | Keep DAGs simple, use task retries/SLA, avoid excessive polling |

For a PoC, cost can stay low by keeping data volume small, running jobs on demand and using a minimal Redis/MSK shape. For production, the cost conversation should include HA, retention, observability, backup, security and operational labor.

## Maturity Roadmap

| Stage | State | Risk | Next step |
| --- | --- | --- | --- |
| 0. Coupled reporting | OLTP tables, APIs and batch reports share source schemas directly | Analytics changes can break operations; source semantics are unclear | Inventory critical tables, reports and data consumers |
| 1. Contracted ingestion | CDC/Outbox or raw landing captures source changes with schema checks | Raw data exists but semantics may still be ambiguous | Add data contracts, semantic checks and rejected-row handling |
| 2. Separated feature paths | Big-data jobs build offline features; stream jobs update online features | Dual-write and replay behavior must be controlled | Add idempotency keys, reconciliation and backfill runbooks |
| 3. Operated platform | GitOps, observability, alert routing, cost controls and SLA reviews exist | Platform overhead can grow | Track SLOs, cost per workload, error budget and ownership |

## Design Boundary

The architecture in this repository is a target operating model. It assumes that source systems can emit usable changes, ownership can be assigned, and batch/online responsibilities can be separated over time. In a less mature environment, the first priority is not to deploy every component. The first priority is to reduce coupling, define data semantics, monitor data quality and introduce reliable handoff points between OLTP, big-data processing and online serving.