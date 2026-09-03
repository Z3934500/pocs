# ADR-005 Appendix: IO Bottleneck Analysis for Stream Processing

**Related**: [ADR-005: Two-Phase Autoscaling Strategy](ADR-005-two-phase-autoscaling-strategy.md)  
**Date**: 2026-09-03

## Context: High-IO vs High-TPS Workloads

The CCE platform stream job is fundamentally an **IO-bound workload**, not a CPU-bound or TPS-bound workload. This distinction is critical for choosing the right autoscaling metrics.

### Workload Classification

| Type | Bottleneck | Example | Right Metric for Scaling |
|------|------------|---------|--------------------------|
| **High TPS/QPS** | Request throughput | REST API, web servers | `http_requests_per_second`, CPU |
| **High Compute** | CPU cycles | Image processing, ML inference | CPU utilization, GPU utilization |
| **High IO** | Data movement | Stream processing, ETL, data pipelines | **Backlog size** (Kafka lag), bytes processed |

### Why Stream Job is IO-Bound

The CCE real-time stream job's workflow:

```
1. Read from Kafka (network IO)
   └─> Deserialize CDC events
2. Query RocksDB state store (disk IO)
   └─> Read existing feature values
3. Compute incremental updates (light CPU)
   └─> cart_items_last_hour++, intent_score recalc
4. Write to RocksDB (disk IO)
   └─> Update local state
5. Write to Redis (network IO)
   └─> Publish online features
6. Commit Kafka offset (network IO)
```

**IO operations dominate**: Steps 1, 2, 4, 5, 6 are all IO.  
**CPU computation is minimal**: Step 3 is simple arithmetic.

### The Problem with CPU-Based Scaling

If we scaled stream job on CPU utilization:

```
Scenario A: High lag, low CPU
- 1M messages waiting in Kafka (high lag)
- Stream job is blocked on Redis writes (IO wait)
- CPU usage: 20%
- ❌ CPU-based HPA would NOT scale up (disaster!)

Scenario B: Normal lag, high CPU
- 100 messages in Kafka (normal lag)
- Temporary CPU spike due to GC or deserialization burst
- CPU usage: 85%
- ❌ CPU-based HPA would scale up (wasteful!)
```

**Kafka lag directly measures the backlog** → It's the right signal.

## Databricks Batch Job IO Profile

Similarly, the Databricks Bronze → Silver → Gold pipeline is IO-intensive:

### Stage 1: Parquet Read + Filter

```python
# Example: Read Bronze orders
df_orders = spark.read.parquet("s3://cce-datalake/bronze/orders/")
df_filtered = df_orders.filter(col("event_date") >= "2026-09-01")
```

**Key metrics**:
- `input.bytesRead` — Total bytes scanned from S3/HDFS
- `input.recordsRead` — Total rows scanned
- `scan time` — Time spent reading from storage

**Bottleneck**: S3/HDFS throughput

**Optimization**:
- Partition pruning: `WHERE event_date >= '2026-09-01'` → only read relevant partitions
- Column pruning: `.select("customer_id", "amount")` → skip unused columns (Parquet columnar)
- Predicate pushdown: Push filters to storage layer before network transfer

**Monitoring**:
```sql
-- Databricks SQL job metrics
SELECT 
  job_id,
  stage_id,
  SUM(bytes_read) / 1024 / 1024 / 1024 AS gb_read,
  AVG(scan_time_ms) AS avg_scan_time_ms
FROM system.query.history
WHERE job_name = 'cce_medallion_job'
GROUP BY job_id, stage_id
```

### Stage 2: Shuffle Write and Read (Aggregation/Join)

```python
# Example: Aggregate customer lifetime value
df_agg = df_orders.groupBy("customer_id").agg(
    sum("amount").alias("lifetime_value"),
    count("*").alias("order_count")
)
```

**Key metrics**:
- `shuffle.write.bytesWritten` — Data written to disk for shuffle
- `shuffle.write.writeTime` — Time spent writing shuffle files
- `shuffle.read.bytesRead` — Data read from other executors
- `shuffle.read.fetchWaitTime` — Time waiting for remote shuffle blocks

**Bottleneck**: Network + local disk IO

**Optimization**:
- Reduce shuffle data volume:
  ```python
  # Bad: shuffle entire dataset
  df.repartition(100, "customer_id")
  
  # Good: filter first, then shuffle
  df.filter(col("amount") > 0).repartition(100, "customer_id")
  ```
- Increase `spark.sql.shuffle.partitions` to parallelize
- Use broadcast join for small dimension tables (< 10MB):
  ```python
  from pyspark.sql.functions import broadcast
  df_orders.join(broadcast(df_customers), "customer_id")
  ```
- Enable adaptive query execution (AQE):
  ```python
  spark.conf.set("spark.sql.adaptive.enabled", "true")
  spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
  ```

**Monitoring**:
```sql
-- Shuffle-heavy stages
SELECT 
  stage_id,
  SUM(shuffle_write_bytes) / 1024 / 1024 / 1024 AS shuffle_write_gb,
  SUM(shuffle_read_bytes) / 1024 / 1024 / 1024 AS shuffle_read_gb,
  AVG(shuffle_read_fetch_wait_time_ms) AS avg_fetch_wait_ms
FROM system.query.execution_metrics
WHERE job_name = 'cce_medallion_job'
  AND shuffle_write_bytes > 0
GROUP BY stage_id
ORDER BY shuffle_write_gb DESC
```

### Stage 3: In-Memory Computation

```python
# Example: Feature engineering (pure computation)
df_features = df_agg.withColumn(
    "customer_segment",
    when(col("lifetime_value") > 10000, "HIGH_VALUE")
    .when(col("lifetime_value") > 1000, "MEDIUM_VALUE")
    .otherwise("LOW_VALUE")
)
```

**Key metrics**:
- `task.executorRunTime` — Total task execution time
- `task.executorCpuTime` — CPU time (excludes IO wait)
- `task.jvmGCTime` — Time spent in garbage collection

**Bottleneck**: CPU and memory

**Optimization**:
- Increase executor memory if GC time > 10% of task time:
  ```python
  spark.conf.set("spark.executor.memory", "8g")
  spark.conf.set("spark.executor.memoryOverhead", "2g")
  ```
- Use `.cache()` for reused DataFrames:
  ```python
  df_orders.cache()  # If used multiple times
  df_orders.count()  # Trigger caching
  ```
- Avoid expensive UDFs; use native Spark functions:
  ```python
  # Bad: Python UDF (serialization overhead)
  @udf(returnType=StringType())
  def categorize(value):
      return "HIGH" if value > 1000 else "LOW"
  
  # Good: Native when/otherwise
  when(col("value") > 1000, "HIGH").otherwise("LOW")
  ```

**Monitoring**:
```sql
-- Tasks with high GC overhead
SELECT 
  stage_id,
  task_id,
  executor_run_time_ms,
  jvm_gc_time_ms,
  (jvm_gc_time_ms * 100.0 / executor_run_time_ms) AS gc_pct
FROM system.query.execution_metrics
WHERE jvm_gc_time_ms > 0
  AND (jvm_gc_time_ms * 100.0 / executor_run_time_ms) > 10
ORDER BY gc_pct DESC
LIMIT 20
```

## Applying IO Analysis to Stream Job Autoscaling

### Current Bottleneck Profile (Estimated)

Based on the stream job's workflow:

| Operation | Time % | Bottleneck | Scalable by Adding Pods? |
|-----------|--------|------------|--------------------------|
| Kafka read | 20% | Network, Kafka broker throughput | ✅ Yes (partition-level parallelism) |
| RocksDB read | 25% | Disk IOPS, EBS throughput | ✅ Yes (state is partitioned per pod) |
| Compute | 10% | CPU | ✅ Yes |
| RocksDB write | 25% | Disk IOPS, EBS throughput | ✅ Yes |
| Redis write | 15% | Network, Redis throughput | ⚠️ Limited (shared Redis) |
| Kafka commit | 5% | Network | ✅ Yes |

**Key insight**: Most operations benefit from horizontal scaling (adding pods), but Redis writes are a shared bottleneck.

### Why Kafka Lag is the Right Metric

Kafka lag measures the **backlog of unprocessed data**, which is:
- ✅ **Directly actionable**: High lag → need more processing capacity → scale up
- ✅ **IO-aware**: Lag increases when IO can't keep up, regardless of CPU
- ✅ **Business-aligned**: Lag directly impacts feature freshness (latency SLO)

Compare to CPU utilization:
- ❌ **Misleading in IO wait**: CPU idle during Redis/RocksDB blocking writes
- ❌ **Not business-aligned**: 80% CPU doesn't tell you if features are stale
- ❌ **Reactive, not predictive**: Only spikes after problem starts

### When Kafka Lag Doesn't Work

Scenario: Redis is the bottleneck (shared, can't scale horizontally with stream job)

```
Situation:
- Kafka lag: 50,000 (high)
- Stream job pods: 6 (max replicas)
- Redis CPU: 95% (saturated)
- Redis write latency: 100ms (normally 5ms)

Problem:
- Scaling stream job pods won't help
- More pods → more Redis writes → Redis even more saturated

Solution:
- Monitor Redis metrics: CPU, write latency, network throughput
- Add secondary autoscaling rule: if Redis CPU > 90%, don't scale stream job
- Or: scale Redis (add read replicas, use Redis Cluster)
```

This is why Phase 2 should also monitor Redis health:

```yaml
# Add to KEDA ScaledObject
triggers:
  - type: kafka
    metadata:
      lagThreshold: "5000"
  
  # Additional constraint: don't scale if Redis is saturated
  - type: prometheus
    metadata:
      serverAddress: http://prometheus:9090
      query: |
        (redis_cpu_usage{instance="cce-elasticache"} < 80) and
        (redis_command_duration_seconds{command="set"} < 0.01)
      threshold: "1"  # Must be true (1) to allow scaling
```

## Databricks Job Monitoring Strategy

Although the batch job is a CronJob (no autoscaling), we should monitor the same IO metrics to detect degradation:

### Monitoring Dashboard (Databricks SQL Analytics)

```sql
-- Daily job IO health check
CREATE OR REPLACE VIEW cce_batch_job_health AS
SELECT 
  job_run_date,
  
  -- Stage 1: Read efficiency
  SUM(CASE WHEN stage_name = 'BronzeRead' THEN bytes_read END) / 1024 / 1024 / 1024 AS stage1_gb_read,
  AVG(CASE WHEN stage_name = 'BronzeRead' THEN scan_time_ms END) AS stage1_avg_scan_ms,
  
  -- Stage 2: Shuffle overhead
  SUM(CASE WHEN stage_name = 'SilverAgg' THEN shuffle_write_bytes END) / 1024 / 1024 / 1024 AS stage2_shuffle_gb,
  AVG(CASE WHEN stage_name = 'SilverAgg' THEN shuffle_read_fetch_wait_time_ms END) AS stage2_fetch_wait_ms,
  
  -- Stage 3: Compute efficiency
  SUM(CASE WHEN stage_name = 'GoldCompute' THEN executor_run_time_ms END) AS stage3_runtime_ms,
  SUM(CASE WHEN stage_name = 'GoldCompute' THEN jvm_gc_time_ms END) AS stage3_gc_ms,
  (SUM(CASE WHEN stage_name = 'GoldCompute' THEN jvm_gc_time_ms END) * 100.0 / 
   NULLIF(SUM(CASE WHEN stage_name = 'GoldCompute' THEN executor_run_time_ms END), 0)) AS stage3_gc_pct,
  
  -- Overall
  MAX(job_duration_seconds) AS total_job_duration_sec
  
FROM system.query.history h
JOIN system.query.execution_metrics m ON h.query_id = m.query_id
WHERE h.job_name = 'cce_medallion_job'
  AND h.status = 'FINISHED'
GROUP BY job_run_date
ORDER BY job_run_date DESC
```

### Alert Thresholds

Set up alerts in Databricks or CloudWatch for:

```python
# Alert 1: Stage 1 read time increasing (partition explosion?)
if stage1_avg_scan_ms > 60000:  # 1 minute per task
    alert("Stage 1 read time excessive - check partition strategy")

# Alert 2: Shuffle volume growing (join explosion?)
if stage2_shuffle_gb > 100:  # 100 GB shuffle
    alert("Stage 2 shuffle volume high - check join selectivity")

# Alert 3: GC overhead high (memory pressure)
if stage3_gc_pct > 10:  # GC > 10% of runtime
    alert("Stage 3 GC pressure - increase executor memory")

# Alert 4: Overall job duration SLO breach
if total_job_duration_sec > 7200:  # 2 hours
    alert("Batch job exceeded 2-hour SLO - manual investigation required")
```

## Capacity Planning: IO-Centric Approach

### For Stream Job (Real-time)

Traditional approach (wrong):
```
# Calculate based on CPU
single_pod_throughput = benchmark_events_per_sec_at_80pct_cpu
required_pods = target_throughput / single_pod_throughput
```

**IO-centric approach** (right):
```python
# Benchmark each IO operation
kafka_read_throughput = 10_000 events/sec per partition
rocksdb_write_iops = 5_000 writes/sec per pod (limited by EBS)
redis_write_throughput = 50_000 writes/sec (shared, cluster-wide)

# Find the bottleneck
bottleneck_throughput = min(
    kafka_read_throughput * num_partitions,  # 10k * 6 = 60k
    rocksdb_write_iops * num_pods,           # 5k * X
    redis_write_throughput                   # 50k (shared!)
)

# Redis is the bottleneck at 50k events/sec
# So max useful pods = redis_throughput / rocksdb_iops_per_pod
#                     = 50,000 / 5,000 = 10 pods
# Beyond 10 pods, Redis saturates and lag increases despite more pods
```

**Action**: Before setting `maxReplicaCount: 10`, ensure Redis can handle 10 pods × 5k writes/sec.

### For Batch Job (Databricks)

Traditional approach (wrong):
```
# Just provision "enough" executors
spark.executor.instances = 50  # arbitrary
```

**IO-centric approach** (right):
```python
# Stage 1: Read throughput
input_data_size_gb = 500  # Bronze layer size
s3_read_throughput_gbps = 10  # AWS S3 limit per prefix
min_read_time_sec = input_data_size_gb / s3_read_throughput_gbps
# → 50 seconds minimum (IO bound)

# Stage 2: Shuffle throughput
shuffle_data_size_gb = 100  # After aggregation
network_bandwidth_gbps = 10  # Inter-executor network
min_shuffle_time_sec = shuffle_data_size_gb / network_bandwidth_gbps
# → 10 seconds minimum (IO bound)

# Stage 3: Compute time
# This is the only CPU-bound stage
# Size executors here: X cores, Y memory

# Total job time ≈ read + shuffle + compute
# If read + shuffle >> compute, adding more executors won't help Stage 1 & 2
# Better: optimize data layout (partition pruning, Z-order clustering)
```

**Action**: If job is slow, profile it first:
- If Stage 1 or 2 dominates → IO optimization (partitioning, clustering)
- If Stage 3 dominates → add executors or memory

## Summary: Metric Selection Decision Tree

```
Is your workload...

├─ Web API serving user requests?
│  └─> Scale on: http_requests_per_second, CPU
│
├─ Stream processing with backlog?
│  └─> Scale on: Kafka consumer lag, (secondary: Redis health)
│
├─ Batch ETL with IO-heavy stages?
│  └─> Monitor: bytes_read, shuffle_size, GC_time
│     (No autoscaling, but optimize IO before adding resources)
│
└─ ML inference or image processing?
   └─> Scale on: GPU utilization, CPU utilization
```

## References

- [Databricks Performance Tuning Guide](https://docs.databricks.com/optimizations/index.html)
- [Spark Shuffle Tuning](https://spark.apache.org/docs/latest/tuning.html#tuning-spark-shuffle)
- [Kafka Consumer Lag Monitoring](https://www.confluent.io/blog/kafka-lag-monitoring-and-metrics-at-appsflyer/)
- [RocksDB Performance Tuning](https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide)
- [Redis Latency Monitoring](https://redis.io/docs/management/optimization/latency/)

## Appendix: Benchmark Script for Stream Job

```python
# benchmark_stream_io.py
# Run this in a dev environment to measure actual IO throughput

import time
import redis
import kafka
from rocksdb import DB, Options

def benchmark_kafka_read(bootstrap_servers, topic, num_messages=10000):
    consumer = kafka.KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset='earliest'
    )
    
    start = time.time()
    count = 0
    for msg in consumer:
        count += 1
        if count >= num_messages:
            break
    duration = time.time() - start
    
    return num_messages / duration  # messages/sec

def benchmark_rocksdb_write(db_path, num_writes=10000):
    opts = Options()
    opts.create_if_missing = True
    db = DB(db_path, opts)
    
    start = time.time()
    for i in range(num_writes):
        db.put(f"key_{i}".encode(), f"value_{i}".encode())
    duration = time.time() - start
    
    return num_writes / duration  # writes/sec

def benchmark_redis_write(redis_url, num_writes=10000):
    r = redis.from_url(redis_url)
    
    start = time.time()
    pipe = r.pipeline()
    for i in range(num_writes):
        pipe.hset(f"cce:features:test:{i}", mapping={"feature_1": i})
    pipe.execute()
    duration = time.time() - start
    
    return num_writes / duration  # writes/sec

if __name__ == "__main__":
    print("Stream Job IO Benchmark")
    print("=" * 50)
    
    kafka_tps = benchmark_kafka_read("localhost:9092", "test-topic")
    print(f"Kafka read:     {kafka_tps:,.0f} msg/sec")
    
    rocks_tps = benchmark_rocksdb_write("/tmp/rocksdb_bench")
    print(f"RocksDB write:  {rocks_tps:,.0f} writes/sec")
    
    redis_tps = benchmark_redis_write("redis://localhost:6379")
    print(f"Redis write:    {redis_tps:,.0f} writes/sec")
    
    print("\nBottleneck analysis:")
    bottleneck = min(kafka_tps, rocks_tps, redis_tps)
    print(f"Limiting factor: {bottleneck:,.0f} events/sec")
    
    if bottleneck == kafka_tps:
        print("→ Kafka is the bottleneck (increase partitions)")
    elif bottleneck == rocks_tps:
        print("→ RocksDB is the bottleneck (upgrade EBS IOPS)")
    else:
        print("→ Redis is the bottleneck (scale Redis cluster)")
```

Run this before deploying Phase 2 to know your actual throughput limits!
