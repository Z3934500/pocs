# ADR-005: Two-Phase Autoscaling Strategy for Stream Processing

**Status**: Accepted  
**Date**: 2026-09-03  
**Decision Makers**: Architecture team

## Context

The CCE platform has two processing paths that converge in Redis:

1. **Batch path (OLAP)**: Databricks Bronze → Silver → Gold, synced daily via CronJob at 02:10
2. **Real-time path (Stream)**: Debezium CDC → MSK Kafka → Stream StatefulSet → Redis

As the platform scales from MVP (20K users, 1.44M events/day = 16.7 events/sec) to promotion stage (480K users), and potentially beyond, we need an autoscaling strategy for the stream processing workload.

Key constraints:
- MVP data volume: ~16.7 events/sec (low, predictable)
- Future scale: 5M+ events/day at peak
- Stream job uses StatefulSet with RocksDB persistent state (20Gi EBS per pod)
- Kafka topics partitioned by `unified_customer_key` (6 partitions initially)
- Early-stage operational simplicity is critical

## Decision

**Adopt a two-phase autoscaling strategy:**

### Phase 1: Fixed Replicas (MVP → Early Production)
- Stream StatefulSet: **fixed 2 replicas**
- API HPA: scale on **CPU (60%) and http_requests_per_second (100 RPS)**
- No automatic scaling for stream job
- Manual intervention if consumer lag exceeds threshold

**Trigger for Phase 1 → 2 transition:**
- Daily event volume > 10M/day, OR
- Consumer lag frequently > 10,000, OR
- Manual scaling interventions > 1/week, OR
- At least 1 month of monitoring data collected

### Phase 2: Kafka Lag-Based Autoscaling (High Scale)
- Stream StatefulSet: scale on **Kafka consumer lag** (2-12 replicas)
- API HPA: unchanged (CPU + http_requests_per_second)
- Use KEDA ScaledObject for Kafka trigger
- Max replicas = Kafka partition count (over-provisioning is wasted)

## Rationale

### Why Phase 1 uses fixed replicas

1. **Operational simplicity**: No external metrics dependencies (Prometheus, KEDA) needed at MVP
2. **Low data volume**: 16.7 events/sec is easily handled by 2 pods
3. **Avoid premature optimization**: HPA for StatefulSet requires K8s 1.23+ and adds complexity
4. **State management**: StatefulSet scaling triggers RocksDB state redistribution (expensive)

### Why Phase 2 uses Kafka lag

1. **Direct signal**: Consumer lag directly measures stream job backlog
2. **Correlation at scale**: High event volume → high lag → likely high API load too
3. **Capacity formula**:
   ```
   partitions = ceil(target_throughput / single_partition_throughput)
   max_replicas = partitions  # each pod consumes 1+ partitions
   lag_threshold = single_pod_throughput × desired_recovery_time_seconds
   ```

4. **Cost efficiency**: Scale down during off-peak hours

### Why API HPA is separate from Stream HPA

API load and Kafka lag are **indirectly correlated**, not directly:
- API serves downstream queries (AJO/CDP/POS) → driven by campaign execution, not event ingestion
- Stream job consumes CDC events → driven by transactional activity (orders, cart events)
- Both increase during business peaks, but asynchronously

Therefore:
- **API scales on its own load** (http_requests_per_second, CPU)
- **Stream scales on its own backlog** (kafka_consumer_lag)

## Implementation

### Phase 1 (Current)

```yaml
# 02_extensions/01_realtime/.../stream-statefulset.yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: cce-realtime-feature-stream
spec:
  replicas: 2  # Fixed, no HPA
  # ...
```

**Monitoring (even without auto-scaling):**
- CloudWatch or Prometheus metrics:
  - `kafka_consumer_lag{topic="cce.rds.orders"}`
  - `kafka_consumer_lag{topic="cce.rds.cart_events"}`
  - `rocksdb_state_size_bytes`
  - Stream pod CPU/memory usage

**Manual scaling runbook:**
```bash
# If lag > 10,000 for >5 minutes
kubectl scale statefulset cce-realtime-feature-stream --replicas=4 -n cce-platform

# Record in incident log: peak throughput, lag before/after, pod count
```

### Phase 2 (Future)

See [ADR-005-keda-kafka-scaler.yaml](#keda-config) for full KEDA configuration.

Key parameters:
- `lagThreshold: "5000"` — scale up if avg lag per pod > 5000 events
- `minReplicaCount: 2` — always keep 2 pods for availability
- `maxReplicaCount: 6` — match initial Kafka partition count
- `cooldownPeriod: 300` — wait 5 minutes before scaling down (RocksDB state redistribution is expensive)

## Consequences

### Positive

1. **Phase 1 simplicity**: No KEDA installation, no Prometheus operator, fewer moving parts at MVP
2. **Gradual complexity**: Only adopt Kafka lag scaling when data justifies it
3. **Cost optimization**: Fixed replicas at low scale, auto-scaling at high scale
4. **Independent scaling**: API and Stream scale on their respective signals
5. **Measurable transition**: Clear criteria for Phase 1 → 2 (event volume, lag, manual intervention frequency)

### Negative

1. **Manual work in Phase 1**: Ops must monitor lag and scale manually if needed
2. **Delayed response**: Phase 1 cannot auto-scale during unexpected traffic spikes
3. **Over-provisioning risk**: Fixed 2 replicas may be idle during off-peak hours
4. **State redistribution cost**: StatefulSet scaling in Phase 2 triggers RocksDB rebalancing (slow)

### Mitigation

- **For manual work**: Set up CloudWatch alarms on lag > 10,000 → page on-call
- **For delayed response**: Size Phase 1 replicas to handle 2-3x normal load
- **For over-provisioning**: Accept the cost (2 pods are cheap compared to operational complexity)
- **For state redistribution**: Use `cooldownPeriod: 300` to avoid flapping; consider [incremental cooperative rebalancing](https://kafka.apache.org/documentation/#consumerconfigs_partition.assignment.strategy)

## Data Consistency: Feature Separation as SSOT Strategy

This ADR also clarifies the **data consistency model** between batch and real-time paths:

### Design Principle: Feature Namespace Separation

Batch and real-time paths update **different feature types**, avoiding conflicts:

| Path | Features | Update Frequency | Authority |
|------|----------|------------------|-----------|
| **Batch (OLAP)** | `lifetime_value`, `segment`, `risk_score`, `avg_order_value_90d` | T+1 (daily at 02:10) | Gold table = SSOT |
| **Real-time (Stream)** | `cart_items_last_hour`, `intent_score`, `last_order_timestamp`, `recent_activity_count_24h` | Seconds | CDC events |

**Key insight**: These feature types are **naturally disjoint**:
- Batch handles historical aggregates requiring full dataset access
- Real-time handles short-window metrics from recent events
- No feature is updated by both paths

### Gold Table as Single Source of Truth (SSOT)

- If historical data needs correction → fix Gold table → next batch sync overwrites Redis
- Real-time features are ephemeral (24h-72h window) and not replayed from Gold
- Disputes resolve to Gold (e.g., if real-time `last_order_timestamp` diverges from batch `last_order_date`, batch wins on next sync)

### Redis Key Naming (Recommended)

Although features are disjoint by type, explicit namespace separation improves clarity:

```python
# Batch importer writes:
redis.hset(f"cce:features:batch:{customer_id}", mapping={
    "lifetime_value": 15000,
    "segment": "HIGH_VALUE",
    # ...
})

# Stream job writes:
redis.hset(f"cce:features:realtime:{customer_id}", mapping={
    "cart_items_last_hour": 3,
    "intent_score": 0.87,
    # ...
})

# API reads both:
batch_features = redis.hgetall(f"cce:features:batch:{customer_id}")
realtime_features = redis.hgetall(f"cce:features:realtime:{customer_id}")
return {**batch_features, **realtime_features}
```

### No Reconciliation Needed (Currently)

Since feature types don't overlap:
- No conflict resolution logic required
- No timestamp comparison needed
- No "last write wins" semantics needed

**If future requirements introduce overlapping features** (e.g., both paths update `last_order_timestamp`), we will need:
- Timestamp-based conflict resolution, OR
- Designate one path as authoritative per feature

## Alternatives Considered

### Alternative 1: Kafka Lag-Based HPA from Day 1
**Rejected** because:
- Adds operational complexity (KEDA, Prometheus, external metrics) at MVP
- MVP data volume (16.7 events/sec) doesn't justify it
- Violates "simplest thing that works" principle

### Alternative 2: CPU-Based HPA for Stream Job
**Rejected** because:
- CPU doesn't reflect backlog (pod can be idle with high lag, or busy with low lag)
- Kafka consumer lag is the direct signal of stream job health
- CPU-based scaling is reactive (lag is already high), lag-based is proactive

### Alternative 3: Single HPA for Both API and Stream
**Rejected** because:
- API and Stream have different scaling triggers (HTTP requests vs Kafka lag)
- Coupling them would cause incorrect scaling behavior
- StatefulSet and Deployment have different scaling characteristics

### Alternative 4: Use Deployment Instead of StatefulSet for Stream Job
**Rejected** because:
- RocksDB state store requires persistent volume per pod (StatefulSet's volumeClaimTemplates)
- Deployment doesn't guarantee stable pod identity needed for Kafka partition assignment
- State redistribution on scale-down is safer with StatefulSet's ordered termination

## Verification

### Phase 1 Readiness Checklist
- [x] Stream StatefulSet deployed with replicas: 2
- [x] API HPA configured with CPU + http_requests_per_second
- [ ] CloudWatch/Prometheus metrics configured for consumer lag
- [ ] Alerting rule: lag > 10,000 for >5min → page on-call
- [ ] Manual scaling runbook documented

### Phase 2 Readiness Checklist
- [ ] Daily event volume > 10M/day sustained for 1 week, OR manual scaling >1/week
- [ ] KEDA operator installed in EKS cluster
- [ ] Kafka Exporter or CloudWatch Container Insights enabled
- [ ] Prometheus Adapter configured (if using Prometheus)
- [ ] KEDA ScaledObject tested in staging environment
- [ ] RocksDB state redistribution time measured (target: <5min for 1 partition)
- [ ] Kafka partition count reviewed (may need increase if >6 pods required)

### Metrics to Collect (Phase 1 → 2 Transition Decision)

Track these for 1 month before Phase 2:
- Daily peak consumer lag
- Daily peak event throughput (events/sec)
- Number of manual scaling interventions
- RocksDB state size growth rate
- Stream pod CPU/memory peak usage
- API request rate correlation with event rate

## References

- [REALTIME_FEATURE_PLATFORM_480K.md](../../02_extensions/01_realtime/docs/REALTIME_FEATURE_PLATFORM_480K.md) — Sizing and architecture
- [stream-statefulset.yaml](../../02_extensions/01_realtime/after_mvp2_dev_stage_prod/deploy/k8s/stream-statefulset.yaml) — Phase 1 config
- [ADR-005-keda-kafka-scaler.yaml](ADR-005-keda-kafka-scaler.yaml) — Phase 2 config (this ADR)
- [ADR-001: OLAP-OLTP Siblings](ADR-001-olap-oltp-siblings.md) — Feature separation principle
- KEDA Kafka Scaler: https://keda.sh/docs/latest/scalers/apache-kafka/
- Kafka Consumer Lag Monitoring: https://docs.confluent.io/platform/current/kafka/monitoring.html
