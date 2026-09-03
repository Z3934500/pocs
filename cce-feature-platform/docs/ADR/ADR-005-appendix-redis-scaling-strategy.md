# ADR-005 Appendix: Redis Scaling Strategy for Stream Job Growth

**Related**: [ADR-005: Two-Phase Autoscaling Strategy](ADR-005-two-phase-autoscaling-strategy.md)  
**Date**: 2026-09-03

## Problem Statement

When the Stream Job scales from 2 pods to 6 pods (3x increase), **Redis write throughput must also scale 3x**. Otherwise, Redis becomes the bottleneck and Kafka lag continues to grow despite adding more Stream pods.

Current architecture:
- Stream Job: StatefulSet, scales 2 → 6 pods
- Redis: ElastiCache, **single master** (cache.m6g.large) + 1 replica
- Write pattern: Each Stream pod writes to Redis master independently

**The bottleneck**: All writes go to one Redis master → horizontal scaling of Stream Job doesn't help if Redis saturates.

## Redis Scaling Options

### Option 1: Vertical Scaling (Upgrade Instance Type)

**Approach**: Increase the master node size as Stream Job scales.

| Stream Pods | Expected Writes/Sec | Recommended ElastiCache Instance |
|-------------|---------------------|----------------------------------|
| 2 (MVP) | ~10,000 | `cache.t4g.medium` (2 vCPU, 3.09 GB) |
| 4 | ~20,000 | `cache.m6g.large` (2 vCPU, 6.38 GB) |
| 6 | ~30,000 | `cache.m6g.xlarge` (4 vCPU, 12.93 GB) |
| 10+ | ~50,000+ | `cache.r6g.2xlarge` (8 vCPU, 26.32 GB) |

**Pros**:
- ✅ Simple: No application code changes
- ✅ Single endpoint: Stream Job connects to one Redis URL
- ✅ ElastiCache supports online instance type changes (minor downtime)

**Cons**:
- ❌ Limited ceiling: Single Redis instance maxes out at ~200K ops/sec (r6g.16xlarge)
- ❌ Cost: Larger instances are expensive (r6g.2xlarge ≈ $600/month)
- ❌ Single point of failure for writes (replica is read-only)

**When to use**: Stream Job scales to ≤10 pods (~50K writes/sec).

### Option 2: Redis Cluster (Horizontal Sharding)

**Approach**: Use ElastiCache Redis Cluster mode with 3-6 shards, each shard is a master+replica.

```
Stream Pod 0 ──┐
Stream Pod 1 ──┼──> Redis Cluster (client-side hashing)
Stream Pod 2 ──┘         ├─ Shard 0 (master + replica): keys hash 0-5460
                         ├─ Shard 1 (master + replica): keys hash 5461-10922
                         └─ Shard 2 (master + replica): keys hash 10923-16383
```

**Sharding strategy**:
- Keys: `cce:features:batch:{customer_id}` and `cce:features:realtime:{customer_id}`
- Hash on `{customer_id}` → each customer's features land on one shard
- Writes distribute across multiple masters → 3x write capacity

**Pros**:
- ✅ Horizontal write scaling: 3 shards = 3x write capacity
- ✅ High availability: Each shard has replica for failover
- ✅ Cost-effective at scale: 3x `cache.m6g.large` cheaper than 1x `cache.r6g.2xlarge`

**Cons**:
- ❌ Application changes required: Must use cluster-aware Redis client
- ❌ Limitations: No multi-key transactions across shards (MGET, MSET limited)
- ❌ Operational complexity: More endpoints to monitor

**When to use**: Stream Job scales beyond 10 pods (>50K writes/sec), or need HA for writes.

### Option 3: Redis Pipelining (Optimize Write Efficiency)

**Approach**: Batch multiple writes into one network round-trip.

**Current code** (inefficient):
```python
# Stream job writes one feature at a time
for customer_id in batch:
    redis.hset(f"cce:features:realtime:{customer_id}", mapping=features)
    # Each call = 1 network round-trip
```

**Optimized code** (pipeline):
```python
# Batch writes using pipeline
pipe = redis.pipeline(transaction=False)  # No MULTI/EXEC overhead
for customer_id in batch:
    pipe.hset(f"cce:features:realtime:{customer_id}", mapping=features)
pipe.execute()  # 1 network round-trip for entire batch
```

**Impact**:
- 10x fewer network round-trips
- Reduces Redis connection overhead
- Same Redis instance can handle 3-5x more writes/sec

**Pros**:
- ✅ Zero infrastructure changes
- ✅ Immediate improvement (code-only change)
- ✅ Works with single master or cluster

**Cons**:
- ❌ Doesn't increase Redis CPU/memory capacity (only network efficiency)
- ❌ If Redis CPU is saturated, pipelining won't help

**When to use**: Always use this, regardless of other options.

### Option 4: Write-Through Cache (Alternative Architecture)

**Approach**: Use a fast in-memory buffer (Redis) + durable store (DynamoDB or RDS).

```
Stream Job writes:
  1. Write to Redis (fast, ephemeral)
  2. Async write to DynamoDB (durable, slower)

API reads:
  1. Try Redis first (cache hit → return)
  2. If miss, read from DynamoDB → backfill Redis
```

**Pros**:
- ✅ Redis only caches hot features (working set)
- ✅ DynamoDB handles unlimited write throughput (pay-per-request)
- ✅ Durable: Features survive Redis restart

**Cons**:
- ❌ Major architecture change (adds DynamoDB, async writer, cache warming logic)
- ❌ Consistency complexity: Redis vs DynamoDB drift
- ❌ Cost: DynamoDB writes are expensive at high volume

**When to use**: Future phase (beyond MVP+1), if Redis becomes cost-prohibitive or durability is required.

---

## Recommended Strategy: Phased Approach

### Phase 1 (Current): Single Master + Vertical Scaling + Pipelining

**Redis config**:
```yaml
# ElastiCache parameter group
cluster-enabled: no  # Single master mode
maxmemory-policy: allkeys-lru  # Evict least-recently-used keys
timeout: 300  # Client idle timeout (seconds)
tcp-keepalive: 300
```

**Stream Job optimization** (implement now):
```python
# src/cce_platform/L2_olap/realtime.py
import redis

class StreamProcessor:
    def __init__(self, redis_url):
        self.redis = redis.from_url(
            redis_url,
            max_connections=50,  # Connection pool per pod
            socket_keepalive=True,
            socket_keepalive_options={
                socket.TCP_KEEPIDLE: 60,
                socket.TCP_KEEPINTVL: 10,
                socket.TCP_KEEPCNT: 3,
            }
        )
    
    def update_features(self, events: List[Event]):
        """Process a batch of events and write to Redis efficiently"""
        pipe = self.redis.pipeline(transaction=False)
        
        for event in events:
            customer_id = event.unified_customer_key
            features = self._compute_features(event)
            
            # Use pipeline for batching
            pipe.hset(
                f"cce:features:realtime:{customer_id}",
                mapping=features
            )
            
            # Optional: set TTL for ephemeral features
            pipe.expire(f"cce:features:realtime:{customer_id}", 86400)  # 24h
        
        # Execute all writes in one round-trip
        pipe.execute()
```

**Scaling plan**:
| Stream Pods | Action |
|-------------|--------|
| 2 → 4 pods | Monitor Redis CPU; if <70%, no action needed |
| 4 → 6 pods | Upgrade to `cache.m6g.xlarge` (4 vCPU) |
| 6 → 10 pods | Upgrade to `cache.r6g.2xlarge` (8 vCPU) or consider Phase 2 |

**Monitoring** (add to CloudWatch or Prometheus):
```yaml
# Redis metrics to watch
- EngineCPUUtilization: <70% (safe), >80% (upgrade needed)
- NetworkBytesIn/Out: Compare to instance limit
- CurrConnections: Should be Stream pods × 50 (connection pool size)
- CommandLatency (SET): <5ms (healthy), >10ms (saturated)
- Evictions: Should be 0 (if >0, increase maxmemory)
```

### Phase 2 (Future): Redis Cluster (If >10 Stream Pods)

**Trigger conditions**:
- Stream Job needs >10 pods (>50K writes/sec)
- Single Redis instance CPU >80% despite being `r6g.2xlarge`
- Cost optimization: 3x smaller instances cheaper than 1x huge instance

**ElastiCache Cluster config**:
```hcl
# Terraform example
resource "aws_elasticache_replication_group" "cce_redis_cluster" {
  replication_group_id       = "cce-feature-store-cluster"
  replication_group_description = "CCE Feature Store - Cluster Mode"
  
  engine                = "redis"
  engine_version        = "7.0"
  node_type             = "cache.m6g.large"
  
  # Cluster mode settings
  cluster_mode {
    num_node_groups         = 3  # 3 shards
    replicas_per_node_group = 1  # 1 replica per shard
  }
  
  # High availability
  automatic_failover_enabled = true
  multi_az_enabled          = true
  
  # Networking
  subnet_group_name = aws_elasticache_subnet_group.cce.name
  security_group_ids = [aws_security_group.redis.id]
  
  # Parameter group for cluster mode
  parameter_group_name = aws_elasticache_parameter_group.cluster_mode.name
  
  # Maintenance
  maintenance_window = "sun:05:00-sun:07:00"
  snapshot_window    = "03:00-05:00"
  snapshot_retention_limit = 5
}

resource "aws_elasticache_parameter_group" "cluster_mode" {
  family = "redis7"
  name   = "cce-redis-cluster-params"
  
  parameter {
    name  = "cluster-enabled"
    value = "yes"
  }
  
  parameter {
    name  = "maxmemory-policy"
    value = "allkeys-lru"
  }
}
```

**Stream Job changes** (cluster-aware client):
```python
# src/cce_platform/L2_olap/realtime.py
from rediscluster import RedisCluster

class StreamProcessor:
    def __init__(self, redis_cluster_endpoint):
        # Cluster-aware client
        self.redis = RedisCluster(
            host=redis_cluster_endpoint,
            port=6379,
            max_connections=50,
            max_connections_per_node=True,  # Pool per shard
            skip_full_coverage_check=True,  # Tolerate temp shard unavailability
            readonly_mode=False,  # We need write access
        )
    
    def update_features(self, events: List[Event]):
        # Same code as before! Pipeline works with cluster mode
        pipe = self.redis.pipeline(transaction=False)
        
        for event in events:
            customer_id = event.unified_customer_key
            features = self._compute_features(event)
            
            # Key uses hash tag to ensure sharding works
            # {customer_id} tells Redis to hash only this part
            pipe.hset(
                f"cce:features:realtime:{{{customer_id}}}",
                mapping=features
            )
        
        pipe.execute()  # Client routes each write to correct shard
```

**Migration steps** (zero downtime):
1. Create new Redis Cluster (parallel to existing)
2. Dual-write from Stream Job (write to both old and new Redis)
3. Backfill new Redis from old Redis (batch script)
4. Switch API reads to new Redis
5. Stop dual-writes, decommission old Redis

**Cost comparison** (ap-southeast-1 pricing):
```
Single master approach:
  1x cache.r6g.2xlarge (8 vCPU, 26 GB) = $0.848/hr = $616/month

Cluster approach:
  3 shards × (1 master + 1 replica) = 6 nodes
  6x cache.m6g.large (2 vCPU, 6.38 GB) = 6 × $0.182/hr = $1.092/hr = $793/month
  
Cluster is 29% more expensive BUT:
  - 3x write capacity (vs 1x)
  - Better HA (3 independent failover groups)
  - Room to grow (add more shards)
```

---

## Connection Pool Sizing

**Problem**: Each Stream pod opens connections to Redis. Too few → contention. Too many → Redis connection limit.

**Formula**:
```python
redis_max_connections = 65000  # ElastiCache default

# Per-pod connection pool
connections_per_pod = 50  # Start here

# Total connections = pods × connections_per_pod
# Example: 6 pods × 50 = 300 connections (well below limit)

# If using cluster mode with 3 shards:
# connections_per_shard = connections_per_pod / 3 ≈ 17 per pod per shard
# Total per shard = 6 pods × 17 = 102 connections per shard (safe)
```

**Tuning**:
- Monitor `CurrConnections` in CloudWatch
- If see connection timeouts → increase `max_connections` in client
- If Redis shows high connection churn → increase `socket_keepalive`

**Stream Job Kubernetes config**:
```yaml
# stream-statefulset.yaml
env:
  - name: REDIS_URL
    valueFrom:
      secretKeyRef:
        name: cce-feature-platform-secrets
        key: redis-url
  
  # Connection pool tuning
  - name: REDIS_MAX_CONNECTIONS
    value: "50"  # Per pod
  
  - name: REDIS_SOCKET_KEEPALIVE
    value: "true"
  
  - name: REDIS_SOCKET_TIMEOUT
    value: "5"  # seconds, fail fast if Redis slow
  
  - name: REDIS_RETRY_ON_TIMEOUT
    value: "true"
```

---

## Write Pattern Optimization

### Current Pattern (Suboptimal)

```python
# Each Kafka message triggers 1 Redis write immediately
def process_message(event):
    features = compute_features(event)
    redis.hset(f"cce:features:realtime:{event.customer_id}", mapping=features)
    kafka_consumer.commit()
```

**Problem**: High network overhead, Redis sees bursty traffic.

### Optimized Pattern: Micro-Batching

```python
def process_batch(kafka_batch: List[KafkaMessage]):
    """Process a batch of messages together"""
    
    # Step 1: Compute features for entire batch (CPU)
    updates = {}
    for msg in kafka_batch:
        customer_id = msg.value.customer_id
        features = compute_features(msg.value)
        
        # Merge updates for same customer (dedupe)
        if customer_id in updates:
            updates[customer_id].update(features)
        else:
            updates[customer_id] = features
    
    # Step 2: Write all updates in one pipeline (IO)
    pipe = redis.pipeline(transaction=False)
    for customer_id, features in updates.items():
        pipe.hset(f"cce:features:realtime:{customer_id}", mapping=features)
    pipe.execute()
    
    # Step 3: Commit Kafka offset once
    kafka_consumer.commit()

# Kafka consumer config
consumer = KafkaConsumer(
    'cce.rds.orders',
    max_poll_records=500,  # Fetch up to 500 messages per poll
    max_poll_interval_ms=300000,  # 5 minutes to process batch
)

for batch in consumer:
    process_batch(batch)
```

**Benefits**:
- Fewer Redis round-trips: 500 messages → 1 pipeline call (vs 500 individual calls)
- Deduplication: If same customer has multiple events, merge them
- Better throughput: Saturate Redis bandwidth, not latency

**Risks**:
- Longer Kafka commit interval → higher reprocessing on crash
- Mitigation: Keep batch size reasonable (500-1000 messages)

---

## Monitoring and Alerting

### Redis Metrics to Track

```yaml
# CloudWatch metrics (ElastiCache automatically provides these)
metrics:
  # CPU
  - EngineCPUUtilization
    threshold: >70% warning, >85% critical
    action: Consider vertical scaling
  
  # Memory
  - DatabaseMemoryUsagePercentage
    threshold: >80% warning, >90% critical
    action: Check evictions, increase maxmemory or upgrade instance
  
  # Network
  - NetworkBytesIn
  - NetworkBytesOut
    threshold: >80% of instance network limit
    action: Upgrade to instance with higher network cap
  
  # Latency
  - StringBasedCmdsLatency (SET, GET, HSET, HGET)
    threshold: p99 >10ms warning, >20ms critical
    action: Indicates saturation, consider scaling
  
  # Connections
  - CurrConnections
    threshold: >80% of max_connections (65000)
    action: Audit connection leaks, increase connection pool efficiency
  
  # Replication
  - ReplicationLag (if using replica)
    threshold: >5 seconds
    action: Check network or master load
```

### Derived Metrics (Calculate in Prometheus/CloudWatch Insights)

```promql
# Redis write throughput (ops/sec)
rate(redis_commands_processed_total{cmd="hset"}[1m])

# Redis write saturation (%)
(redis_instantaneous_ops_per_sec / redis_max_ops_per_sec) * 100

# Connection pool utilization per Stream pod
(redis_connected_clients / (stream_job_pods * redis_max_connections_per_pod)) * 100
```

### Alert Rules

```yaml
# Example: Prometheus AlertManager rules
groups:
  - name: redis-alerts
    rules:
      - alert: RedisCPUHigh
        expr: elasticache_cpu_utilization > 80
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Redis CPU >80% for 5 minutes"
          description: "Consider upgrading instance type or enabling cluster mode"
      
      - alert: RedisWriteLatencyHigh
        expr: elasticache_command_latency{command="hset"} > 0.010  # 10ms
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Redis HSET latency >10ms"
          description: "Redis may be saturated. Check CPU, network, and memory."
      
      - alert: RedisConnectionPoolExhaustion
        expr: redis_connected_clients > 60000
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "Redis approaching connection limit (65000)"
          description: "Audit connection leaks or reduce connections_per_pod"
```

---

## Decision Tree: When to Scale Redis

```
Is Stream Job scaling from 2 pods to 4+ pods?
│
├─ YES: Check Redis metrics
│   │
│   ├─ Redis CPU <70%?
│   │   └─> No action needed (current instance is sufficient)
│   │
│   ├─ Redis CPU 70-85%?
│   │   └─> Implement pipelining (code change)
│   │       └─> If CPU still high → vertical scale (upgrade instance type)
│   │
│   └─ Redis CPU >85%?
│       └─> Urgent: Vertical scale immediately
│           └─> If reaching r6g.2xlarge limit → plan cluster mode migration
│
└─ NO: Monitor only
```

**Phase transition thresholds**:
| Redis CPU | Action |
|-----------|--------|
| <60% | No action, current config sufficient |
| 60-70% | Implement pipelining (quick win) |
| 70-80% | Vertical scale to next instance type |
| 80-90% | Urgent: Vertical scale + consider cluster mode planning |
| >90% | Critical: Immediate intervention required, begin cluster migration |

---

## Summary

### Immediate Actions (Phase 1)
1. **Implement pipelining** in Stream Job (code change, zero infrastructure cost)
2. **Monitor Redis metrics** (CPU, latency, connections) in CloudWatch/Prometheus
3. **Set up alerts** for CPU >70%, latency >10ms

### Short-Term Actions (When Stream Job >4 pods)
1. **Vertical scale Redis**: cache.m6g.large → cache.m6g.xlarge or cache.r6g.xlarge
2. **Tune connection pools**: Adjust `max_connections` based on pod count
3. **Optimize batch size**: Tune Kafka `max_poll_records` for micro-batching efficiency

### Long-Term Actions (When Stream Job >10 pods or Redis CPU >80% on large instance)
1. **Migrate to Redis Cluster**: 3-6 shards for horizontal write scaling
2. **Update Stream Job**: Use cluster-aware Redis client
3. **Load test**: Verify 3x write capacity before cutover

### Optional (Future Phases)
- **Write-through cache with DynamoDB**: For durability and unlimited scale (major architecture change)
- **Read replicas**: If API read load grows (currently API reads from same Redis)

---

## References

- [ElastiCache Redis Best Practices](https://docs.aws.amazon.com/AmazonElastiCache/latest/red-ug/BestPractices.html)
- [Redis Pipelining](https://redis.io/docs/manual/pipelining/)
- [Redis Cluster Tutorial](https://redis.io/docs/management/scaling/)
- [redis-py-cluster Documentation](https://redis-py-cluster.readthedocs.io/)
- [ElastiCache for Redis Cluster Mode](https://docs.aws.amazon.com/AmazonElastiCache/latest/red-ug/Replication.Redis-RedisCluster.html)
