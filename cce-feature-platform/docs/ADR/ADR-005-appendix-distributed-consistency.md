# ADR-005 Appendix: Distributed Data Consistency Analysis

**Related**: [ADR-005: Two-Phase Autoscaling Strategy](ADR-005-two-phase-autoscaling-strategy.md)  
**Date**: 2026-09-03

## Problem Statement

The CCE platform has multiple sources of truth writing to Redis concurrently:
1. **Batch Job** (CronJob, daily at 02:10): Writes Gold features from Databricks
2. **Stream Job** (6 pods, real-time): Writes CDC-derived features from Kafka
3. **API** (read-only, but caches): Reads from Redis

Additionally, when using **Redis Cluster**, data is sharded across multiple nodes, introducing distributed system consistency challenges.

**Key question**: What data consistency issues can occur, and how do we prevent them?

---

## Consistency Issue #1: Race Condition Between Stream Job Pods

### Scenario: Multiple Pods Processing Same Customer

**Problem**: Two Stream Job pods process events for the same customer concurrently.

```
Timeline (same customer ID: U0001):

t0: Kafka partition 0 has event: "U0001 added item to cart"
t1: Kafka partition 3 has event: "U0001 viewed product"

t2: Pod A (consuming partition 0) reads Redis:
    cart_items_last_hour = 5

t3: Pod B (consuming partition 3) reads Redis:
    cart_items_last_hour = 5  (still 5, Pod A hasn't written yet)

t4: Pod A computes: cart_items_last_hour = 6 (5+1)
    Pod A writes to Redis: HSET cce:features:realtime:U0001 cart_items_last_hour 6

t5: Pod B computes: cart_items_last_hour = 6 (5+1, based on stale read)
    Pod B writes to Redis: HSET cce:features:realtime:U0001 cart_items_last_hour 6

Result: cart_items_last_hour = 6 (should be 7)
→ Lost update! One increment was silently dropped.
```

### Root Cause

**Kafka partitioning does NOT guarantee same customer → same partition** if:
- Customer key hashing distributes unevenly
- Kafka rebalance reassigns partitions during processing
- Different source tables (orders vs cart_events) go to different topics with different partition assignments

### Impact

- ❌ Counters and aggregates (cart_items, order_count) become inaccurate
- ❌ Time-window features (last_24h_activity) may miss events
- ❌ Silent data loss (no error, just wrong results)

### Solution A: Partition by Customer ID (Current Design)

**Implementation**:
```python
# Debezium connector config
"transforms": "SetKey",
"transforms.SetKey.type": "org.apache.kafka.connect.transforms.ExtractField$Key",
"transforms.SetKey.field": "unified_customer_key",

# Kafka topic partitioning
# Same unified_customer_key always goes to same partition
# Same partition always consumed by same pod (single-threaded per partition)
```

**Guarantees**:
- ✅ Events for customer U0001 are ordered within a partition
- ✅ Only one pod processes customer U0001 at a time
- ✅ No race condition between pods

**Limitations**:
- ⚠️ During Kafka **rebalance** (pod restart, scaling), partition reassignment → brief window of disorder
- ⚠️ If orders and cart_events topics partition differently, same customer could be on different partitions

**Verification**:
```bash
# Check partition assignment for a customer
kafka-console-consumer.sh \
  --bootstrap-server $KAFKA_BOOTSTRAP \
  --topic cce.rds.orders \
  --property print.key=true \
  --property print.partition=true \
  --from-beginning \
  | grep "U0001"

# All U0001 events should be on same partition number
```

### Solution B: Optimistic Locking with Lua Script

**For critical counters, use atomic Redis operations**:

```python
# Instead of read-modify-write (3 steps, non-atomic)
def update_counter_unsafe(customer_id):
    current = redis.hget(f"cce:features:realtime:{customer_id}", "order_count")
    new_value = int(current) + 1
    redis.hset(f"cce:features:realtime:{customer_id}", "order_count", new_value)

# Use atomic increment (1 step, atomic)
def update_counter_safe(customer_id):
    redis.hincrby(f"cce:features:realtime:{customer_id}", "order_count", 1)
```

**For complex logic, use Lua script** (executes atomically on Redis):
```python
# Lua script for conditional update
lua_script = """
local key = KEYS[1]
local field = ARGV[1]
local increment = tonumber(ARGV[2])
local max_value = tonumber(ARGV[3])

local current = tonumber(redis.call('HGET', key, field) or 0)
local new_value = math.min(current + increment, max_value)

redis.call('HSET', key, field, new_value)
return new_value
"""

# Execute atomically
redis.eval(
    lua_script,
    1,  # number of keys
    f"cce:features:realtime:{customer_id}",
    "cart_items_last_hour",
    1,   # increment
    100  # max_value
)
```

**Pros**:
- ✅ Guarantees atomicity even if partitioning fails
- ✅ Works across cluster shards (Lua executes on single node)

**Cons**:
- ❌ More complex code
- ❌ Slightly higher Redis CPU usage

---

## Consistency Issue #2: Batch and Stream Overwrite Conflict

### Scenario: Batch Job Overwrites Real-Time Features

**Problem**: Batch CronJob runs at 02:10, overwrites entire Redis key.

```
Timeline:

02:00: Stream Job writes:
       HSET cce:features:realtime:U0001 cart_items_last_hour 3

02:10: Batch Job runs:
       DEL cce:features:batch:U0001  (wrong! deletes wrong key)
       or
       HSET cce:features:realtime:U0001 lifetime_value 10000  (overwrites entire hash)

Result: cart_items_last_hour is lost!
```

### Root Cause

**Namespace collision**: Batch and Stream write to same Redis key.

### Solution: Separate Key Namespaces (Current Design)

**Enforce strict key separation**:
```python
# Batch importer
def write_batch_features(customer_id, features):
    redis.hset(
        f"cce:features:batch:{customer_id}",  # "batch" namespace
        mapping=features
    )

# Stream job
def write_realtime_features(customer_id, features):
    redis.hset(
        f"cce:features:realtime:{customer_id}",  # "realtime" namespace
        mapping=features
    )

# API reads both
def get_all_features(customer_id):
    batch_features = redis.hgetall(f"cce:features:batch:{customer_id}")
    realtime_features = redis.hgetall(f"cce:features:realtime:{customer_id}")
    return {**batch_features, **realtime_features}
```

**Additional safeguard: Use HSET, never DEL**:
```python
# Bad: Deletes entire key, including fields from other writers
redis.delete(f"cce:features:realtime:{customer_id}")
redis.hset(f"cce:features:realtime:{customer_id}", mapping=new_features)

# Good: Updates only specified fields, preserves others
redis.hset(f"cce:features:batch:{customer_id}", mapping=new_features)
```

**Verification**:
```python
# Add assertion in code
def write_batch_features(customer_id, features):
    key = f"cce:features:batch:{customer_id}"
    assert "batch" in key, "Batch writer must use 'batch' namespace"
    redis.hset(key, mapping=features)
```

---

## Consistency Issue #3: Redis Cluster Replication Lag

### Scenario: Master Fails Before Replica Syncs

**Problem**: Redis uses **asynchronous replication** by default.

```
Timeline:

t0: Stream Job writes to Master A: HSET ... cart_items 5
t1: Master A ACKs write to Stream Job (write considered "successful")
t2: Master A starts replicating to Replica A (async, in background)
t3: Master A CRASHES (hardware failure, network partition)
t4: Replica A is promoted to new master
    BUT: Replica A never received the write from t2!

Result: cart_items = 4 (old value), not 5
→ Data loss of most recent writes (typically last 1-5 seconds)
```

### Root Cause

**Asynchronous replication trades consistency for performance**:
- Client gets ACK immediately after master writes (fast)
- Replica syncs in background (eventual consistency)
- If master fails before sync completes → data lost

### Impact

- ❌ Recent feature updates lost (last 1-5 seconds before failover)
- ❌ Features become stale until next update
- ⚠️ Rare (ElastiCache failover is infrequent), but possible

### Solution A: Accept Risk (Recommended for CCE)

**Rationale**: Real-time features are **ephemeral** and **self-healing**.

```
Scenario: U0001's cart_items_last_hour lost during failover

What happens:
1. Redis fails over, cart_items_last_hour lost
2. Next cart event for U0001 arrives within minutes
3. Stream Job recomputes cart_items_last_hour from event
4. Feature is back to correct value

Impact: Features stale for a few minutes, then self-correct.
```

**This is acceptable because**:
- Real-time features have short TTL (24-72 hours)
- Features are recomputed from every new event
- Batch job refreshes Gold features daily (SSOT)

**Not acceptable for**: Financial transactions, order totals, payment state (but those aren't in Redis, they're in RDS).

### Solution B: Wait for Replica ACK (High Consistency Mode)

**Use Redis WAIT command** (synchronous replication):
```python
# After write, wait for 1 replica to confirm
redis.hset(f"cce:features:realtime:{customer_id}", mapping=features)
redis.execute_command("WAIT", 1, 5000)  # Wait for 1 replica, timeout 5 seconds
```

**Pros**:
- ✅ Guarantees data written to ≥1 replica before ACK
- ✅ Survives master failure

**Cons**:
- ❌ 2-5x slower writes (wait for network round-trip to replica)
- ❌ Stream Job throughput drops significantly
- ❌ If replica is down, writes block (timeout = availability issue)

**Recommendation**: Only use for critical features (e.g., fraud_score, account_balance), not for typical real-time features.

---

## Consistency Issue #4: Kafka Offset Commit Timing (Duplicate Processing)

### Scenario: Stream Job Crashes After Write, Before Commit

**Problem**: Kafka provides **at-least-once** delivery by default.

```
Timeline:

t0: Stream Job reads message from Kafka offset 1000: "U0001 added to cart"
t1: Stream Job writes to Redis: cart_items_last_hour = 6
t2: Stream Job crashes BEFORE committing offset 1001
t3: Stream Job restarts, resumes from last committed offset 1000
t4: Stream Job reads SAME message again: "U0001 added to cart"
t5: Stream Job writes to Redis: cart_items_last_hour = 7 (should still be 6!)

Result: cart_items_last_hour = 7 (over-counted by 1)
→ Duplicate processing leads to incorrect aggregate
```

### Root Cause

**Kafka offset commit happens AFTER processing**:
- Read message → Process → Write to Redis → Commit offset
- If crash happens between "Write to Redis" and "Commit offset" → message reprocessed

### Impact

- ❌ Counters and sums over-counted (cart_items, order_count, total_spent)
- ❌ Time-window features polluted with duplicate events
- ⚠️ Rare (pod crashes are infrequent), but systematic bias (always over-counts)

### Solution A: Idempotent Writes (Recommended)

**Store event ID in Redis to detect duplicates**:

```python
def process_event_idempotent(event):
    event_id = event.event_id  # e.g., Kafka offset or CDC transaction_id
    customer_id = event.unified_customer_key
    
    # Check if already processed
    event_key = f"cce:processed_events:{customer_id}"
    if redis.sismember(event_key, event_id):
        print(f"Event {event_id} already processed, skipping")
        return  # Duplicate, skip
    
    # Process event
    features = compute_features(event)
    redis.hset(f"cce:features:realtime:{customer_id}", mapping=features)
    
    # Mark as processed
    redis.sadd(event_key, event_id)
    redis.expire(event_key, 86400)  # Keep processed IDs for 24 hours
    
    # Now safe to commit Kafka offset
    kafka_consumer.commit()
```

**Pros**:
- ✅ Guarantees exactly-once semantics (each event processed once)
- ✅ Works with Kafka at-least-once delivery

**Cons**:
- ❌ Extra Redis read/write per event (2x Redis operations)
- ❌ Memory overhead (store event IDs for 24 hours)

**Optimization**: Use bloom filter for memory efficiency (probabilistic, but very space-efficient).

### Solution B: Kafka Transactions (Exactly-Once Semantics)

**Use Kafka's exactly-once semantics (EOS)**:
```python
# Producer config (for Debezium)
"producer.enable.idempotence": "true",
"producer.transactional.id": "cce-debezium-connector",

# Consumer config (for Stream Job)
"isolation.level": "read_committed",
"enable.auto.commit": "false",  # Manual commit

# In Stream Job
consumer.begin_transaction()
process_batch(messages)
write_to_redis(features)
consumer.commit_transaction()  # Atomically commits offset + output
```

**Pros**:
- ✅ Native exactly-once support in Kafka
- ✅ No application-level deduplication logic

**Cons**:
- ❌ Kafka transactions don't cover Redis writes (only Kafka → Kafka)
- ❌ Complex setup, requires Kafka 2.5+ and transactional producers
- ❌ Doesn't prevent duplicate Redis writes if crash happens after transaction commit but before Redis ACK

**Recommendation**: Stick with Solution A (idempotent writes) for CCE use case.

---

## Consistency Issue #5: Redis Cluster Split-Brain During Network Partition

### Scenario: Network Partition Isolates Master

**Problem**: Redis Cluster nodes lose connectivity, but both sides think they're alive.

```
Network partition:

       [Master A]  [Master B]  [Master C]
           |           |           |
       [Replica A] [Replica B] [Replica C]

       Network split:

       [Master A]      |    [Master B]  [Master C]
           |           |        |           |
       [Replica A]     |    [Replica B] [Replica C]

Left side:
- Master A thinks it's still primary
- Accepts writes from Stream Pods on left side

Right side:
- Cluster promotes Replica A to new master (quorum-based)
- Accepts writes from Stream Pods on right side

Result: Two masters accepting writes → data divergence
```

### Root Cause

**CAP theorem**: In a network partition, you can't have both consistency AND availability.
- Redis chooses **availability** (AP in CAP) by default
- Both sides continue accepting writes to avoid downtime

### Impact

- ❌ Data diverges during partition (writes to different masters)
- ❌ After partition heals, one side's data is discarded (last-writer-wins)
- ⚠️ Very rare (requires specific network failure), but possible in multi-AZ

### Solution: Cluster Quorum Configuration

**ElastiCache Redis Cluster uses majority quorum**:
```hcl
# Terraform ElastiCache config
resource "aws_elasticache_replication_group" "cce_redis_cluster" {
  cluster_mode {
    num_node_groups         = 3  # 3 shards
    replicas_per_node_group = 1  # 1 replica per shard
  }
  
  # Quorum-based failover (requires majority of nodes)
  automatic_failover_enabled = true
  multi_az_enabled          = true  # Spread across AZs to survive AZ failure
}
```

**Quorum rule**:
- 3 shards = requires ≥2 shards to form quorum
- If network partition splits 1 vs 2 shards:
  - Side with 2 shards continues (has quorum)
  - Side with 1 shard rejects writes (no quorum) → Stream Job sees write errors

**Pros**:
- ✅ Prevents split-brain (minority side goes read-only)
- ✅ Data consistency maintained

**Cons**:
- ❌ Minority side unavailable during partition (CP in CAP)
- ❌ Stream Job on minority side will accumulate Kafka lag (writes fail)

**Mitigation**: Stream Job retry logic + exponential backoff.

---

## Consistency Issue #6: Clock Skew Between Batch and Stream

### Scenario: Timestamp-Based Logic Disagrees

**Problem**: Batch job and Stream job use timestamps to compute time-window features.

```
Batch Job (Databricks):
- Uses event_timestamp from source data
- Computes: last_order_date = "2026-09-03 14:30:00"

Stream Job (EKS pod):
- Uses pod system clock (may be skewed by seconds)
- Receives same event via CDC
- Computes: last_order_timestamp = "2026-09-03 14:30:05" (5 seconds later)

Result: Timestamp mismatch
→ API serves inconsistent data depending on which feature it reads
```

### Root Cause

**Distributed clocks are never perfectly synchronized**:
- Databricks cluster clock vs EKS pod clock
- NTP drift (seconds-level skew)
- CDC event timestamp vs processing timestamp

### Impact

- ⚠️ Timestamps differ by seconds (usually <10 seconds)
- ⚠️ Time-window features slightly off (e.g., "last 24 hours" may be 24h ± 10s)
- ❌ Order of events may be incorrect if events happen within skew window

### Solution A: Use Source Timestamp (Event Time)

**Always use event_timestamp from source data, never processing time**:

```python
# Bad: Uses processing time (clock-dependent)
def process_event(event):
    timestamp = datetime.now()  # Pod's system clock
    redis.hset(..., "last_order_timestamp", timestamp)

# Good: Uses event time (clock-independent)
def process_event(event):
    timestamp = event.event_timestamp  # From source database
    redis.hset(..., "last_order_timestamp", timestamp)
```

**Pros**:
- ✅ Consistent across batch and stream (same source timestamp)
- ✅ Correct ordering even with clock skew

**Cons**:
- ⚠️ Events may arrive out-of-order (network delays)
- ⚠️ Need to handle late-arriving events (event_timestamp in past)

### Solution B: Accept Small Skew

**For non-critical features, tolerate seconds-level skew**:
- cart_items_last_hour: 5 seconds doesn't matter
- last_order_timestamp: UI shows "2 minutes ago" (rounded), precise timestamp irrelevant

**Only enforce strict ordering for critical features**:
- Fraud detection: order of transactions matters
- Inventory: order of purchases matters

---

## Summary: Consistency Risk Matrix

| Issue | Likelihood | Impact | Mitigation | Implemented? |
|-------|------------|--------|------------|--------------|
| **Race condition between pods** | Low (if partitioned by customer) | High (lost updates) | Partition by customer ID + atomic Redis ops | ✅ Yes (partition), ⚠️ Partial (atomic ops) |
| **Batch overwrites Stream** | Medium (if not namespaced) | High (data loss) | Separate key namespaces | ✅ Yes |
| **Redis replication lag** | Low (only during failover) | Low (self-healing) | Accept risk OR use WAIT | ✅ Accept risk |
| **Duplicate processing** | Medium (pod crashes) | Medium (over-counting) | Idempotent writes with event ID | ⚠️ Not implemented |
| **Split-brain** | Very low (requires network partition) | High (data divergence) | Quorum-based cluster | ✅ Yes (ElastiCache default) |
| **Clock skew** | High (always present) | Low (seconds-level) | Use event timestamps | ✅ Yes |

---

## Recommendations

### Immediate (Phase 1)
1. ✅ **Enforce key namespaces**: `cce:features:batch:{id}` vs `cce:features:realtime:{id}`
2. ✅ **Verify Kafka partitioning**: All events for same customer → same partition
3. ⚠️ **Implement idempotent writes**: Add event ID deduplication for critical counters

### Short-Term (Phase 2)
1. **Use atomic Redis operations**: Replace read-modify-write with HINCRBY/Lua scripts for counters
2. **Add monitoring**: Track duplicate event rate, replication lag, failover frequency
3. **Document consistency trade-offs**: Make team aware of accepted risks

### Long-Term (If Needed)
1. **Exactly-once semantics**: Only if counter accuracy becomes critical (e.g., billing)
2. **Synchronous replication (WAIT)**: Only for high-value features (fraud scores)
3. **Distributed transaction coordinator**: Only if cross-system consistency required (overkill for CCE)

---

## Testing Consistency Issues

### Chaos Engineering Scenarios

```bash
# Scenario 1: Kill Redis master mid-write
# Expected: Stream Job retries, writes not lost

# Scenario 2: Partition network between pods
# Expected: Each pod continues processing its partitions independently

# Scenario 3: Kill Stream Job pod after Redis write, before Kafka commit
# Expected: Event reprocessed, idempotent logic prevents double-counting

# Scenario 4: Run Batch Job and Stream Job concurrently
# Expected: No overwrites, keys are separate

# Scenario 5: Inject 10-second clock skew in one pod
# Expected: Timestamps differ by ≤10s, time-window features still reasonable
```

### Unit Tests

```python
def test_idempotent_write():
    """Same event processed twice should not double-count"""
    event = create_test_event(event_id="evt_123", customer_id="U0001")
    
    # Process once
    processor.process_event(event)
    count_1 = redis.hget("cce:features:realtime:U0001", "order_count")
    
    # Process again (simulate duplicate)
    processor.process_event(event)
    count_2 = redis.hget("cce:features:realtime:U0001", "order_count")
    
    assert count_1 == count_2, "Duplicate event should not increment counter"

def test_namespace_isolation():
    """Batch and Stream should not overwrite each other"""
    # Batch writes
    write_batch_features("U0001", {"lifetime_value": 10000})
    
    # Stream writes
    write_realtime_features("U0001", {"cart_items": 5})
    
    # Both should exist
    batch_features = redis.hgetall("cce:features:batch:U0001")
    realtime_features = redis.hgetall("cce:features:realtime:U0001")
    
    assert "lifetime_value" in batch_features
    assert "cart_items" in realtime_features
    assert "cart_items" not in batch_features  # No cross-contamination
```

---

## References

- [Redis Cluster Specification](https://redis.io/docs/reference/cluster-spec/)
- [Kafka Exactly-Once Semantics](https://www.confluent.io/blog/exactly-once-semantics-are-possible-heres-how-apache-kafka-does-it/)
- [Designing Data-Intensive Applications (Chapter 9: Consistency)](https://dataintensive.net/)
- [AWS ElastiCache Replication](https://docs.aws.amazon.com/AmazonElastiCache/latest/red-ug/Replication.html)
