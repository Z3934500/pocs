# ADR-005 Appendix: Kafka Consumer Backpressure and Memory Management

**Related**: [ADR-005: Two-Phase Autoscaling Strategy](ADR-005-two-phase-autoscaling-strategy.md)  
**Date**: 2026-09-03

## Problem Statement

In production Stream Job deployments, a critical issue emerges:

```
Kafka Consumer poll() is fast (milliseconds)
   ↓
Messages accumulate in memory queue
   ↓
Worker threads process slowly (500ms per message)
   ↓
Memory queue grows unbounded
   ↓
OOM or excessive GC pressure
   ↓
Pod killed by Kubernetes (OOMKilled)
   ↓
Consumer group rebalance triggered
   ↓
Kafka lag spikes, cascading failures
```

**Key observation**: **Kafka lag may be low, but pod memory keeps growing** → The bottleneck is inside the pod, not in Kafka.

This document addresses:
1. The relationship between Kafka Consumer poll(), Worker Thread Pool, and Partition consumption
2. How to prevent Consumer pull speed > Worker processing speed
3. How to avoid Consumer Group being judged "dead" and triggering rebalance
4. How to debug when Kafka lag is low but pod memory is rising

---

## Part 1: Architecture - Three Components Relationship

### Current CCE Stream Job Architecture (Simplified)

```
┌─────────────────────────────────────────────────────────────┐
│  EKS Pod (cce-realtime-feature-stream-0)                    │
│                                                               │
│  ┌─────────────────┐                                        │
│  │ Kafka Consumer  │  ← poll() every 1-5 seconds            │
│  │ Thread (1)      │                                         │
│  └────────┬────────┘                                        │
│           │ put()                                            │
│           ↓                                                  │
│  ┌─────────────────────────────────────────────┐           │
│  │  In-Memory Blocking Queue                   │           │
│  │  Capacity: 1000 messages (configurable)     │           │
│  │  Current: 850 messages ← DANGER!            │           │
│  └────────┬────────────────────────────────────┘           │
│           │ take()                                           │
│           ↓                                                  │
│  ┌──────────────────────────────────────────────┐          │
│  │  Worker Thread Pool (8 threads)              │          │
│  │  Thread 1: processing message (350ms so far) │          │
│  │  Thread 2: writing to Redis (150ms)          │          │
│  │  Thread 3-8: processing...                   │          │
│  └──────────────────────────────────────────────┘          │
│           ↓                                                  │
│  ┌──────────────────────────────────────────────┐          │
│  │  Redis / RocksDB (external I/O)              │          │
│  └──────────────────────────────────────────────┘          │
└─────────────────────────────────────────────────────────────┘
         ↑
         │ Assigned 3 partitions (out of 30 total)
         │
┌────────┴──────────────────────────────────────┐
│  MSK Kafka Cluster                            │
│  Topic: cce.rds.orders (30 partitions)        │
│  Consumer Group: cce-realtime-feature-stream  │
│  10 pods × 3 partitions each = 30 total       │
└───────────────────────────────────────────────┘
```

### The Relationship

| Component | Role | Speed | Buffering |
|-----------|------|-------|-----------|
| **Kafka Consumer Thread** | Poll messages from Kafka broker | Fast (network I/O, 10-50ms per poll) | Controlled by `max.poll.records` |
| **In-Memory Queue** | Buffer between Consumer and Workers | N/A (just storage) | **Unbounded growth risk** if not managed |
| **Worker Thread Pool** | Process business logic (compute features, write Redis) | Slow (CPU + I/O, 100-500ms per message) | Limited by thread pool size |

**Key insight**: Consumer and Worker operate at **different speeds** → Queue is the pressure relief valve.

---

## Part 2: Backpressure Control - Preventing Consumer from Outpacing Workers

### Problem: Unbounded Queue Growth

**Default behavior** (危险):
```python
# Kafka consumer pulls as fast as possible
consumer = KafkaConsumer(
    'cce.rds.orders',
    max_poll_records=5000,  # Pull 5000 messages per poll!
    fetch_max_wait_ms=500,
)

# Worker thread pool
executor = ThreadPoolExecutor(max_workers=8)

# No backpressure!
while True:
    messages = consumer.poll(timeout_ms=1000)
    for msg in messages:
        queue.put(msg)  # Queue grows infinitely if workers can't keep up
        executor.submit(process_message, msg)
```

**What happens**:
- Consumer pulls 5000 messages in 1 second
- Worker pool processes 8 messages × 2/sec = 16 messages/sec
- Queue grows by 5000 - 16 = 4984 messages/sec
- In 60 seconds, queue has 299,040 messages → OOM

### Solution A: Limit `max.poll.records`

**Reduce the batch size per poll**:
```python
consumer = KafkaConsumer(
    'cce.rds.orders',
    max_poll_records=500,  # Only pull 500 at a time (vs 5000)
    max_poll_interval_ms=300000,  # 5 minutes to process 500 messages
)
```

**Benefits**:
- Smaller bursts → queue growth is slower
- Gives workers time to drain queue between polls

**Limitations**:
- Still no hard limit on queue size
- If workers are consistently slower, queue still grows (just slower)

### Solution B: Pause/Resume Based on Queue Depth (Recommended)

**Implement active backpressure**:
```python
import queue
from kafka import KafkaConsumer, TopicPartition

# Configuration
MAX_QUEUE_SIZE = 1000
QUEUE_HIGH_WATERMARK = 0.8  # 80% full → pause
QUEUE_LOW_WATERMARK = 0.3   # 30% full → resume

# Bounded queue
message_queue = queue.Queue(maxsize=MAX_QUEUE_SIZE)

# Kafka consumer
consumer = KafkaConsumer(
    'cce.rds.orders',
    group_id='cce-realtime-feature-stream',
    max_poll_records=500,
    max_poll_interval_ms=300000,  # 5 minutes
    enable_auto_commit=False,
)

# Track pause state
paused = False

def consumer_thread():
    """Consumer thread: poll from Kafka and feed queue"""
    global paused
    
    while True:
        # Check queue depth
        queue_depth = message_queue.qsize()
        queue_utilization = queue_depth / MAX_QUEUE_SIZE
        
        # Backpressure logic
        if queue_utilization > QUEUE_HIGH_WATERMARK and not paused:
            print(f"Queue {queue_utilization:.0%} full, PAUSING consumer")
            consumer.pause(*consumer.assignment())  # Stop fetching
            paused = True
        
        elif queue_utilization < QUEUE_LOW_WATERMARK and paused:
            print(f"Queue {queue_utilization:.0%} full, RESUMING consumer")
            consumer.resume(*consumer.assignment())  # Resume fetching
            paused = False
        
        # Poll messages (respects pause state)
        messages = consumer.poll(timeout_ms=1000)
        
        for topic_partition, records in messages.items():
            for record in records:
                try:
                    # Block if queue is full (shouldn't happen with pause, but safety)
                    message_queue.put(record, timeout=5)
                except queue.Full:
                    print("Queue full despite pause, dropping message (data loss!)")
                    # Alternative: block indefinitely until space available
                    message_queue.put(record)  # Blocking put

def worker_thread():
    """Worker thread: process messages from queue"""
    while True:
        try:
            msg = message_queue.get(timeout=1)
            process_message(msg)  # 500ms processing time
            message_queue.task_done()
        except queue.Empty:
            continue

# Start threads
import threading
threading.Thread(target=consumer_thread, daemon=True).start()

# Worker pool
for i in range(8):
    threading.Thread(target=worker_thread, daemon=True).start()

# Monitor and commit offsets periodically
while True:
    time.sleep(30)
    consumer.commit()  # Commit processed offsets
    print(f"Queue depth: {message_queue.qsize()}/{MAX_QUEUE_SIZE}")
```

**How it works**:
1. When queue reaches 80% capacity → `consumer.pause()` stops fetching from Kafka
2. Workers drain the queue down to 30%
3. `consumer.resume()` starts fetching again
4. Queue stays bounded, no OOM risk

**Benefits**:
- ✅ Hard limit on memory usage (bounded queue)
- ✅ Dynamic adaptation to worker speed
- ✅ No data loss (consumer pauses gracefully)

**Trade-offs**:
- ⚠️ Kafka lag may increase during pause (expected, not a bug)
- ⚠️ Adds complexity to consumer logic

### Solution C: Block on Queue Put (Simpler)

**Alternative: blocking queue with timeout**:
```python
message_queue = queue.Queue(maxsize=1000)  # Hard limit

for record in records:
    # Block until space available (backpressure propagates to consumer)
    message_queue.put(record)  # Blocks if full
```

**How it works**:
- If queue is full, `put()` blocks → consumer thread stalls
- Consumer doesn't fetch more messages until workers catch up
- Implicit backpressure (no explicit pause/resume)

**Benefits**:
- ✅ Simpler code (no pause/resume logic)
- ✅ Bounded memory

**Trade-offs**:
- ⚠️ Consumer thread blocked → no heartbeat? (See Part 3)
- ⚠️ Less observable (can't see "paused" state in metrics)

---

## Part 3: Avoiding Consumer Group Rebalance

### Problem: Worker Slowness Triggers Rebalance

**Scenario**:
```
t0: Consumer polls 500 messages
t1: Consumer puts messages in queue (fast, <100ms)
t2: Workers start processing (slow, 500ms × 500 messages = 250 seconds total)
t3: 5 minutes pass...
t4: Kafka broker: "Consumer hasn't called poll() in >5 min → assume dead"
t5: Kafka triggers rebalance, reassigns partitions to other pods
t6: Original pod wakes up, realizes it's been kicked out
t7: Partial message processing, Kafka lag spike, chaos
```

**Root cause**: `max.poll.interval.ms` is the maximum time between consecutive `poll()` calls. If workers are slow, consumer can't call `poll()` again in time.

### Solution A: Increase `max.poll.interval.ms`

**Give workers more time to process**:
```python
consumer = KafkaConsumer(
    'cce.rds.orders',
    max_poll_interval_ms=600000,  # 10 minutes (up from 5 min default)
    max_poll_records=500,
)
```

**Calculation**:
```
max_poll_interval_ms = max_poll_records × avg_processing_time_per_message × safety_factor

Example:
  500 messages × 500ms × 2 (safety) = 500,000ms = 8.3 minutes
  → Set to 600,000ms (10 minutes)
```

**Benefits**:
- ✅ Prevents premature rebalance due to slow workers

**Limitations**:
- ⚠️ If pod truly dies, takes 10 minutes before Kafka detects it (slow failover)
- ⚠️ Doesn't solve the underlying problem (workers too slow)

### Solution B: Separate Heartbeat from Processing (Recommended)

**Problem with Solution A**: If consumer thread is blocked (e.g., queue.put() blocks), it can't send heartbeats either.

**Kafka's built-in solution**: Separate background heartbeat thread.

```python
consumer = KafkaConsumer(
    'cce.rds.orders',
    
    # Heartbeat thread settings (independent of poll)
    heartbeat_interval_ms=3000,       # Send heartbeat every 3 seconds
    session_timeout_ms=30000,         # Kafka waits 30 seconds for heartbeat
    
    # Poll interval settings (for consumer thread)
    max_poll_interval_ms=600000,      # 10 minutes to process batch
    max_poll_records=500,
)
```

**How it works**:
- Kafka client spawns a **background thread** that sends heartbeats every 3 seconds
- Main consumer thread can be blocked on slow workers for up to 10 minutes
- As long as heartbeat thread is alive, Kafka considers consumer healthy
- Only if heartbeat stops for >30 seconds does Kafka trigger rebalance

**Configuration guide**:
```python
# Rule of thumb:
# heartbeat_interval_ms < session_timeout_ms < max_poll_interval_ms

# Conservative settings (prioritize stability):
heartbeat_interval_ms = 3000        # 3 seconds
session_timeout_ms = 30000          # 30 seconds (10x heartbeat)
max_poll_interval_ms = 600000       # 10 minutes (20x session timeout)

# Aggressive settings (faster failure detection):
heartbeat_interval_ms = 1000        # 1 second
session_timeout_ms = 10000          # 10 seconds
max_poll_interval_ms = 300000       # 5 minutes
```

**Benefits**:
- ✅ Decouples heartbeat from processing speed
- ✅ Fast failure detection (30 sec) + slow processing tolerance (10 min)
- ✅ Built-in to Kafka client (no custom code)

---

## Part 4: Debugging High Memory with Low Kafka Lag

### Symptom

```
Observation:
- Kafka consumer lag: 500 messages (low, healthy)
- Pod memory usage: 1.8 GB / 2 GB limit (90%, critical)
- Memory growth rate: +50 MB/minute (unsustainable)
- GC frequency: every 10 seconds (excessive)

Question: If lag is low, where are the messages?
```

### Root Cause Analysis: Layer-by-Layer

#### Layer 1: JVM Heap Analysis

**Tool**: Heap dump
```bash
# Trigger heap dump from running pod
kubectl exec -it cce-realtime-feature-stream-0 -n cce-platform -- \
  jmap -dump:live,format=b,file=/tmp/heapdump.hprof 1

# Download and analyze
kubectl cp cce-platform/cce-realtime-feature-stream-0:/tmp/heapdump.hprof ./heapdump.hprof

# Use Eclipse MAT or VisualVM to analyze
# Common findings:
# 1. Byte arrays (serialized messages in queue)
# 2. Thread objects (too many worker threads)
# 3. Connection pools (leaked HTTP/Redis connections)
```

**Typical issues**:
- **Message queue bloat**: Queue contains thousands of messages → check queue.qsize()
- **Object churn**: Creating many temporary objects during processing → GC pressure
- **Thread leak**: Too many threads created (check thread count in metrics)

#### Layer 2: Application Code Review

**Check 1: Queue capacity**
```python
# Is queue unbounded?
message_queue = queue.Queue()  # BAD: no maxsize

# Should be bounded
message_queue = queue.Queue(maxsize=1000)  # GOOD
```

**Check 2: Worker thread pool size**
```python
# Too many workers?
executor = ThreadPoolExecutor(max_workers=100)  # BAD: 100 threads × 1MB stack = 100MB

# Right-size based on I/O wait time
# Formula: workers = (avg_processing_time / avg_io_wait_time) × num_cores
# Example: (500ms / 400ms) × 4 cores = 5 workers
executor = ThreadPoolExecutor(max_workers=8)  # GOOD
```

**Check 3: Message processing failure retry**
```python
# Are failed messages retried infinitely?
def process_message(msg):
    try:
        update_redis(msg)
    except Exception:
        message_queue.put(msg)  # BAD: infinite retry fills queue

# Better: Use retry limit or DLQ
def process_message_safe(msg, retry_count=0):
    try:
        update_redis(msg)
    except Exception as e:
        if retry_count < 3:
            message_queue.put((msg, retry_count + 1))
        else:
            send_to_dlq(msg)  # Dead letter queue
```

#### Layer 3: Kafka Client Configuration

**Check 1: `max.poll.records` too large**
```python
# Pulls 5000 messages at once → 5000 × 10KB = 50MB in memory
consumer = KafkaConsumer(max_poll_records=5000)  # BAD for memory-constrained pods

# Reduce batch size
consumer = KafkaConsumer(max_poll_records=500)  # GOOD
```

**Check 2: `fetch.min.bytes` causing large fetches**
```python
# Waits for 10MB of data before returning → large memory spike
consumer = KafkaConsumer(fetch_min_bytes=10485760)  # 10MB

# Use smaller fetch size
consumer = KafkaConsumer(fetch_min_bytes=1048576)  # 1MB
```

#### Layer 4: Kubernetes Resource Limits

**Check 1: Memory limit too low**
```yaml
# Current config
resources:
  limits:
    memory: 2Gi  # Total limit
  requests:
    memory: 1Gi

# JVM heap may be too close to limit
# Rule: JVM heap should be 75% of memory limit
# 2GB limit → 1.5GB heap + 500MB off-heap (threads, network buffers, etc.)
```

**Check 2: Memory leak (unclosed connections)**
```python
# Memory leak example
def update_redis(msg):
    r = redis.Redis(host='redis-url')  # BAD: creates new connection every time
    r.set('key', 'value')
    # Connection never closed → connection pool exhaustion + memory leak

# Fix: Use connection pool
redis_pool = redis.ConnectionPool(host='redis-url', max_connections=50)
r = redis.Redis(connection_pool=redis_pool)  # Reuse connections
```

#### Layer 5: Metrics and Observability

**Monitor these metrics** (Prometheus/Grafana):
```promql
# Memory usage trend
container_memory_working_set_bytes{pod=~"cce-realtime-feature-stream-.*"}

# GC frequency and duration
jvm_gc_collection_seconds_count
jvm_gc_collection_seconds_sum

# Queue depth (custom metric, export from application)
kafka_consumer_queue_depth{pod="cce-realtime-feature-stream-0"}

# Thread count
jvm_threads_current

# Heap usage
jvm_memory_bytes_used{area="heap"}

# Redis connection pool usage
redis_connection_pool_active
redis_connection_pool_idle
```

**Alert rules**:
```yaml
# Alert if memory usage >85% for 5 minutes
- alert: StreamJobHighMemory
  expr: (container_memory_working_set_bytes / container_spec_memory_limit_bytes) > 0.85
  for: 5m

# Alert if GC time >10% of CPU time
- alert: StreamJobExcessiveGC
  expr: rate(jvm_gc_collection_seconds_sum[5m]) / rate(process_cpu_seconds_total[5m]) > 0.1
  for: 5m

# Alert if queue depth >80% capacity
- alert: StreamJobQueueBacklog
  expr: kafka_consumer_queue_depth / kafka_consumer_queue_capacity > 0.8
  for: 5m
```

---

## Part 5: Production-Ready Stream Job Implementation

### Complete Example with Backpressure

```python
import queue
import threading
import time
from kafka import KafkaConsumer
from prometheus_client import Gauge, Counter, Histogram
import redis

# ============================================================
# Configuration
# ============================================================
KAFKA_BOOTSTRAP_SERVERS = "msk-broker:9092"
KAFKA_TOPIC = "cce.rds.orders"
KAFKA_GROUP_ID = "cce-realtime-feature-stream"

MAX_QUEUE_SIZE = 1000
QUEUE_HIGH_WATERMARK = 0.8
QUEUE_LOW_WATERMARK = 0.3

WORKER_THREADS = 8
MAX_POLL_RECORDS = 500
MAX_POLL_INTERVAL_MS = 600000  # 10 minutes
HEARTBEAT_INTERVAL_MS = 3000   # 3 seconds
SESSION_TIMEOUT_MS = 30000     # 30 seconds

REDIS_URL = "redis://elasticache:6379"
redis_pool = redis.ConnectionPool.from_url(REDIS_URL, max_connections=50)
redis_client = redis.Redis(connection_pool=redis_pool)

# ============================================================
# Metrics (Prometheus)
# ============================================================
queue_depth_gauge = Gauge('kafka_consumer_queue_depth', 'Current queue depth')
queue_capacity_gauge = Gauge('kafka_consumer_queue_capacity', 'Max queue capacity')
messages_processed_counter = Counter('messages_processed_total', 'Total messages processed')
processing_time_histogram = Histogram('message_processing_seconds', 'Time to process one message')
consumer_paused_gauge = Gauge('kafka_consumer_paused', 'Consumer pause state (1=paused, 0=running)')

queue_capacity_gauge.set(MAX_QUEUE_SIZE)

# ============================================================
# Shared State
# ============================================================
message_queue = queue.Queue(maxsize=MAX_QUEUE_SIZE)
consumer_paused = False
shutdown_flag = threading.Event()

# ============================================================
# Consumer Thread
# ============================================================
def kafka_consumer_thread():
    global consumer_paused
    
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_GROUP_ID,
        max_poll_records=MAX_POLL_RECORDS,
        max_poll_interval_ms=MAX_POLL_INTERVAL_MS,
        heartbeat_interval_ms=HEARTBEAT_INTERVAL_MS,
        session_timeout_ms=SESSION_TIMEOUT_MS,
        enable_auto_commit=False,
        auto_offset_reset='latest',
    )
    
    print(f"Consumer started, assigned partitions: {consumer.assignment()}")
    
    while not shutdown_flag.is_set():
        # Backpressure control
        queue_depth = message_queue.qsize()
        queue_utilization = queue_depth / MAX_QUEUE_SIZE
        queue_depth_gauge.set(queue_depth)
        
        if queue_utilization > QUEUE_HIGH_WATERMARK and not consumer_paused:
            print(f"[BACKPRESSURE] Queue {queue_utilization:.0%} full, PAUSING consumer")
            consumer.pause(*consumer.assignment())
            consumer_paused = True
            consumer_paused_gauge.set(1)
        
        elif queue_utilization < QUEUE_LOW_WATERMARK and consumer_paused:
            print(f"[BACKPRESSURE] Queue {queue_utilization:.0%} full, RESUMING consumer")
            consumer.resume(*consumer.assignment())
            consumer_paused = False
            consumer_paused_gauge.set(0)
        
        # Poll messages
        messages = consumer.poll(timeout_ms=1000)
        
        for topic_partition, records in messages.items():
            for record in records:
                try:
                    # Put in queue with timeout to avoid infinite blocking
                    message_queue.put(record, timeout=5)
                except queue.Full:
                    print("[ERROR] Queue full despite backpressure, blocking...")
                    message_queue.put(record)  # Block until space available
        
        # Commit offsets periodically (every 30 seconds or 500 messages)
        if time.time() % 30 < 1:  # Rough periodic check
            consumer.commit()
    
    consumer.close()
    print("Consumer thread stopped")

# ============================================================
# Worker Threads
# ============================================================
def worker_thread(worker_id):
    print(f"Worker {worker_id} started")
    
    while not shutdown_flag.is_set():
        try:
            msg = message_queue.get(timeout=1)
        except queue.Empty:
            continue
        
        # Process message
        with processing_time_histogram.time():
            try:
                process_message(msg)
                messages_processed_counter.inc()
            except Exception as e:
                print(f"[ERROR] Worker {worker_id} failed to process message: {e}")
                # TODO: Send to dead letter queue
            finally:
                message_queue.task_done()
    
    print(f"Worker {worker_id} stopped")

def process_message(record):
    """Business logic: compute features and write to Redis"""
    customer_id = record.key.decode('utf-8')
    event_data = json.loads(record.value.decode('utf-8'))
    
    # Compute features (example)
    features = {
        'cart_items_last_hour': compute_cart_items(customer_id, event_data),
        'intent_score': compute_intent_score(customer_id, event_data),
        'last_order_timestamp': event_data.get('timestamp'),
    }
    
    # Write to Redis (with connection pooling)
    redis_client.hset(
        f"cce:features:realtime:{customer_id}",
        mapping=features
    )
    redis_client.expire(f"cce:features:realtime:{customer_id}", 86400)  # 24h TTL

# ============================================================
# Main
# ============================================================
if __name__ == "__main__":
    # Start consumer thread
    consumer_t = threading.Thread(target=kafka_consumer_thread, daemon=False)
    consumer_t.start()
    
    # Start worker threads
    worker_threads = []
    for i in range(WORKER_THREADS):
        t = threading.Thread(target=worker_thread, args=(i,), daemon=False)
        t.start()
        worker_threads.append(t)
    
    # Start Prometheus metrics server
    from prometheus_client import start_http_server
    start_http_server(9404)  # Expose metrics on :9404/metrics
    print("Metrics server started on :9404")
    
    # Main loop: monitor queue and log stats
    try:
        while True:
            time.sleep(10)
            print(f"[STATS] Queue: {message_queue.qsize()}/{MAX_QUEUE_SIZE}, "
                  f"Paused: {consumer_paused}, "
                  f"Processed: {messages_processed_counter._value.get()}")
    except KeyboardInterrupt:
        print("Shutting down...")
        shutdown_flag.set()
        consumer_t.join()
        for t in worker_threads:
            t.join()
        print("Shutdown complete")
```

---

## Summary: Production Checklist

### Configuration Tuning
- [ ] Set `max.poll.records` based on memory and processing time (recommend 500)
- [ ] Set `max.poll.interval.ms` to allow workers to finish batch (recommend 10 minutes)
- [ ] Set `heartbeat.interval.ms` low enough for fast failure detection (recommend 3 seconds)
- [ ] Set `session.timeout.ms` to tolerate brief network glitches (recommend 30 seconds)

### Backpressure Implementation
- [ ] Use bounded queue (`Queue(maxsize=N)`)
- [ ] Implement pause/resume based on queue depth (80% high, 30% low watermark)
- [ ] Export queue depth metric to Prometheus
- [ ] Alert on queue >80% for >5 minutes

### Memory Management
- [ ] Right-size worker thread pool (8-16 threads for I/O-bound workload)
- [ ] Use connection pooling for Redis/HTTP (no connection leaks)
- [ ] Set JVM heap to 75% of pod memory limit
- [ ] Monitor GC frequency and duration (target <5% CPU time in GC)

### Observability
- [ ] Export metrics: queue depth, messages processed, processing time, GC stats
- [ ] Set up alerts: high memory, excessive GC, queue backlog, consumer lag
- [ ] Periodic heap dumps for memory leak investigation
- [ ] Log slow messages (processing time >1 second)

### Testing
- [ ] Load test with synthetic events to find breaking point
- [ ] Chaos test: kill pod mid-processing, verify no data loss
- [ ] Backpressure test: flood Kafka topic, verify queue doesn't OOM
- [ ] Rebalance test: scale pods up/down, verify smooth handoff

---

## References

- [Kafka Consumer Configuration](https://kafka.apache.org/documentation/#consumerconfigs)
- [Kafka Rebalance Protocol](https://kafka.apache.org/documentation/#consumerops)
- [Java Memory Management Best Practices](https://docs.oracle.com/javase/8/docs/technotes/guides/vm/gctuning/)
- [Prometheus Client Python](https://github.com/prometheus/client_python)
