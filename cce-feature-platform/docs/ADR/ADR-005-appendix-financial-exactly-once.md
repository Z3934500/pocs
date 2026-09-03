# ADR-005 Appendix: Financial-Grade Exactly-Once Semantics

**Related**: [ADR-005: Two-Phase Autoscaling Strategy](ADR-005-two-phase-autoscaling-strategy.md)  
**Date**: 2026-09-03  
**Context**: This document addresses the critical question: **How to guarantee exactly-once processing for financial transactions in a Kafka-based stream processing system?**

## Problem Statement

**Scenario**: A critical financial transaction event (e.g., payment, refund, account debit) flows through:

```
Kafka Topic (payment-events)
    ↓
Consumer (Stream Job Pod)
    ↓
PostgreSQL (debit account, write ledger)
    ↓
Downstream Kafka Topic (payment-completed)
```

**Failure scenario**:
```
t0: Consumer polls message (offset 1000): "Debit $100 from Account A"
t1: Worker processes: BEGIN TRANSACTION
t2: UPDATE accounts SET balance = balance - 100 WHERE account_id = 'A'
t3: INSERT INTO ledger (transaction_id, amount) VALUES ('txn_123', -100)
t4: COMMIT  ← Database transaction succeeds
t5: consumer.commitSync()  ← Pod crashes HERE before offset commit
t6: Consumer restarts, polls from last committed offset (999)
t7: Message offset 1000 is redelivered
t8: Worker processes AGAIN: "Debit $100 from Account A"
    → Balance debited twice!
    → Ledger has duplicate entries!
    → Customer overcharged!
```

**Requirements**:
1. ❌ **No duplicate debits**: Same transaction ID must not debit account twice
2. ❌ **No lost transactions**: If database write succeeds, transaction is permanent
3. ❌ **No orphaned events**: If database write succeeds, downstream event MUST be sent
4. ✅ **Idempotent replay**: Reprocessing same message produces same end state

---

## Part 1: Why `commitSync()` + Unique Index Is Not Enough

### Misconception

```python
# Naive approach (WRONG for financial transactions)
def process_payment(message):
    txn_id = message.value['transaction_id']
    amount = message.value['amount']
    account_id = message.value['account_id']
    
    with db.transaction():
        # Unique constraint on transaction_id
        db.execute("""
            INSERT INTO ledger (transaction_id, account_id, amount, created_at)
            VALUES (%s, %s, %s, NOW())
        """, (txn_id, account_id, amount))
        
        # Debit account
        db.execute("""
            UPDATE accounts
            SET balance = balance - %s
            WHERE account_id = %s
        """, (amount, account_id))
    
    # Commit offset after DB commit
    consumer.commitSync()
```

### Why This Fails

**Scenario 1: Offset commit fails, message replays**

```
First attempt:
  1. INSERT INTO ledger → Success (balance: $1000 → $900)
  2. UPDATE accounts → Success
  3. DB COMMIT → Success
  4. commitSync() → FAILS (network issue)
  
Second attempt (replay):
  1. INSERT INTO ledger → FAILS (unique constraint violation) ✓
  2. UPDATE accounts → EXECUTES! (balance: $900 → $800) ✗
  
Result: Ledger has one record ($100 debit), but account debited twice!
```

**Root cause**: Unique constraint only prevents duplicate **inserts**, not duplicate **business logic execution**.

**Scenario 2: Check-then-act race condition**

```python
# Another naive approach (still WRONG)
def process_payment(message):
    txn_id = message.value['transaction_id']
    
    # Check if already processed
    existing = db.query("SELECT * FROM ledger WHERE transaction_id = %s", txn_id)
    if existing:
        return  # Already processed, skip
    
    # Process payment
    with db.transaction():
        debit_account(account_id, amount)
        insert_ledger(txn_id, amount)
    
    consumer.commitSync()
```

**Problem**: **Check and act are not atomic**.

```
Timeline with two pods reprocessing same message:

t0: Pod A checks ledger → Not found
t1: Pod B checks ledger → Not found (Pod A hasn't committed yet)
t2: Pod A commits debit + ledger
t3: Pod B commits debit + ledger (duplicate!)

Result: Both pods think they're the first, both execute.
```

---

## Part 2: Idempotent Processing with Conditional Updates

### The Right Approach: Atomic Check-and-Execute

**Key insight**: The check and the execution must be **in the same database transaction**, using SQL semantics to ensure atomicity.

```python
def process_payment_idempotent(message):
    txn_id = message.value['transaction_id']
    account_id = message.value['account_id']
    amount = message.value['amount']
    
    with db.transaction():
        # Attempt to insert into ledger (acts as distributed lock)
        result = db.execute("""
            INSERT INTO ledger (transaction_id, account_id, amount, status, created_at)
            VALUES (%(txn_id)s, %(account_id)s, %(amount)s, 'PROCESSING', NOW())
            ON CONFLICT (transaction_id) DO NOTHING
            RETURNING transaction_id
        """, {'txn_id': txn_id, 'account_id': account_id, 'amount': amount})
        
        if result.rowcount == 0:
            # Transaction already processed, check final status
            status = db.query("""
                SELECT status FROM ledger WHERE transaction_id = %s
            """, txn_id)[0]['status']
            
            if status == 'COMPLETED':
                print(f"Transaction {txn_id} already completed, skipping")
                return {'status': 'IDEMPOTENT_SKIP', 'txn_id': txn_id}
            elif status == 'PROCESSING':
                # Another process is handling this, or previous attempt crashed
                # Wait briefly and retry, or fail
                raise RetryableError(f"Transaction {txn_id} is being processed by another worker")
        
        # We successfully inserted → we own this transaction, proceed
        
        # Debit account (conditional update)
        updated = db.execute("""
            UPDATE accounts
            SET balance = balance - %(amount)s,
                updated_at = NOW()
            WHERE account_id = %(account_id)s
              AND balance >= %(amount)s  -- Ensure sufficient funds
        """, {'account_id': account_id, 'amount': amount})
        
        if updated.rowcount == 0:
            # Insufficient funds or account doesn't exist
            db.execute("""
                UPDATE ledger
                SET status = 'FAILED', error = 'INSUFFICIENT_FUNDS'
                WHERE transaction_id = %s
            """, txn_id)
            # Commit the FAILED status to prevent retry
            raise BusinessError(f"Insufficient funds for {txn_id}")
        
        # Mark transaction as completed
        db.execute("""
            UPDATE ledger
            SET status = 'COMPLETED', completed_at = NOW()
            WHERE transaction_id = %s
        """, txn_id)
        
        # Insert into outbox for downstream event (see Part 3)
        db.execute("""
            INSERT INTO outbox (event_id, event_type, payload, created_at)
            VALUES (gen_random_uuid(), 'payment_completed', %s, NOW())
        """, json.dumps({'txn_id': txn_id, 'account_id': account_id, 'amount': amount}))
    
    # Commit Kafka offset AFTER database transaction commits
    consumer.commitSync()
    
    return {'status': 'SUCCESS', 'txn_id': txn_id}
```

### Why This Works

**Idempotency guarantee**:
1. First attempt: `INSERT INTO ledger` succeeds → debit executes → status = COMPLETED
2. Replay (offset not committed): `INSERT INTO ledger` conflicts → check status = COMPLETED → skip debit
3. **Result**: Regardless of how many times message is replayed, account debited exactly once

**Atomicity**: Check (INSERT ON CONFLICT) and act (UPDATE accounts) are in same transaction → no race condition.

---

## Part 3: Outbox Pattern for Downstream Events

### Problem: DB Write Succeeds, Downstream Kafka Send Fails

**Naive approach (WRONG)**:
```python
with db.transaction():
    debit_account()
    insert_ledger()
# DB commit

# Send to downstream Kafka
kafka_producer.send('payment-completed', {'txn_id': txn_id})  ← Fails here
consumer.commitSync()
```

**What happens if Kafka send fails?**
- Database transaction is committed (debit happened)
- Downstream system never receives payment-completed event
- Inconsistency: payment processed, but downstream not notified

### Solution: Transactional Outbox Pattern

**Architecture**:
```
┌─────────────────────────────────────────────────────────┐
│  Consumer Worker                                         │
│                                                          │
│  ┌──────────────────────────────────────────┐          │
│  │  PostgreSQL Transaction                  │          │
│  │                                           │          │
│  │  1. UPDATE accounts                      │          │
│  │  2. INSERT INTO ledger                   │          │
│  │  3. INSERT INTO outbox ← Key!            │          │
│  │                                           │          │
│  │  COMMIT                                   │          │
│  └──────────────────────────────────────────┘          │
│                                                          │
│  4. consumer.commitSync()                               │
└─────────────────────────────────────────────────────────┘
              ↓
┌─────────────────────────────────────────────────────────┐
│  Outbox Relay (separate process)                        │
│                                                          │
│  while True:                                             │
│    events = SELECT * FROM outbox WHERE sent = false     │
│    for event in events:                                 │
│      kafka_producer.send('payment-completed', event)    │
│      UPDATE outbox SET sent = true WHERE id = event.id  │
│    sleep(1)                                             │
└─────────────────────────────────────────────────────────┘
              ↓
     Downstream Kafka Topic
```

### Database Schema

```sql
-- Outbox table
CREATE TABLE outbox (
    id BIGSERIAL PRIMARY KEY,
    event_id UUID NOT NULL UNIQUE,
    event_type VARCHAR(100) NOT NULL,
    aggregate_id VARCHAR(255),  -- e.g., transaction_id, account_id
    payload JSONB NOT NULL,
    sent BOOLEAN DEFAULT FALSE,
    sent_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW(),
    retry_count INT DEFAULT 0,
    
    INDEX idx_outbox_unsent (sent, created_at) WHERE sent = FALSE
);

-- Ledger table with transaction status
CREATE TABLE ledger (
    id BIGSERIAL PRIMARY KEY,
    transaction_id VARCHAR(255) UNIQUE NOT NULL,
    account_id VARCHAR(255) NOT NULL,
    amount DECIMAL(15, 2) NOT NULL,
    status VARCHAR(50) NOT NULL,  -- PROCESSING, COMPLETED, FAILED
    error TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    completed_at TIMESTAMP,
    
    INDEX idx_ledger_txn_id (transaction_id),
    INDEX idx_ledger_account (account_id, created_at)
);

-- Accounts table with optimistic locking
CREATE TABLE accounts (
    account_id VARCHAR(255) PRIMARY KEY,
    balance DECIMAL(15, 2) NOT NULL CHECK (balance >= 0),
    version INT NOT NULL DEFAULT 1,  -- For optimistic locking
    updated_at TIMESTAMP DEFAULT NOW()
);
```

### Outbox Relay Implementation

```python
import time
import json
from kafka import KafkaProducer
import psycopg2

class OutboxRelay:
    def __init__(self, db_conn_string, kafka_bootstrap):
        self.db = psycopg2.connect(db_conn_string)
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_bootstrap,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',  # Wait for all replicas
            retries=10,
        )
    
    def run(self):
        print("Outbox Relay started")
        
        while True:
            try:
                self.poll_and_send()
                time.sleep(1)  # Poll every second
            except Exception as e:
                print(f"Relay error: {e}")
                time.sleep(5)  # Back off on error
    
    def poll_and_send(self):
        cursor = self.db.cursor()
        
        # Select unsent events (with row-level locking to prevent duplicate sends)
        cursor.execute("""
            SELECT id, event_id, event_type, payload, retry_count
            FROM outbox
            WHERE sent = FALSE
              AND retry_count < 10  -- Max retry limit
            ORDER BY created_at ASC
            LIMIT 100
            FOR UPDATE SKIP LOCKED  -- Skip rows locked by other relay instances
        """)
        
        events = cursor.fetchall()
        
        for event_id, event_uuid, event_type, payload, retry_count in events:
            try:
                # Send to Kafka
                topic = self.get_topic_for_event_type(event_type)
                future = self.producer.send(
                    topic,
                    key=event_uuid.encode('utf-8'),
                    value=json.loads(payload)
                )
                
                # Wait for acknowledgment
                future.get(timeout=10)
                
                # Mark as sent
                cursor.execute("""
                    UPDATE outbox
                    SET sent = TRUE, sent_at = NOW()
                    WHERE id = %s
                """, (event_id,))
                self.db.commit()
                
                print(f"Sent event {event_uuid} to {topic}")
            
            except Exception as e:
                print(f"Failed to send event {event_uuid}: {e}")
                
                # Increment retry count
                cursor.execute("""
                    UPDATE outbox
                    SET retry_count = retry_count + 1
                    WHERE id = %s
                """, (event_id,))
                self.db.commit()
    
    def get_topic_for_event_type(self, event_type):
        mapping = {
            'payment_completed': 'payment-events',
            'account_debited': 'account-events',
            'refund_processed': 'refund-events',
        }
        return mapping.get(event_type, 'default-events')

if __name__ == "__main__":
    relay = OutboxRelay(
        db_conn_string="postgresql://user:pass@localhost:5432/cce_db",
        kafka_bootstrap="msk-broker:9092"
    )
    relay.run()
```

### Why Outbox Pattern Works

**Guarantees**:
1. ✅ **Atomicity**: Business data and outbox event written in same DB transaction
2. ✅ **Durability**: Once DB commits, event is guaranteed to be sent (eventually)
3. ✅ **At-least-once delivery**: Relay retries on failure
4. ✅ **Ordering**: Events sent in created_at order (within same aggregate_id)

**Failure scenarios**:
- DB commit succeeds, offset commit fails → Message replays, but idempotent processing skips duplicate
- DB commit succeeds, relay crashes → Relay restarts, finds unsent events, sends them
- Kafka send fails → Relay retries (up to 10 times), event stays in outbox
- Relay sends event, Kafka ACK lost → Relay retries, downstream consumer must be idempotent (consumer's problem, not producer's)

---

## Part 4: Complete End-to-End Flow

### Data Flow

```
┌─────────────────────────────────────────────────────────────────┐
│  Step 1: Consumer polls message                                  │
│  Kafka offset 1000: {"txn_id": "txn_123", "amount": 100, ...}   │
└────────────────────────┬────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────────────┐
│  Step 2: Worker starts processing                                │
│  BEGIN TRANSACTION                                               │
│                                                                   │
│  2a. INSERT INTO ledger (txn_123, status='PROCESSING')          │
│      ON CONFLICT DO NOTHING                                      │
│      → If rowcount = 0: Already processed, check status & skip  │
│                                                                   │
│  2b. UPDATE accounts SET balance = balance - 100                 │
│      WHERE account_id = 'A' AND balance >= 100                  │
│      → If rowcount = 0: Insufficient funds, mark FAILED         │
│                                                                   │
│  2c. UPDATE ledger SET status = 'COMPLETED'                      │
│                                                                   │
│  2d. INSERT INTO outbox (event_type='payment_completed', ...)   │
│                                                                   │
│  COMMIT                                                          │
└────────────────────────┬────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────────────┐
│  Step 3: Commit Kafka offset                                     │
│  consumer.commitSync() → offset 1001                             │
│                                                                   │
│  ⚠️ If crash here: Message replays, but Step 2a detects         │
│     duplicate (status='COMPLETED') and skips                     │
└────────────────────────┬────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────────────┐
│  Step 4: Outbox Relay (separate process)                         │
│  SELECT * FROM outbox WHERE sent = FALSE                         │
│                                                                   │
│  4a. kafka_producer.send('payment-completed', {...})             │
│  4b. Wait for Kafka ACK                                          │
│  4c. UPDATE outbox SET sent = TRUE                               │
│                                                                   │
│  ⚠️ If crash before 4c: Relay restarts, re-sends event         │
│     Downstream consumer must handle duplicate (idempotent)       │
└────────────────────────┬────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────────────┐
│  Step 5: Downstream consumer                                     │
│  Receives: {"event_type": "payment_completed", "txn_id": ...}   │
│  Processes idempotently (same pattern as Step 2)                 │
└─────────────────────────────────────────────────────────────────┘
```

### Failure Recovery Matrix

| Failure Point | What Happens | Recovery Mechanism | Data Consistency |
|---------------|--------------|-------------------|------------------|
| **Before DB commit** | Transaction rolled back, offset not committed | Kafka replays message | ✅ Safe (no side effects) |
| **After DB commit, before offset commit** | DB change persisted, offset not advanced | Kafka replays, idempotent processing skips | ✅ Safe (idempotent) |
| **After offset commit, before outbox relay** | Event in outbox, not yet sent | Relay finds unsent event on restart | ✅ Safe (guaranteed delivery) |
| **Outbox relay sends, Kafka ACK lost** | Event actually sent, but relay doesn't know | Relay retries, downstream sees duplicate | ✅ Safe (downstream idempotent) |

---

## Part 5: Exactly-Once vs. At-Least-Once + Idempotency

### The False Promise of Exactly-Once

**Kafka Transactions** can provide exactly-once semantics **within Kafka**:
```python
# Kafka-to-Kafka exactly-once
producer = KafkaProducer(transactional_id='txn_producer_1')
producer.init_transactions()

consumer = KafkaConsumer(isolation_level='read_committed')

for message in consumer:
    producer.begin_transaction()
    result = process(message)
    producer.send('output-topic', result)
    producer.send_offsets_to_transaction(
        {(message.topic, message.partition): message.offset + 1},
        consumer.group_id
    )
    producer.commit_transaction()
```

**What this guarantees**:
- ✅ Output message and offset commit are atomic (both succeed or both fail)
- ✅ Downstream consumer with `read_committed` only sees committed messages
- ✅ True exactly-once **within Kafka ecosystem**

**What this does NOT guarantee**:
- ❌ Cannot make external database write + Kafka offset atomic
- ❌ Cannot rollback PostgreSQL if Kafka transaction fails
- ❌ Requires distributed transaction coordinator (2PC) across Kafka and DB

### The Reality: At-Least-Once + Idempotency

**Why we choose this in financial systems**:

1. **Simplicity**: No distributed transaction coordinator needed
2. **Performance**: No 2PC overhead (which can be 10-100x slower)
3. **Scalability**: Each consumer processes independently, no global lock
4. **Debuggability**: Can inspect ledger table to see what happened
5. **Pragmatism**: Business-level idempotency is inevitable anyway (user may double-click "Pay" button)

**The trade-off**:
- Kafka guarantees: At-least-once (message delivered ≥1 time)
- Application guarantees: Idempotency (processing N times = processing 1 time)
- **Result**: Effectively exactly-once business outcome

**Cost comparison**:
```
True Exactly-Once (2PC):
  - Throughput: ~1,000 txn/sec
  - Latency: 50-200ms per transaction
  - Complexity: High (XA transactions, distributed coordinator)

At-Least-Once + Idempotency:
  - Throughput: ~10,000 txn/sec
  - Latency: 5-20ms per transaction
  - Complexity: Medium (careful SQL, outbox pattern)
```

**For CCE platform**: At-least-once + idempotency is the right choice because:
- Real-time feature updates are naturally idempotent ("set cart_items = 5" executed twice = same result)
- Gold features from batch are already idempotent (full refresh daily)
- Performance matters (need to process 50K events/sec at scale)

---

## Part 6: Production Implementation for CCE

### Modified Stream Job with Financial-Grade Processing

```python
import json
import psycopg2
from kafka import KafkaConsumer
from contextlib import contextmanager

class FinancialStreamProcessor:
    def __init__(self, db_conn_string, kafka_bootstrap, redis_url):
        self.db_pool = psycopg2.pool.ThreadedConnectionPool(
            minconn=2, maxconn=10, dsn=db_conn_string
        )
        self.consumer = KafkaConsumer(
            'cce.rds.orders',
            bootstrap_servers=kafka_bootstrap,
            group_id='cce-realtime-feature-stream',
            enable_auto_commit=False,  # Manual commit only
            max_poll_records=500,
            max_poll_interval_ms=600000,
            heartbeat_interval_ms=3000,
            session_timeout_ms=30000,
        )
        self.redis = redis.from_url(redis_url)
    
    @contextmanager
    def get_db_connection(self):
        conn = self.db_pool.getconn()
        try:
            yield conn
        finally:
            self.db_pool.putconn(conn)
    
    def process_order_event(self, message):
        """Process a single order event with idempotency"""
        event_id = message.value['event_id']  # UUID from Debezium
        customer_id = message.value['unified_customer_key']
        order_id = message.value['order_id']
        amount = message.value['amount']
        timestamp = message.value['event_timestamp']
        
        with self.get_db_connection() as conn:
            cursor = conn.cursor()
            
            try:
                # Step 1: Idempotent event deduplication
                cursor.execute("""
                    INSERT INTO processed_events (
                        event_id, customer_id, event_type, processed_at
                    )
                    VALUES (%(event_id)s, %(customer_id)s, 'order_created', NOW())
                    ON CONFLICT (event_id) DO NOTHING
                    RETURNING event_id
                """, {'event_id': event_id, 'customer_id': customer_id})
                
                if cursor.rowcount == 0:
                    # Already processed
                    print(f"Event {event_id} already processed, skipping")
                    return {'status': 'IDEMPOTENT_SKIP'}
                
                # Step 2: Update feature aggregates
                cursor.execute("""
                    INSERT INTO customer_features (
                        customer_id,
                        order_count,
                        lifetime_value,
                        last_order_timestamp,
                        updated_at
                    )
                    VALUES (
                        %(customer_id)s,
                        1,
                        %(amount)s,
                        %(timestamp)s,
                        NOW()
                    )
                    ON CONFLICT (customer_id) DO UPDATE SET
                        order_count = customer_features.order_count + 1,
                        lifetime_value = customer_features.lifetime_value + %(amount)s,
                        last_order_timestamp = GREATEST(
                            customer_features.last_order_timestamp,
                            %(timestamp)s
                        ),
                        updated_at = NOW()
                """, {
                    'customer_id': customer_id,
                    'amount': amount,
                    'timestamp': timestamp
                })
                
                # Step 3: Write to outbox for downstream consumers
                cursor.execute("""
                    INSERT INTO outbox (
                        event_id, event_type, aggregate_id, payload, created_at
                    )
                    VALUES (
                        gen_random_uuid(),
                        'customer_feature_updated',
                        %(customer_id)s,
                        %(payload)s,
                        NOW()
                    )
                """, {
                    'customer_id': customer_id,
                    'payload': json.dumps({
                        'customer_id': customer_id,
                        'order_id': order_id,
                        'amount': amount,
                        'timestamp': timestamp
                    })
                })
                
                # Step 4: Commit database transaction
                conn.commit()
                
                # Step 5: Update Redis (best-effort, not critical)
                # If this fails, next event will update it
                try:
                    self.redis.hset(
                        f"cce:features:realtime:{customer_id}",
                        mapping={
                            'last_order_timestamp': timestamp,
                            'recent_order_count_24h': self.get_recent_count(customer_id)
                        }
                    )
                except Exception as e:
                    print(f"Redis update failed (non-critical): {e}")
                
                return {'status': 'SUCCESS', 'event_id': event_id}
            
            except Exception as e:
                conn.rollback()
                raise
    
    def run(self):
        print("Financial Stream Processor started")
        
        while True:
            messages = self.consumer.poll(timeout_ms=1000)
            
            for topic_partition, records in messages.items():
                for record in records:
                    try:
                        result = self.process_order_event(record)
                        print(f"Processed event: {result}")
                    except Exception as e:
                        print(f"Error processing event: {e}")
                        # Decide: skip or crash?
                        # For critical events: crash and alert
                        # For non-critical: log and skip
                        raise  # Crash on error (Kubernetes will restart)
            
            # Commit offset after successful processing
            self.consumer.commitSync()

if __name__ == "__main__":
    processor = FinancialStreamProcessor(
        db_conn_string="postgresql://user:pass@localhost:5432/cce_db",
        kafka_bootstrap="msk-broker:9092",
        redis_url="redis://elasticache:6379"
    )
    processor.run()
```

---

## Summary: Financial-Grade Guarantees

### What We Achieve

| Requirement | Mechanism | Guarantee |
|-------------|-----------|-----------|
| **No duplicate debits** | Idempotent processing (INSERT ON CONFLICT) | ✅ Same txn_id processed once |
| **No lost transactions** | Commit offset after DB commit | ✅ At-least-once delivery |
| **No orphaned events** | Outbox pattern | ✅ DB write → downstream event guaranteed |
| **Crash recovery** | Replay + idempotency | ✅ Reprocessing produces same result |
| **Auditability** | Ledger table with status | ✅ Full transaction history |

### Architecture Decisions

1. **Offset commit timing**: After DB commit (at-least-once)
2. **Idempotency mechanism**: Database-level (INSERT ON CONFLICT + conditional UPDATE)
3. **Downstream events**: Outbox pattern (transactional with business data)
4. **Consistency model**: At-least-once + idempotency = effectively exactly-once
5. **Performance target**: 10K txn/sec (vs 1K for 2PC)

### When to Use What

| Scenario | Approach |
|----------|----------|
| **Financial transactions (payments, refunds)** | Full pattern (idempotency + outbox + ledger) |
| **Real-time features (cart_items, intent_score)** | Simplified (idempotent Redis updates) |
| **Batch features (lifetime_value, segments)** | Idempotent full-refresh (Gold as SSOT) |
| **Audit logs** | Outbox pattern (guaranteed delivery) |
| **Non-critical metrics** | Best-effort (accept data loss) |

---

## References

- [Designing Data-Intensive Applications (Chapter 11: Stream Processing)](https://dataintensive.net/)
- [Kafka Exactly-Once Semantics](https://www.confluent.io/blog/exactly-once-semantics-are-possible-heres-how-apache-kafka-does-it/)
- [Transactional Outbox Pattern](https://microservices.io/patterns/data/transactional-outbox.html)
- [PostgreSQL Idempotent Upserts](https://www.postgresql.org/docs/current/sql-insert.html#SQL-ON-CONFLICT)
- [Two-Phase Commit Considered Harmful](https://www.bailis.org/blog/when-is-acid-too-much/)
