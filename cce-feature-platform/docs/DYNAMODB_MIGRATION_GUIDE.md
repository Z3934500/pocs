# DynamoDB Migration Guide: Redis to DynamoDB + DAX

This guide covers migrating the CCE Feature Platform from Redis (ElastiCache) to DynamoDB + DAX for online feature serving and transaction state management.

## Table of Contents
- [Architecture Comparison](#architecture-comparison)
- [Prerequisites](#prerequisites)
- [Step 1: Create DynamoDB Tables](#step-1-create-dynamodb-tables)
- [Step 2: Deploy DAX Cluster (Optional)](#step-2-deploy-dax-cluster-optional)
- [Step 3: Migrate Feature Store](#step-3-migrate-feature-store)
- [Step 4: Migrate State Machine](#step-4-migrate-state-machine)
- [Cost Analysis](#cost-analysis)
- [Rollback Plan](#rollback-plan)

## Architecture Comparison

### Current (Redis)
```
Databricks Gold → batch_importer → ElastiCache Redis → Feature API
                                         ↓
                               redis_state_machine.py
                                 (txn:state:* ZSET)
```

### Target (DynamoDB + DAX)
```
Databricks Gold → batch_importer → DynamoDB + DAX → Feature API
                                         ↓
                              dynamodb_state_machine.py
                                (composite key table)
```

## Prerequisites

- AWS CLI configured with appropriate credentials
- IAM permissions for DynamoDB, DAX, and CloudWatch
- Python dependencies: `boto3>=1.26`, `amazon-dax-client>=2.0` (for DAX)

## Step 1: Create DynamoDB Tables

### 1.1 Feature Store Table

```bash
aws dynamodb create-table \
  --table-name cce_features \
  --attribute-definitions \
    AttributeName=unified_customer_key,AttributeType=S \
  --key-schema \
    AttributeName=unified_customer_key,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST \
  --region us-east-1 \
  --tags Key=Project,Value=CCE Key=Environment,Value=production
```

**Table schema:**
```
Primary Key: unified_customer_key (String)
Attributes: 
  - recency_days (Number)
  - tx_count_30d (Number)
  - monetary_30d (Number)
  - segment_name (String)
  - cluster_id (Number)
  - risk_score (Number)
  - feature_source (String)
  - updated_at (String)
  ... (other features)
```

**Recommended: Enable TTL for automatic cleanup** ⭐
```bash
aws dynamodb update-time-to-live \
  --table-name cce_features \
  --time-to-live-specification \
    Enabled=true,AttributeName=ttl
```

**Why TTL is recommended**:
- ✅ **Free deletion**: TTL deletes are free (no WCU consumed)
- ✅ **Zero ops**: Automatic background cleanup
- ✅ **Cost savings**: 30-day old data auto-removed, saves storage costs
- ✅ **AWS best practice**: Recommended for time-series data

**Optional: Enable point-in-time recovery**
```bash
aws dynamodb update-continuous-backups \
  --table-name cce_features \
  --point-in-time-recovery-specification \
    PointInTimeRecoveryEnabled=true
```

### 1.2 Transaction State Table

```bash
aws dynamodb create-table \
  --table-name cce_transaction_state \
  --attribute-definitions \
    AttributeName=txn_id,AttributeType=S \
    AttributeName=timestamp,AttributeType=N \
    AttributeName=state,AttributeType=S \
  --key-schema \
    AttributeName=txn_id,KeyType=HASH \
    AttributeName=timestamp,KeyType=RANGE \
  --billing-mode PAY_PER_REQUEST \
  --global-secondary-indexes \
    IndexName=state-index,KeySchema=[{AttributeName=state,KeyType=HASH},{AttributeName=timestamp,KeyType=RANGE}],Projection={ProjectionType=ALL} \
  --region us-east-1 \
  --tags Key=Project,Value=CCE Key=Environment,Value=production
```

**Table schema:**
```
Primary Key: 
  - txn_id (String) - HASH
  - timestamp (Number) - RANGE (microseconds since epoch)
Attributes:
  - state (String): "PENDING", "APPROVED", "SETTLED", etc.
  - event_id (String): UUID
  - actor (String): "system", "risk_engine", etc.
  - reason (String): transition reason
  - from_state (String): previous state
GSI:
  - state-index: Query all transactions in a given state
```

### 1.3 Transaction Metadata Table

```bash
aws dynamodb create-table \
  --table-name cce_transaction_meta \
  --attribute-definitions \
    AttributeName=txn_id,AttributeType=S \
  --key-schema \
    AttributeName=txn_id,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST \
  --region us-east-1 \
  --tags Key=Project,Value=CCE Key=Environment,Value=production
```

**Table schema:**
```
Primary Key: txn_id (String)
Attributes:
  - product (String): "PREMIUM_FINANCING", "INVESTMENT", etc.
  - amount (String): transaction amount
  - customer_key (String): unified_customer_key
  - created_at (String): ISO timestamp
```

## Step 2: Deploy DAX Cluster (Optional)

DAX provides microsecond-level read latency. Skip this step if your read QPS < 100 or you're optimizing for cost.

### 2.1 Create DAX Subnet Group

```bash
aws dax create-subnet-group \
  --subnet-group-name cce-dax-subnet-group \
  --subnet-ids subnet-12345abc subnet-67890def \
  --description "Subnet group for CCE DAX cluster" \
  --region us-east-1
```

### 2.2 Create IAM Role for DAX

```bash
aws iam create-role \
  --role-name CCE-DAX-Role \
  --assume-role-policy-document '{
    "Version": "2012-10-17",
    "Statement": [{
      "Effect": "Allow",
      "Principal": {"Service": "dax.amazonaws.com"},
      "Action": "sts:AssumeRole"
    }]
  }'

aws iam attach-role-policy \
  --role-name CCE-DAX-Role \
  --policy-arn arn:aws:iam::aws:policy/AmazonDynamoDBFullAccess
```

### 2.3 Create DAX Cluster

```bash
aws dax create-cluster \
  --cluster-name cce-feature-cache \
  --node-type dax.r5.large \
  --replication-factor 3 \
  --iam-role-arn arn:aws:iam::123456789012:role/CCE-DAX-Role \
  --subnet-group-name cce-dax-subnet-group \
  --security-group-ids sg-12345abc \
  --region us-east-1 \
  --tags Key=Project,Value=CCE Key=Environment,Value=production
```

**Wait for cluster to become available:**
```bash
aws dax describe-clusters \
  --cluster-names cce-feature-cache \
  --query 'Clusters[0].Status' \
  --output text
```

**Get DAX endpoint:**
```bash
aws dax describe-clusters \
  --cluster-names cce-feature-cache \
  --query 'Clusters[0].ClusterDiscoveryEndpoint.Address' \
  --output text
```

Example output: `cce-feature-cache.abc123.dax-clusters.us-east-1.amazonaws.com:8111`

## Step 3: Migrate Feature Store

### 3.1 Update Application Configuration

**Environment variables:**
```bash
export CCE_DYNAMODB_TABLE=cce_features
export CCE_DAX_ENDPOINT=cce-feature-cache.abc123.dax-clusters.us-east-1.amazonaws.com:8111
export AWS_REGION=us-east-1
# Keep Redis as fallback during migration
export REDIS_URL=redis://my-redis-cluster.cache.amazonaws.com:6379
```

**Kubernetes ConfigMap:**
```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: cce-config
  namespace: cce-production
data:
  CCE_DYNAMODB_TABLE: "cce_features"
  CCE_DAX_ENDPOINT: "cce-feature-cache.abc123.dax-clusters.us-east-1.amazonaws.com:8111"
  AWS_REGION: "us-east-1"
  CCE_RUNTIME_ENV: "production"
```

### 3.2 Update Code to Add TTL

**Modify batch_importer.py to add TTL**:
```python
# batch_importer.py
import time
from datetime import datetime, timedelta

def publish_to_dynamodb(features: dict, ttl_days: int = 30):
    """Publish features with automatic 30-day TTL"""
    expire_at = datetime.now() + timedelta(days=ttl_days)
    ttl_timestamp = int(expire_at.timestamp())
    
    for customer_key, feature_data in features.items():
        dynamodb_store.upsert(customer_key, {
            **feature_data,
            'ttl': ttl_timestamp,  # Auto-delete after 30 days
            'feature_source': 'batch',
            'updated_at': datetime.now().isoformat()
        })
```

**Modify Flink job to add TTL**:
```python
# flink_cdc_pipeline.py or realtime_stream_job.py
def sink_to_dynamodb(customer_key, features):
    """Write real-time features with TTL"""
    ttl_timestamp = int(time.time()) + 30 * 24 * 3600  # 30 days
    
    dynamodb.put_item(
        TableName='cce_features',
        Item={
            'unified_customer_key': customer_key,
            **features,
            'ttl': ttl_timestamp,  # DynamoDB will auto-delete
            'feature_source': 'realtime',
            'stream_updated_at': datetime.now().isoformat()
        }
    )
```

**Verify TTL in reads** (optional safety check):
```python
# api.py
def get_features(customer_key: str):
    features = dynamodb_store.get(customer_key)
    
    if features:
        # Optional: app-level TTL validation
        ttl = features.get('ttl')
        if ttl and int(ttl) < time.time():
            return None  # Expired, treat as not found
    
    return features
```

### 3.3 Update API Code

**Option A: Switch immediately (all-in)**
```python
# api.py
from .online_store import make_online_store_with_dynamodb

def ensure_online_store():
    return make_online_store_with_dynamodb()
```

**Option B: Dual-write during migration (safer)**
```python
# api.py
from .online_store import make_online_store, make_online_store_with_dynamodb

def ensure_online_store():
    """Return DynamoDB, but keep Redis warm for rollback."""
    dynamodb_store = make_online_store_with_dynamodb()
    redis_store = make_online_store()  # Fallback
    return dynamodb_store
```

### 3.3 Backfill Historical Data

Run the batch importer to populate DynamoDB from SQLite Gold tables:

```bash
# Local test
python -m cce_platform.batch_importer \
  --replace \
  --dynamodb-table cce_features

# Production (from Databricks Gold)
export DATABRICKS_HOST=https://your-workspace.databricks.com
export DATABRICKS_TOKEN=dapi...
export DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/abc123

python -m cce_platform.batch_importer \
  --replace \
  --dynamodb-table cce_features \
  --dax-endpoint cce-feature-cache.abc123.dax-clusters.us-east-1.amazonaws.com:8111
```

### 3.4 Verify Data Migration

```bash
# Count items in DynamoDB
aws dynamodb scan \
  --table-name cce_features \
  --select COUNT \
  --output text

# Spot-check a customer
aws dynamodb get-item \
  --table-name cce_features \
  --key '{"unified_customer_key": {"S": "U0001"}}' \
  --output json
```

### 3.5 Traffic Cutover

1. **Deploy new API version** with DynamoDB backend
2. **Monitor CloudWatch metrics:**
   - `UserErrors` (should be 0)
   - `ConsumedReadCapacityUnits`
   - `ConsumedWriteCapacityUnits`
   - API P99 latency (should be < 10ms with DAX)
3. **Run smoke tests:**
   ```bash
   curl http://api.cce.example.com/api/online-features/U0001
   curl http://api.cce.example.com/api/features?segment=HIGH_VALUE
   ```
4. **Wait 24 hours** to ensure stability
5. **Decommission Redis** (see Rollback Plan if issues arise)

## Step 4: Migrate State Machine

### 4.1 Update State Machine Code

```python
# Replace redis_state_machine.TransactionStateMachine with:
from .dynamodb_state_machine import DynamoDBTransactionStateMachine

sm = DynamoDBTransactionStateMachine(
    table_name="cce_transaction_state",
    meta_table_name="cce_transaction_meta",
    region_name="us-east-1",
)
sm.init_transaction("TXN-001", amount=1500.0, product="PREMIUM_FINANCING")
sm.advance("TXN-001", TxnState.RISK_CHECK, actor="risk_engine")
```

### 4.2 Migrate Existing State (If Needed)

**Warning:** Only needed if you have active transactions in Redis. New deployments can skip this.

```python
# migration_script.py
from cce_platform.redis_state_machine import TransactionStateMachine as RedisSM
from cce_platform.dynamodb_state_machine import DynamoDBTransactionStateMachine as DynamoSM

redis_sm = RedisSM(redis_url="redis://...")
dynamo_sm = DynamoSM()

# List all txn_ids from Redis (scan pattern txn:state:*)
for txn_id in redis_txn_ids:
    history = redis_sm.get_history(txn_id)
    meta = redis_sm.get_transaction_meta(txn_id)
    
    # Replay to DynamoDB
    dynamo_sm.init_transaction(
        txn_id=txn_id,
        amount=float(meta["amount"]),
        product=meta["product"],
        customer_key=meta.get("customer_key", ""),
    )
    
    for transition in history[1:]:  # Skip initial PENDING
        dynamo_sm.advance(
            txn_id=txn_id,
            to_state=transition.to_state,
            actor=transition.actor,
            reason=transition.reason,
        )
```

## Cost Analysis

### Baseline: 480K active users, 100 QPS reads, 10 QPS writes, 30-day data retention

| Service | Configuration | Monthly Cost (USD) | Notes |
|---------|--------------|-------------------|-------|
| **Current: ElastiCache Redis** | cache.r7g.large × 2 (multi-AZ) | ~$257 | Must pre-allocate memory for 30 days data |
| **DynamoDB (no DAX)** | On-demand: 260M reads/mo, 26M writes/mo, 28.8 GB storage | ~$77 | **TTL cleanup: $0 (free!)** |
| **DynamoDB + DAX** | On-demand DynamoDB + dax.r5.large × 3 | ~$760 | Micro-second latency |

### DynamoDB Cost Breakdown
```
Writes:  1.44M/month × $1.25/million = $1.80
Reads:   260M/month × $0.25/million = $65.00
Storage: 480K users × 2KB × 30 days = 28.8 GB
         28.8 GB × $0.25/GB-month = $7.20
TTL Deletion: $0 (FREE!)
─────────────────────────────────────
Total: $74.00/month
```

**Key Insight**: DynamoDB with TTL costs **$74/month vs Redis $257/month = 71% savings**

**Decision matrix:**
- **Read QPS < 50, cost-sensitive**: DynamoDB only (~$103/mo, 60% savings)
- **Read QPS 50-200, balanced**: DynamoDB + dax.t3.small × 3 (~$320/mo, +24%)
- **Read QPS > 200, performance-critical**: DynamoDB + dax.r5.large × 3 (~$760/mo, +196%)

**Cost optimization tips:**
1. Use **on-demand billing** during MVP to avoid over-provisioning
2. Switch to **provisioned capacity** once traffic is predictable (30-40% savings)
3. Enable **DynamoDB auto-scaling** for provisioned mode
4. Use **strongly consistent reads** only where required (eventually consistent reads are 50% cheaper)

## Rollback Plan

If issues arise, revert to Redis within 15 minutes:

### Immediate Rollback (< 5 min)

```bash
# 1. Update ConfigMap to disable DynamoDB
kubectl patch configmap cce-config -n cce-production \
  -p '{"data":{"CCE_DYNAMODB_TABLE":""}}'

# 2. Restart API pods to pick up new config
kubectl rollout restart deployment cce-api -n cce-production

# 3. Verify Redis is serving traffic
kubectl logs -f deployment/cce-api -n cce-production | grep "online store: Redis"
```

### Verify Rollback
```bash
curl http://api.cce.example.com/api/health
# Should show: "online_store_backend": "redis"
```

### Post-Rollback Cleanup
- Keep DynamoDB tables for investigation
- Export CloudWatch logs for post-mortem
- Document issues encountered

## Testing Checklist

Before production cutover:

- [ ] Create all DynamoDB tables
- [ ] Verify IAM permissions (describe-table, put-item, query, scan)
- [ ] Backfill feature store from Gold tables
- [ ] Spot-check 10 random customers (compare Redis vs DynamoDB)
- [ ] Load test API with DynamoDB backend (100 QPS for 10 minutes)
- [ ] Verify P99 latency < 10ms (with DAX) or < 50ms (without DAX)
- [ ] Test state machine transitions (init, advance, get_history)
- [ ] Test concurrent writes (optimistic locking works correctly)
- [ ] Test rollback procedure in staging
- [ ] Document runbook for on-call team

## Monitoring

Key CloudWatch metrics to watch:

**DynamoDB:**
- `UserErrors` (should be 0)
- `SystemErrors` (should be 0)
- `ConsumedReadCapacityUnits`
- `ConsumedWriteCapacityUnits`
- `ThrottledRequests` (increase capacity if > 0)

**DAX:**
- `ItemCacheHits` / `ItemCacheMisses` (aim for >90% hit rate)
- `CPUUtilization` (scale up if >70%)
- `RequestCount`

**Application:**
- API P99 latency
- Feature lookup error rate
- State machine transition failures

## References

- [AWS DynamoDB Best Practices](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/best-practices.html)
- [DAX Developer Guide](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DAX.html)
- [CCE Architecture: OLTP Boundary](../docs/ARCHITECTURE_OLTP_BOUNDARY.md)
