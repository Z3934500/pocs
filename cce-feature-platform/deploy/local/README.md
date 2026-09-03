# Local Testing Environment

Docker Compose setup for local development and testing of the CCE Feature Platform.

## What's Included

This environment provides:

- **Apache Kafka** (port 9092): Event streaming platform
- **Zookeeper** (port 2181): Kafka coordination service
- **Redis** (port 6379): Online feature store
- **Kafka UI** (port 8080): Web UI for Kafka management
- **Prometheus** (port 9090): Metrics collection
- **Grafana** (port 3000): Metrics visualization

## Quick Start

### 1. Start Services

```bash
cd deploy/local
docker-compose up -d
```

**Verify services are running:**
```bash
docker-compose ps
```

All services should show `Up` status.

### 2. Setup Kafka Topics

```bash
bash scripts/setup_kafka.sh
```

Creates:
- `cce.rds.orders` (6 partitions)
- `cce.rds.cart_events` (6 partitions)

### 3. Run Stream Job

```bash
cd ../..  # Back to project root
export PYTHONPATH=src
python -m cce_platform.L2_olap.realtime_stream_job \
  --kafka-bootstrap localhost:9092 \
  --redis-url redis://localhost:6379
```

### 4. Send Test Events

In a new terminal:

```bash
cd deploy/local

# Linux/Mac
bash scripts/send_test_events.sh

# Windows
scripts/send_test_events.bat
```

### 5. Verify Processing

**Check Redis:**
```bash
docker exec -it local-redis-1 redis-cli

# In Redis CLI:
KEYS cce:features:realtime:*
HGETALL cce:features:realtime:U0001
SMEMBERS processed_events:U0001
```

**Check Prometheus Metrics:**
```bash
curl http://localhost:9404/metrics | grep messages_processed_total
```

**Check Kafka UI:**

Open http://localhost:8080 and navigate to:
- Topics → `cce.rds.orders` → Messages

---

## Service Details

### Kafka (9092)

**Kafka UI**: http://localhost:8080

**CLI Access:**
```bash
# List topics
docker exec -it local-kafka-1 kafka-topics \
  --bootstrap-server localhost:9092 \
  --list

# Consume messages
docker exec -it local-kafka-1 kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic cce.rds.orders \
  --from-beginning
```

### Redis (6379)

**CLI Access:**
```bash
docker exec -it local-redis-1 redis-cli

# Common commands:
KEYS *                              # List all keys
HGETALL cce:features:realtime:U0001 # Get features for customer
SMEMBERS processed_events:U0001     # Get processed event IDs
INFO stats                          # Redis statistics
```

**Feature Key Structure:**
- **Features**: `cce:features:realtime:<customer_id>` (HASH)
- **Dedup**: `processed_events:<customer_id>` (SET, 24h TTL)

### Prometheus (9090)

**Web UI**: http://localhost:9090

**Query Examples:**
```promql
# Total messages processed
messages_processed_total

# Processing rate (per second)
rate(messages_processed_total[1m])

# Kafka consumer lag
kafka_consumer_lag

# Queue depth
kafka_consumer_queue_depth

# P95 processing latency
histogram_quantile(0.95, rate(message_processing_seconds_bucket[5m]))
```

### Grafana (3000)

**Web UI**: http://localhost:3000
- **Username**: admin
- **Password**: admin

**Pre-configured:**
- Prometheus data source
- Stream Job dashboard (if imported)

**Import Dashboard:**
1. Go to Dashboards → Import
2. Upload `monitoring/grafana-dashboard.json` (if exists)
3. Select Prometheus data source

---

## Scripts

### `scripts/setup_kafka.sh`

Creates Kafka topics with proper configuration:
- 6 partitions (matches target pod count)
- 1 replication factor (local only)
- 24h retention

### `scripts/send_test_events.sh` / `.bat`

Sends 3 test CDC events:
1. **U0001**: Order for INSURANCE ($100)
2. **U0002**: Order for INVESTMENT ($250)
3. **U0001**: Cart add for CARD ($80)

These simulate Debezium CDC events from RDS.

---

## Configuration Files

### `docker-compose.yml`

Main service definitions. Key configurations:

```yaml
kafka:
  environment:
    KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
    KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1

redis:
  command: redis-server --appendonly yes
  volumes:
    - redis-data:/data

prometheus:
  volumes:
    - ./monitoring/prometheus.yml:/etc/prometheus/prometheus.yml
```

### `monitoring/prometheus.yml`

Prometheus scrape configuration:

```yaml
scrape_configs:
  - job_name: 'stream_job'
    static_configs:
      - targets: ['host.docker.internal:9404']
    scrape_interval: 15s
```

**Note**: Uses `host.docker.internal` to reach Stream Job running on host machine.

---

## Testing Workflows

### Manual Testing

```bash
# 1. Start environment
docker-compose up -d

# 2. Setup topics
bash scripts/setup_kafka.sh

# 3. Start Stream Job (Terminal 1)
cd ../..
export PYTHONPATH=src
python -m cce_platform.L2_olap.realtime_stream_job

# 4. Send events (Terminal 2)
cd deploy/local
bash scripts/send_test_events.sh

# 5. Check results (Terminal 3)
docker exec -it local-redis-1 redis-cli
> HGETALL cce:features:realtime:U0001
```

### Automated Testing

```bash
# Unit tests (no services needed)
pytest tests/unit/ -v

# Integration tests (needs Docker)
docker-compose -f deploy/local/docker-compose.yml up -d
pytest tests/integration/ -v -m integration

# E2E tests (needs Docker + Stream Job)
# Terminal 1:
docker-compose -f deploy/local/docker-compose.yml up -d
python -m cce_platform.L2_olap.realtime_stream_job &

# Terminal 2:
pytest tests/e2e/ -v --run-e2e
```

---

## Troubleshooting

### Services Won't Start

```bash
# Check logs
docker-compose logs kafka
docker-compose logs redis

# Common issue: Port already in use
netstat -an | grep 9092  # Check if port is taken

# Solution: Stop conflicting service or change port in docker-compose.yml
```

### Kafka "No Brokers Available"

```bash
# Wait for Kafka to fully start (can take 30-60s)
docker-compose logs -f kafka

# Look for: "Started NetworkTrafficServerConnector"
# Then retry setup_kafka.sh
```

### Redis Connection Refused

```bash
# Check Redis is running
docker-compose ps redis

# Test connection
docker exec -it local-redis-1 redis-cli PING
# Should return: PONG

# If not running:
docker-compose restart redis
```

### Stream Job Can't Connect

**Error**: `kafka.errors.NoBrokersAvailable`

**Solution**:
```bash
# Verify Kafka is accessible
telnet localhost 9092

# Check advertised listeners in docker-compose.yml
# Should be: KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
```

### Prometheus Can't Scrape Metrics

**Issue**: `connection refused` on `host.docker.internal:9404`

**Solution**:
```bash
# Check Stream Job is running with metrics enabled
curl http://localhost:9404/metrics

# If connection refused:
# 1. Ensure Stream Job is running
# 2. Ensure --metrics-port 9404 (default)
# 3. Check firewall allows connections
```

---

## Cleanup

### Stop Services (Keep Data)

```bash
docker-compose stop
```

### Stop and Remove Containers (Keep Volumes)

```bash
docker-compose down
```

### Complete Cleanup (Remove Everything)

```bash
docker-compose down -v  # Remove volumes (deletes data!)
```

---

## Production Differences

This local environment differs from production:

| Component | Local | Production |
|-----------|-------|------------|
| **Kafka** | Single broker, 1 replica | Amazon MSK, 3 brokers, 3 replicas |
| **Redis** | Single instance | ElastiCache cluster mode |
| **Partitions** | 6 partitions | 24+ partitions |
| **Monitoring** | Prometheus + Grafana | CloudWatch + Prometheus |
| **Autoscaling** | Manual | HPA + KEDA |
| **Network** | Localhost | VPC, security groups |

**Do NOT use this setup for production!**

---

## Next Steps

- **Testing**: See `../../tests/README.md`
- **Architecture**: See `../../docs/ADR/ADR-005-*.md`
- **Deployment**: See `../app/k8s/`

---

## Support

For issues:
1. Check logs: `docker-compose logs <service>`
2. Review troubleshooting section above
3. Check main testing guide: `../../docs/testing/TESTING_GUIDE.md`
