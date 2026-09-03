# CCE Stream Job Testing Guide

## Quick Start

### 1. Start Infrastructure

```bash
# Start all services (Kafka, Redis, Prometheus, Grafana)
docker-compose up -d

# Wait for services to be ready (30 seconds)
sleep 30

# Setup Kafka topics
bash scripts/setup_kafka.sh

# Or on Windows:
# docker-compose up -d
# timeout /t 30
# bash scripts/setup_kafka.sh
```

### 2. Verify Services

```bash
# Check service status
docker-compose ps

# Expected output:
# cce-kafka       Up      9092->9092
# cce-redis       Up      6379->6379
# cce-zookeeper   Up      2181->2181
# cce-kafka-ui    Up      8080->8080
# cce-prometheus  Up      9090->9090
# cce-grafana     Up      3000->3000

# Test Redis
docker exec cce-redis redis-cli ping
# Expected: PONG

# Test Kafka
docker exec cce-kafka kafka-broker-api-versions --bootstrap-server localhost:9092
# Should show broker API versions
```

### 3. Install Python Dependencies

```bash
cd cce-feature-platform

# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # Linux/Mac
# Or on Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
pip install kafka-python redis prometheus-client
```

### 4. Run Stream Job

```bash
# Set PYTHONPATH
export PYTHONPATH=src  # Linux/Mac
# Or on Windows: set PYTHONPATH=src

# Run stream job
python -m cce_platform.L2_olap.realtime_stream_job \
  --kafka-bootstrap localhost:9092 \
  --kafka-topic cce.rds.orders \
  --redis-url redis://localhost:6379 \
  --log-level INFO

# Expected output:
# INFO StreamJob initialized: topic=cce.rds.orders
# INFO Consumer thread starting...
# INFO Worker 0 starting...
# ...
# INFO Prometheus metrics server started on port 9404
```

### 5. Send Test Events

In another terminal:

```bash
# Send test CDC events
bash scripts/send_test_events.sh

# Or on Windows:
# scripts\send_test_events.bat
```

### 6. Verify Processing

```bash
# Check stream job logs
# Should see:
# INFO Worker 0 processed: {'status': 'SUCCESS', 'event_id': 'evt_001', ...}

# Check Redis
docker exec cce-redis redis-cli --scan --pattern "cce:features:realtime:*"
# Expected: cce:features:realtime:U0001, cce:features:realtime:U0002

docker exec cce-redis redis-cli HGETALL "cce:features:realtime:U0001"
# Expected: rt_order_count_1d, rt_order_amount_1d, rt_intent_score, etc.

# Check Prometheus metrics
curl http://localhost:9404/metrics | grep messages_processed_total
# Expected: messages_processed_total 3.0
```

---

## Testing Scenarios

### Scenario 1: Idempotency Test (Duplicate Events)

```bash
# Send same event twice
bash scripts/send_test_events.sh
bash scripts/send_test_events.sh

# Check metrics
curl http://localhost:9404/metrics | grep messages_duplicate_total
# Expected: messages_duplicate_total 3.0 (second batch detected as duplicates)

# Verify Redis data not double-counted
docker exec cce-redis redis-cli HGET "cce:features:realtime:U0001" rt_order_count_1d
# Expected: 2 (not 4)
```

### Scenario 2: Backpressure Test (Queue Full)

```bash
# Generate high volume of events
for i in {1..2000}; do
  echo "evt_$i|{\"schema\":null,\"payload\":{\"after\":{\"order_id\":\"O-$i\",\"unified_customer_key\":\"U$((i % 100))\",\"amount\":100.0,\"product\":\"TEST\"}}}" | \
  docker exec -i cce-kafka kafka-console-producer \
    --bootstrap-server localhost:9092 \
    --topic cce.rds.orders \
    --property "parse.key=true" \
    --property "key.separator=|"
done

# Monitor stream job logs
# Should see:
# WARNING Queue 80% full, PAUSING consumer (800/1000)
# INFO Queue 30% full, RESUMING consumer (300/1000)

# Check metrics
curl http://localhost:9404/metrics | grep kafka_consumer_paused
# Expected: kafka_consumer_paused 1.0 (if still paused) or 0.0 (if resumed)
```

### Scenario 3: Consumer Lag Monitoring

```bash
# Check lag via Kafka
docker exec cce-kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe \
  --group cce-realtime-feature-stream

# Expected output:
# TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
# cce.rds.orders  0          100             100             0
# cce.rds.orders  1          98              100             2

# Check lag via Prometheus
curl http://localhost:9404/metrics | grep kafka_consumer_lag
# Expected: kafka_consumer_lag{partition="0",topic="cce.rds.orders"} 0.0
```

### Scenario 4: Graceful Shutdown

```bash
# Send SIGTERM to stream job
kill -TERM <stream_job_pid>

# Or press Ctrl+C

# Check logs
# Expected:
# INFO Received signal 15
# INFO Shutting down StreamJob...
# INFO Final offset commit on shutdown
# INFO Consumer thread stopped
# INFO All worker threads stopped
# INFO StreamJob shutdown complete
```

---

## Monitoring Dashboards

### Kafka UI
- URL: http://localhost:8080
- View topics, consumer groups, lag

### Prometheus
- URL: http://localhost:9090
- Query examples:
  ```promql
  # Processing rate
  rate(messages_processed_total[1m])
  
  # Queue depth
  kafka_consumer_queue_depth
  
  # P99 latency
  histogram_quantile(0.99, rate(message_processing_seconds_bucket[5m]))
  
  # Consumer lag
  kafka_consumer_lag{topic="cce.rds.orders"}
  ```

### Grafana
- URL: http://localhost:3000
- Username: admin
- Password: admin
- Add Prometheus datasource (already configured)
- Import dashboard (JSON provided in monitoring/grafana-dashboard.json)

---

## Troubleshooting

### Issue: Stream job can't connect to Kafka

```bash
# Check Kafka is running
docker exec cce-kafka kafka-broker-api-versions --bootstrap-server localhost:9092

# Check network
docker network inspect cce-network

# Try from host
telnet localhost 9092
```

### Issue: Redis connection timeout

```bash
# Check Redis is running
docker exec cce-redis redis-cli ping

# Check from host
redis-cli -h localhost -p 6379 ping
```

### Issue: No messages consumed

```bash
# Check topic exists
docker exec cce-kafka kafka-topics --list --bootstrap-server localhost:9092

# Check messages in topic
docker exec cce-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic cce.rds.orders \
  --from-beginning \
  --max-messages 10

# Check consumer group
docker exec cce-kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe \
  --group cce-realtime-feature-stream
```

### Issue: High memory usage

```bash
# Check queue depth
curl http://localhost:9404/metrics | grep kafka_consumer_queue_depth

# Check GC
# Add to stream job startup: -Xmx2g -XX:+PrintGCDetails

# Monitor Docker stats
docker stats cce-kafka cce-redis
```

---

## Cleanup

```bash
# Stop stream job (Ctrl+C)

# Stop all services
docker-compose down

# Remove volumes (clean slate)
docker-compose down -v

# Remove all data
rm -rf monitoring/prometheus-data monitoring/grafana-data
```

---

## Next Steps

1. **Load Testing**: Use tools like `kafka-producer-perf-test` to generate high load
2. **Chaos Testing**: Kill pods mid-processing, verify idempotency
3. **Deploy to Kubernetes**: Use `stream-statefulset.yaml` from `deploy/k8s/`
4. **Enable KEDA**: Deploy Phase 2 autoscaling based on Kafka lag

---

## References

- Docker Compose file: `docker-compose.yml`
- Stream Job code: `src/cce_platform/L2_olap/realtime_stream_job.py`
- ADR Documentation: `docs/ADR/ADR-005-*`
