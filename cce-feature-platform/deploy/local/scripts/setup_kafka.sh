#!/bin/bash
# Setup script for Kafka topics and test data

set -e

echo "Waiting for Kafka to be ready..."
sleep 10

echo "Creating Kafka topics..."

# Create topics with proper partitioning
docker exec cce-kafka kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic cce.rds.orders \
  --partitions 6 \
  --replication-factor 1 \
  --if-not-exists \
  --config retention.ms=86400000

docker exec cce-kafka kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic cce.rds.cart_events \
  --partitions 6 \
  --replication-factor 1 \
  --if-not-exists \
  --config retention.ms=86400000

echo "Topics created successfully!"

echo "Listing topics:"
docker exec cce-kafka kafka-topics --list --bootstrap-server localhost:9092

echo ""
echo "Topic details:"
docker exec cce-kafka kafka-topics --describe \
  --bootstrap-server localhost:9092 \
  --topic cce.rds.orders

echo ""
echo "Setup complete! You can now:"
echo "  1. Start the stream job: python -m cce_platform.L2_olap.realtime_stream_job"
echo "  2. Send test messages: ./scripts/send_test_events.sh"
echo "  3. View Kafka UI: http://localhost:8080"
echo "  4. View Prometheus: http://localhost:9090"
echo "  5. View Grafana: http://localhost:3000 (admin/admin)"
