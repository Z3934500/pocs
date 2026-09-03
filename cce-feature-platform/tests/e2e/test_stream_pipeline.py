"""
End-to-end tests for the complete stream processing pipeline

Prerequisites:
1. Docker Compose services running
2. Stream Job running: python -m cce_platform.L2_olap.realtime_stream_job

These tests verify the complete flow from Kafka ingestion to Redis storage.
"""

import pytest
import json
import time
import requests
from kafka import KafkaProducer
import redis


@pytest.fixture(scope='module')
def kafka_producer():
    """Kafka producer for E2E tests"""
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None
    )
    yield producer
    producer.close()


@pytest.fixture(scope='module')
def redis_client():
    """Redis client for E2E verification"""
    client = redis.Redis(host='localhost', port=6379, decode_responses=True)
    yield client
    # Cleanup E2E test keys
    for key in client.scan_iter('cce:features:realtime:E2E_*'):
        client.delete(key)
    for key in client.scan_iter('processed_events:E2E_*'):
        client.delete(key)


def wait_for_feature(redis_client, customer_id, timeout=30, check_interval=1):
    """
    Wait for feature to appear in Redis (with timeout)

    Returns: features dict or None if timeout
    """
    start_time = time.time()
    key = f'cce:features:realtime:{customer_id}'

    while time.time() - start_time < timeout:
        features = redis_client.hgetall(key)
        if features:
            return features
        time.sleep(check_interval)

    return None


@pytest.mark.e2e
@pytest.mark.slow
class TestStreamPipelineE2E:
    """End-to-end tests for stream processing pipeline"""

    def test_single_order_processing(self, kafka_producer, redis_client):
        """Test: Single order event → processed → features in Redis"""
        customer_id = f'E2E_U{int(time.time())}'
        event_id = f'e2e_evt_{int(time.time())}'

        # Send order event
        event = {
            "schema": None,
            "payload": {
                "op": "c",
                "after": {
                    "order_id": f"E2E-O-{int(time.time())}",
                    "unified_customer_key": customer_id,
                    "amount": 250.0,
                    "product": "E2E_INSURANCE"
                },
                "source": {
                    "table": "orders",
                    "ts_ms": int(time.time() * 1000)
                }
            }
        }

        kafka_producer.send('cce.rds.orders', key=event_id, value=event)
        kafka_producer.flush()

        # Wait for processing
        features = wait_for_feature(redis_client, customer_id, timeout=30)

        assert features is not None, f"Features not found for {customer_id} after 30s"
        assert features['rt_order_count_1d'] == '1'
        assert features['rt_order_amount_1d'] == '250.0'
        assert features['feature_source'] == 'cdc_stream'
        assert 'rt_intent_score' in features

        # Verify idempotency tracking
        dedup_key = f'processed_events:{customer_id}'
        is_processed = redis_client.sismember(dedup_key, event_id)
        assert is_processed is True

    def test_duplicate_event_idempotency(self, kafka_producer, redis_client):
        """Test: Duplicate event → not double-counted"""
        customer_id = f'E2E_U{int(time.time())}'
        event_id = f'e2e_evt_dup_{int(time.time())}'

        event = {
            "schema": None,
            "payload": {
                "op": "c",
                "after": {
                    "order_id": f"E2E-O-{int(time.time())}",
                    "unified_customer_key": customer_id,
                    "amount": 100.0,
                    "product": "E2E_TEST"
                },
                "source": {
                    "table": "orders",
                    "ts_ms": int(time.time() * 1000)
                }
            }
        }

        # Send event twice
        kafka_producer.send('cce.rds.orders', key=event_id, value=event)
        kafka_producer.send('cce.rds.orders', key=event_id, value=event)
        kafka_producer.flush()

        # Wait for processing
        time.sleep(10)
        features = redis_client.hgetall(f'cce:features:realtime:{customer_id}')

        # Should only be counted once
        assert features['rt_order_count_1d'] == '1'
        assert features['rt_order_amount_1d'] == '100.0'

    def test_multiple_events_aggregation(self, kafka_producer, redis_client):
        """Test: Multiple events → correctly aggregated"""
        customer_id = f'E2E_U{int(time.time())}'
        base_time = int(time.time())

        events = [
            {
                "event_id": f'e2e_evt_{base_time}_1',
                "table": "orders",
                "amount": 100.0,
                "product": "INSURANCE"
            },
            {
                "event_id": f'e2e_evt_{base_time}_2',
                "table": "orders",
                "amount": 200.0,
                "product": "INVESTMENT"
            },
            {
                "event_id": f'e2e_evt_{base_time}_3',
                "table": "cart_events",
                "amount": 50.0,
                "product": "CARD"
            }
        ]

        # Send all events
        for evt in events:
            cdc_event = {
                "schema": None,
                "payload": {
                    "op": "c",
                    "after": {
                        "order_id" if evt["table"] == "orders" else "cart_id": f"E2E-{evt['event_id']}",
                        "unified_customer_key": customer_id,
                        "amount": evt["amount"],
                        "product": evt["product"]
                    },
                    "source": {
                        "table": evt["table"],
                        "ts_ms": int(time.time() * 1000)
                    }
                }
            }
            kafka_producer.send('cce.rds.orders', key=evt["event_id"], value=cdc_event)

        kafka_producer.flush()

        # Wait for all to process
        time.sleep(15)
        features = redis_client.hgetall(f'cce:features:realtime:{customer_id}')

        # Verify aggregates
        assert features['rt_order_count_1d'] == '2'  # 2 orders
        assert features['rt_order_amount_1d'] == '300.0'  # 100 + 200
        assert features['rt_cart_add_count_1d'] == '1'  # 1 cart event
        assert features['rt_cart_value_1d'] == '50.0'

        # Intent score should be > 0
        intent_score = float(features['rt_intent_score'])
        assert intent_score > 0

    @pytest.mark.metrics
    def test_prometheus_metrics_availability(self):
        """Test: Prometheus metrics are exposed"""
        response = requests.get('http://localhost:9404/metrics')

        assert response.status_code == 200
        metrics_text = response.text

        # Check key metrics exist
        assert 'messages_processed_total' in metrics_text
        assert 'kafka_consumer_queue_depth' in metrics_text
        assert 'message_processing_seconds' in metrics_text
        assert 'kafka_consumer_lag' in metrics_text

    @pytest.mark.metrics
    def test_metrics_increment(self, kafka_producer):
        """Test: Metrics increment after processing"""
        # Get initial count
        response1 = requests.get('http://localhost:9404/metrics')
        metrics1 = response1.text

        # Extract messages_processed_total
        for line in metrics1.split('\n'):
            if line.startswith('messages_processed_total '):
                initial_count = float(line.split()[1])
                break

        # Send a test event
        customer_id = f'E2E_U{int(time.time())}'
        event_id = f'e2e_metrics_{int(time.time())}'

        event = {
            "schema": None,
            "payload": {
                "op": "c",
                "after": {
                    "order_id": f"E2E-O-{int(time.time())}",
                    "unified_customer_key": customer_id,
                    "amount": 100.0,
                    "product": "METRICS_TEST"
                },
                "source": {"table": "orders"}
            }
        }

        kafka_producer.send('cce.rds.orders', key=event_id, value=event)
        kafka_producer.flush()

        # Wait for processing
        time.sleep(10)

        # Get updated metrics
        response2 = requests.get('http://localhost:9404/metrics')
        metrics2 = response2.text

        for line in metrics2.split('\n'):
            if line.startswith('messages_processed_total '):
                new_count = float(line.split()[1])
                break

        # Should have incremented
        assert new_count > initial_count


@pytest.mark.e2e
@pytest.mark.slow
@pytest.mark.performance
class TestStreamPerformance:
    """Performance tests for stream processing"""

    def test_throughput_100_events(self, kafka_producer, redis_client):
        """Test: Process 100 events and measure time"""
        base_customer_id = f'E2E_PERF_{int(time.time())}'
        num_events = 100

        start_time = time.time()

        # Send 100 events
        for i in range(num_events):
            event = {
                "schema": None,
                "payload": {
                    "op": "c",
                    "after": {
                        "order_id": f"PERF-O-{i}",
                        "unified_customer_key": f"{base_customer_id}_{i % 10}",  # 10 customers
                        "amount": 100.0,
                        "product": "PERF_TEST"
                    },
                    "source": {"table": "orders"}
                }
            }
            kafka_producer.send('cce.rds.orders', key=f'perf_evt_{i}', value=event)

        kafka_producer.flush()

        # Wait for all to process (check one customer's features)
        features = wait_for_feature(redis_client, f"{base_customer_id}_0", timeout=60)

        end_time = time.time()
        duration = end_time - start_time

        assert features is not None, "Features not found after 60s"

        print(f"\nProcessed {num_events} events in {duration:.2f}s")
        print(f"Throughput: {num_events/duration:.2f} events/sec")

        # Should process 100 events in < 60 seconds (conservative)
        assert duration < 60


if __name__ == '__main__':
    pytest.main([__file__, '-v', '-s', '-m', 'e2e'])
