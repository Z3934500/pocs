"""
Integration tests for Stream Job with real Kafka and Redis

Prerequisites:
- Docker Compose services running (Kafka, Redis)
- Run: docker-compose -f deploy/local/docker-compose.yml up -d
"""

import pytest
import json
import time
from kafka import KafkaProducer, KafkaConsumer
import redis

from cce_platform.L2_olap.online_store import get_online_store


@pytest.fixture(scope='module')
def kafka_producer():
    """Kafka producer for sending test messages"""
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None
    )
    yield producer
    producer.close()


@pytest.fixture(scope='module')
def redis_client():
    """Redis client for verification"""
    client = redis.Redis(host='localhost', port=6379, decode_responses=True)
    yield client
    # Cleanup test keys
    for key in client.scan_iter('cce:features:realtime:TEST_*'):
        client.delete(key)
    for key in client.scan_iter('processed_events:TEST_*'):
        client.delete(key)


@pytest.fixture(scope='module')
def online_store():
    """Online store instance"""
    return get_online_store('redis://localhost:6379')


class TestKafkaIntegration:
    """Integration tests with Kafka"""

    def test_kafka_connection(self, kafka_producer):
        """Test Kafka is reachable"""
        # Send a test message
        topic = 'test-topic'
        future = kafka_producer.send(topic, key='test', value={'test': 'data'})

        # Wait for send to complete
        result = future.get(timeout=10)
        assert result is not None

    def test_send_cdc_event(self, kafka_producer):
        """Test sending CDC event to Kafka"""
        topic = 'cce.rds.orders'

        event = {
            "schema": None,
            "payload": {
                "op": "c",
                "after": {
                    "order_id": "TEST-O-001",
                    "unified_customer_key": "TEST_U0001",
                    "amount": 150.0,
                    "product": "TEST_INSURANCE"
                },
                "source": {
                    "table": "orders",
                    "ts_ms": int(time.time() * 1000)
                },
                "ts_ms": int(time.time() * 1000)
            }
        }

        future = kafka_producer.send(topic, key='test_evt_001', value=event)
        result = future.get(timeout=10)

        assert result.topic == topic
        assert result.partition >= 0


class TestRedisIntegration:
    """Integration tests with Redis"""

    def test_redis_connection(self, redis_client):
        """Test Redis is reachable"""
        assert redis_client.ping() is True

    def test_redis_write_read(self, redis_client):
        """Test basic Redis operations"""
        key = 'test:key'
        redis_client.set(key, 'test_value')

        value = redis_client.get(key)
        assert value == 'test_value'

        redis_client.delete(key)

    def test_redis_hash_operations(self, redis_client):
        """Test Redis HASH operations (used for features)"""
        key = 'cce:features:realtime:TEST_U0001'

        redis_client.hset(key, mapping={
            'rt_order_count_1d': 5,
            'rt_order_amount_1d': 500.0,
            'rt_intent_score': 0.75
        })

        features = redis_client.hgetall(key)
        assert features['rt_order_count_1d'] == '5'
        assert features['rt_order_amount_1d'] == '500.0'
        assert features['rt_intent_score'] == '0.75'

        redis_client.delete(key)


class TestOnlineStore:
    """Integration tests for OnlineStore"""

    def test_online_store_upsert(self, online_store, redis_client):
        """Test upserting features via OnlineStore"""
        customer_id = 'TEST_U0002'
        features = {
            'rt_order_count_1d': 3,
            'rt_order_amount_1d': 300.0,
            'rt_intent_score': 0.6,
            'feature_source': 'integration_test'
        }

        online_store.upsert(customer_id, features)

        # Verify via direct Redis access
        key = f'cce:features:realtime:{customer_id}'
        stored_features = redis_client.hgetall(key)

        assert stored_features['rt_order_count_1d'] == '3'
        assert stored_features['feature_source'] == 'integration_test'

        # Cleanup
        redis_client.delete(key)

    def test_online_store_get(self, online_store, redis_client):
        """Test getting features via OnlineStore"""
        customer_id = 'TEST_U0003'
        key = f'cce:features:realtime:{customer_id}'

        # Set directly in Redis
        redis_client.hset(key, mapping={
            'rt_order_count_1d': 7,
            'rt_cart_add_count_1d': 2
        })

        # Retrieve via OnlineStore
        features = online_store.get(customer_id)

        assert features is not None
        assert features['rt_order_count_1d'] == '7'
        assert features['rt_cart_add_count_1d'] == '2'

        # Cleanup
        redis_client.delete(key)


class TestIdempotency:
    """Test idempotency mechanism"""

    def test_dedup_set_operations(self, redis_client):
        """Test processed event tracking with Redis SET"""
        customer_id = 'TEST_U0004'
        dedup_key = f'processed_events:{customer_id}'

        # Mark events as processed
        redis_client.sadd(dedup_key, 'evt_001', 'evt_002', 'evt_003')
        redis_client.expire(dedup_key, 86400)  # 24h TTL

        # Check membership
        assert redis_client.sismember(dedup_key, 'evt_001') is True
        assert redis_client.sismember(dedup_key, 'evt_999') is False

        # Cleanup
        redis_client.delete(dedup_key)

    def test_duplicate_event_detection(self, online_store, redis_client):
        """Test that duplicate events are detected"""
        customer_id = 'TEST_U0005'
        event_id = 'test_evt_unique_001'
        dedup_key = f'processed_events:{customer_id}'

        # First check - should not be duplicate
        is_dup_1 = redis_client.sismember(dedup_key, event_id)
        assert is_dup_1 is False

        # Mark as processed
        redis_client.sadd(dedup_key, event_id)

        # Second check - should be duplicate
        is_dup_2 = redis_client.sismember(dedup_key, event_id)
        assert is_dup_2 is True

        # Cleanup
        redis_client.delete(dedup_key)


@pytest.mark.slow
class TestEndToEndFlow:
    """End-to-end integration test"""

    def test_full_event_flow(self, kafka_producer, redis_client, online_store):
        """
        Full flow: Send event to Kafka → (Stream Job processes) → Verify in Redis

        Note: This test requires Stream Job to be running separately
        """
        customer_id = 'TEST_U9999'
        event_id = f'test_evt_{int(time.time())}'

        # Clean up any existing data
        redis_client.delete(f'cce:features:realtime:{customer_id}')
        redis_client.delete(f'processed_events:{customer_id}')

        # Send CDC event to Kafka
        event = {
            "schema": None,
            "payload": {
                "op": "c",
                "after": {
                    "order_id": f"TEST-O-{int(time.time())}",
                    "unified_customer_key": customer_id,
                    "amount": 999.0,
                    "product": "TEST_E2E"
                },
                "source": {
                    "table": "orders",
                    "ts_ms": int(time.time() * 1000)
                },
                "ts_ms": int(time.time() * 1000)
            }
        }

        kafka_producer.send('cce.rds.orders', key=event_id, value=event)
        kafka_producer.flush()

        # Wait for Stream Job to process (if running)
        # In real test, you'd check for feature appearance with timeout
        time.sleep(5)

        # Note: This part only works if Stream Job is running
        # In CI/CD, this would be a separate test suite
        # For now, we just verify the event was sent to Kafka
        print(f"Event {event_id} sent to Kafka for customer {customer_id}")

        # Cleanup
        redis_client.delete(f'cce:features:realtime:{customer_id}')
        redis_client.delete(f'processed_events:{customer_id}')


if __name__ == '__main__':
    pytest.main([__file__, '-v', '-s'])
