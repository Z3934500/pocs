"""
Unit tests for realtime_stream_job module

Tests the core components of the production stream job:
- FeatureProcessor logic
- Event parsing
- Idempotency checks
- Feature aggregation
"""

import pytest
from datetime import datetime
from unittest.mock import Mock, MagicMock, patch
from collections import namedtuple

from cce_platform.L2_olap.realtime_stream_job import (
    CdcEvent,
    FeatureProcessor,
)


# Mock Kafka message
KafkaMessage = namedtuple('KafkaMessage', ['key', 'value', 'offset', 'topic', 'partition'])


class TestCdcEvent:
    """Test CDC event parsing"""

    def test_from_kafka_message_basic(self):
        """Test parsing a basic Debezium CDC message"""
        message = KafkaMessage(
            key=b'evt_001',
            value=b'{"schema":null,"payload":{"op":"c","after":{"order_id":"O-1001","unified_customer_key":"U0001","amount":100.0},"source":{"table":"orders"},"ts_ms":1725350400000}}',
            offset=100,
            topic='cce.rds.orders',
            partition=0
        )

        event = CdcEvent.from_kafka_message(message)

        assert event.event_id == 'evt_001'
        assert event.table == 'orders'
        assert event.op == 'c'
        assert event.after['order_id'] == 'O-1001'
        assert event.after['unified_customer_key'] == 'U0001'
        assert event.after['amount'] == 100.0

    def test_from_kafka_message_no_key(self):
        """Test parsing message without key (uses offset)"""
        message = KafkaMessage(
            key=None,
            value=b'{"schema":null,"payload":{"op":"c","after":{"order_id":"O-1001"},"source":{"table":"orders"}}}',
            offset=123,
            topic='cce.rds.orders',
            partition=0
        )

        event = CdcEvent.from_kafka_message(message)
        assert event.event_id == '123'


class TestFeatureProcessor:
    """Test feature processing logic"""

    @pytest.fixture
    def mock_online_store(self):
        """Create mock online store"""
        store = Mock()
        store.redis_client = Mock()
        return store

    @pytest.fixture
    def processor(self, mock_online_store):
        """Create feature processor instance"""
        return FeatureProcessor(mock_online_store)

    def test_resolve_customer_id_with_unified_key(self, processor):
        """Test customer ID resolution with unified key"""
        payload = {'unified_customer_key': 'U0001'}
        customer_id = processor.resolve_customer_id(payload)
        assert customer_id == 'U0001'

    def test_resolve_customer_id_with_id_type(self, processor):
        """Test customer ID resolution with id_type and id_value"""
        with patch('cce_platform.L2_olap.realtime_stream_job.resolve_unified_key') as mock_resolve:
            mock_resolve.return_value = 'U0002'

            payload = {
                'id_type': 'NRIC',
                'id_value': 'S1234567A'
            }
            customer_id = processor.resolve_customer_id(payload)

            assert customer_id == 'U0002'
            mock_resolve.assert_called_once_with('NRIC', 'S1234567A')

    def test_is_duplicate_new_event(self, processor, mock_online_store):
        """Test duplicate check for new event"""
        mock_online_store.redis_client.sismember.return_value = False

        result = processor.is_duplicate('U0001', 'evt_001')

        assert result is False
        mock_online_store.redis_client.sismember.assert_called_once_with(
            'processed_events:U0001',
            'evt_001'
        )

    def test_is_duplicate_existing_event(self, processor, mock_online_store):
        """Test duplicate check for already processed event"""
        mock_online_store.redis_client.sismember.return_value = True

        result = processor.is_duplicate('U0001', 'evt_001')

        assert result is True

    def test_is_duplicate_redis_failure(self, processor, mock_online_store):
        """Test duplicate check when Redis fails (fail-safe)"""
        mock_online_store.redis_client.sismember.side_effect = Exception("Redis down")

        result = processor.is_duplicate('U0001', 'evt_001')

        # Should return False (process anyway) when Redis fails
        assert result is False

    def test_mark_processed(self, processor, mock_online_store):
        """Test marking event as processed"""
        processor.mark_processed('U0001', 'evt_001')

        mock_online_store.redis_client.sadd.assert_called_once_with(
            'processed_events:U0001',
            'evt_001'
        )
        mock_online_store.redis_client.expire.assert_called_once_with(
            'processed_events:U0001',
            86400  # 24 hours
        )

    def test_process_event_new_order(self, processor, mock_online_store):
        """Test processing a new order event"""
        # Mock Redis responses
        mock_online_store.redis_client.hgetall.return_value = {}
        mock_online_store.redis_client.sismember.return_value = False
        mock_pipeline = Mock()
        mock_online_store.redis_client.pipeline.return_value = mock_pipeline

        event = CdcEvent(
            event_id='evt_001',
            table='orders',
            op='c',
            event_ts='2026-09-03T10:00:00',
            after={
                'unified_customer_key': 'U0001',
                'order_id': 'O-1001',
                'amount': 100.0,
                'product': 'INSURANCE'
            }
        )

        result = processor.process_event(event)

        assert result['status'] == 'SUCCESS'
        assert result['customer_id'] == 'U0001'

        # Verify Redis pipeline was used
        mock_pipeline.hset.assert_called_once()
        call_args = mock_pipeline.hset.call_args
        assert call_args[0][0] == 'cce:features:realtime:U0001'

        features = call_args[1]['mapping']
        assert features['rt_order_count_1d'] == 1
        assert features['rt_order_amount_1d'] == 100.0
        assert features['rt_last_product'] == 'INSURANCE'

    def test_process_event_duplicate(self, processor, mock_online_store):
        """Test processing a duplicate event (idempotency)"""
        mock_online_store.redis_client.sismember.return_value = True

        event = CdcEvent(
            event_id='evt_001',
            table='orders',
            op='c',
            event_ts='2026-09-03T10:00:00',
            after={'unified_customer_key': 'U0001', 'amount': 100.0}
        )

        result = processor.process_event(event)

        assert result['status'] == 'DUPLICATE'
        assert result['customer_id'] == 'U0001'

        # Should not write to Redis
        mock_online_store.redis_client.pipeline.assert_not_called()

    def test_process_event_incremental_update(self, processor, mock_online_store):
        """Test incremental feature update"""
        # Existing features
        mock_online_store.redis_client.hgetall.return_value = {
            b'rt_order_count_1d': b'2',
            b'rt_order_amount_1d': b'200.0',
            b'rt_cart_add_count_1d': b'0',
            b'rt_cart_value_1d': b'0.0'
        }
        mock_online_store.redis_client.sismember.return_value = False
        mock_pipeline = Mock()
        mock_online_store.redis_client.pipeline.return_value = mock_pipeline

        event = CdcEvent(
            event_id='evt_002',
            table='orders',
            op='c',
            event_ts='2026-09-03T11:00:00',
            after={
                'unified_customer_key': 'U0001',
                'order_id': 'O-1002',
                'amount': 50.0,
                'product': 'SAVINGS'
            }
        )

        result = processor.process_event(event)

        assert result['status'] == 'SUCCESS'

        features = mock_pipeline.hset.call_args[1]['mapping']
        assert features['rt_order_count_1d'] == 3  # 2 + 1
        assert features['rt_order_amount_1d'] == 250.0  # 200 + 50

    def test_process_event_cart_event(self, processor, mock_online_store):
        """Test processing cart event"""
        mock_online_store.redis_client.hgetall.return_value = {}
        mock_online_store.redis_client.sismember.return_value = False
        mock_pipeline = Mock()
        mock_online_store.redis_client.pipeline.return_value = mock_pipeline

        event = CdcEvent(
            event_id='evt_003',
            table='cart_events',
            op='c',
            event_ts='2026-09-03T12:00:00',
            after={
                'unified_customer_key': 'U0002',
                'cart_id': 'C-1001',
                'amount': 80.0,
                'product': 'CARD'
            }
        )

        result = processor.process_event(event)

        assert result['status'] == 'SUCCESS'

        features = mock_pipeline.hset.call_args[1]['mapping']
        assert features['rt_cart_add_count_1d'] == 1
        assert features['rt_cart_value_1d'] == 80.0
        assert features['rt_order_count_1d'] == 0  # No orders yet

    def test_process_event_intent_score_calculation(self, processor, mock_online_store):
        """Test intent score calculation"""
        mock_online_store.redis_client.hgetall.return_value = {
            b'rt_order_count_1d': b'5',
            b'rt_order_amount_1d': b'1000.0',
            b'rt_cart_add_count_1d': b'3',
            b'rt_cart_value_1d': b'500.0'
        }
        mock_online_store.redis_client.sismember.return_value = False
        mock_pipeline = Mock()
        mock_online_store.redis_client.pipeline.return_value = mock_pipeline

        event = CdcEvent(
            event_id='evt_004',
            table='cart_events',
            op='c',
            event_ts='2026-09-03T13:00:00',
            after={
                'unified_customer_key': 'U0003',
                'amount': 100.0,
                'product': 'INVESTMENT'
            }
        )

        result = processor.process_event(event)
        features = mock_pipeline.hset.call_args[1]['mapping']

        # intent_score = min(1.0, cart_count*0.18 + order_count*0.25 + amount/5000)
        # = min(1.0, 4*0.18 + 5*0.25 + 1100/5000)
        # = min(1.0, 0.72 + 1.25 + 0.22) = min(1.0, 2.19) = 1.0
        assert features['rt_intent_score'] == 1.0

    def test_process_event_unresolved_customer(self, processor, mock_online_store):
        """Test handling event with unresolved customer"""
        event = CdcEvent(
            event_id='evt_005',
            table='orders',
            op='c',
            event_ts='2026-09-03T14:00:00',
            after={
                'order_id': 'O-1003',
                'amount': 100.0
                # No customer_id fields
            }
        )

        result = processor.process_event(event)

        assert result['status'] == 'UNRESOLVED'
        mock_online_store.redis_client.pipeline.assert_not_called()


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
