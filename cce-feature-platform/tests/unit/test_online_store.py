"""
Unit tests for online_store module

Tests the OnlineStore abstraction layer for Redis operations
"""

import pytest
from unittest.mock import Mock, patch, MagicMock

from cce_platform.L2_olap.online_store import (
    get_online_store,
    RedisOnlineStore,
    MockRedisClient
)


class TestGetOnlineStore:
    """Test online store factory function"""

    @patch('cce_platform.L2_olap.online_store.make_online_store')
    def test_get_online_store_redis(self, mock_make_online_store):
        """Test creating Redis online store"""
        mock_store = Mock(spec=RedisOnlineStore)
        mock_client = Mock()
        mock_store._client = mock_client
        mock_make_online_store.return_value = mock_store

        store = get_online_store('redis://localhost:6379')

        assert store == mock_store
        assert store.redis_client == mock_client
        mock_make_online_store.assert_called_once_with(redis_url='redis://localhost:6379')

    @patch('cce_platform.L2_olap.online_store.make_online_store')
    def test_get_online_store_local(self, mock_make_online_store):
        """Test creating local (JSON) online store"""
        mock_store = Mock()
        # Not a RedisOnlineStore
        mock_make_online_store.return_value = mock_store

        store = get_online_store(None)

        assert store == mock_store
        assert isinstance(store.redis_client, MockRedisClient)

    @patch('cce_platform.L2_olap.online_store.make_online_store')
    def test_get_online_store_default(self, mock_make_online_store):
        """Test creating online store with default URL"""
        mock_store = Mock(spec=RedisOnlineStore)
        mock_client = Mock()
        mock_store._client = mock_client
        mock_make_online_store.return_value = mock_store

        store = get_online_store()

        assert store.redis_client is not None


class TestMockRedisClient:
    """Test mock Redis client for local development"""

    @pytest.fixture
    def mock_client(self):
        """Create mock Redis client"""
        return MockRedisClient()

    def test_sismember_empty(self, mock_client):
        """Test checking membership in empty set"""
        result = mock_client.sismember('test_set', 'value1')
        assert result is False

    def test_sadd_and_sismember(self, mock_client):
        """Test adding and checking set membership"""
        mock_client.sadd('test_set', 'value1', 'value2')

        assert mock_client.sismember('test_set', 'value1') is True
        assert mock_client.sismember('test_set', 'value2') is True
        assert mock_client.sismember('test_set', 'value3') is False

    def test_hset_and_hgetall(self, mock_client):
        """Test hash operations"""
        mapping = {
            'field1': 'value1',
            'field2': 'value2',
            'count': 42
        }
        mock_client.hset('test_hash', mapping=mapping)

        result = mock_client.hgetall('test_hash')

        assert result['field1'] == 'value1'
        assert result['field2'] == 'value2'
        assert result['count'] == 42

    def test_hgetall_empty(self, mock_client):
        """Test getting non-existent hash"""
        result = mock_client.hgetall('nonexistent')
        assert result == {}

    def test_expire(self, mock_client):
        """Test expire operation (no-op in mock)"""
        mock_client.sadd('test_set', 'value1')
        mock_client.expire('test_set', 3600)

        # Should still be accessible (mock doesn't actually expire)
        assert mock_client.sismember('test_set', 'value1') is True

    def test_pipeline(self, mock_client):
        """Test pipeline operations"""
        pipeline = mock_client.pipeline()

        pipeline.hset('hash1', mapping={'field1': 'value1'})
        pipeline.sadd('set1', 'value1')
        pipeline.execute()

        # Verify operations were applied
        assert mock_client.hgetall('hash1')['field1'] == 'value1'
        assert mock_client.sismember('set1', 'value1') is True

    def test_multiple_hashes(self, mock_client):
        """Test multiple independent hashes"""
        mock_client.hset('hash1', mapping={'field': 'value1'})
        mock_client.hset('hash2', mapping={'field': 'value2'})

        assert mock_client.hgetall('hash1')['field'] == 'value1'
        assert mock_client.hgetall('hash2')['field'] == 'value2'

    def test_multiple_sets(self, mock_client):
        """Test multiple independent sets"""
        mock_client.sadd('set1', 'a', 'b')
        mock_client.sadd('set2', 'x', 'y')

        assert mock_client.sismember('set1', 'a') is True
        assert mock_client.sismember('set1', 'x') is False
        assert mock_client.sismember('set2', 'x') is True
        assert mock_client.sismember('set2', 'a') is False


class TestRedisOnlineStoreIntegration:
    """Integration-style tests for RedisOnlineStore (with mocked Redis)"""

    @pytest.fixture
    def mock_redis_client(self):
        """Mock Redis client"""
        client = Mock()
        client.hset = Mock()
        client.hgetall = Mock(return_value={})
        client.sismember = Mock(return_value=False)
        client.sadd = Mock()
        client.expire = Mock()

        pipeline = Mock()
        pipeline.hset = Mock()
        pipeline.sadd = Mock()
        pipeline.expire = Mock()
        pipeline.execute = Mock()
        client.pipeline = Mock(return_value=pipeline)

        return client

    @pytest.fixture
    def redis_store(self, mock_redis_client):
        """Create RedisOnlineStore with mocked client"""
        with patch('cce_platform.L2_olap.online_store.make_online_store') as mock_make:
            store = Mock(spec=RedisOnlineStore)
            store._client = mock_redis_client
            store.redis_client = mock_redis_client
            mock_make.return_value = store

            return get_online_store('redis://localhost:6379')

    def test_upsert_operation(self, redis_store, mock_redis_client):
        """Test upserting features to Redis"""
        # Setup store to actually call redis_client methods
        redis_store.upsert = lambda customer_id, features: mock_redis_client.hset(
            f'cce:features:realtime:{customer_id}',
            mapping=features
        )

        features = {
            'rt_order_count_1d': 5,
            'rt_order_amount_1d': 500.0
        }

        redis_store.upsert('U0001', features)

        mock_redis_client.hset.assert_called_once_with(
            'cce:features:realtime:U0001',
            mapping=features
        )

    def test_get_operation(self, redis_store, mock_redis_client):
        """Test getting features from Redis"""
        mock_redis_client.hgetall.return_value = {
            b'rt_order_count_1d': b'5',
            b'rt_order_amount_1d': b'500.0'
        }

        redis_store.get = lambda customer_id: mock_redis_client.hgetall(
            f'cce:features:realtime:{customer_id}'
        )

        features = redis_store.get('U0001')

        assert features[b'rt_order_count_1d'] == b'5'
        mock_redis_client.hgetall.assert_called_once_with('cce:features:realtime:U0001')


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
