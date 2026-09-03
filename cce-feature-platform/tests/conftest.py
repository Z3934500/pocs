"""
Pytest configuration and shared fixtures for all tests

This file provides:
- Pytest markers for categorizing tests
- Shared fixtures
- Test environment setup
"""

import pytest
import os


def pytest_configure(config):
    """Register custom markers"""
    config.addinivalue_line(
        "markers", "e2e: End-to-end tests (require full environment)"
    )
    config.addinivalue_line(
        "markers", "slow: Slow-running tests (> 5 seconds)"
    )
    config.addinivalue_line(
        "markers", "integration: Integration tests (require Docker services)"
    )
    config.addinivalue_line(
        "markers", "metrics: Tests that verify Prometheus metrics"
    )
    config.addinivalue_line(
        "markers", "performance: Performance and throughput tests"
    )


@pytest.fixture(scope='session')
def test_env():
    """
    Fixture providing test environment configuration

    Usage:
        def test_something(test_env):
            kafka_url = test_env['kafka_bootstrap']
    """
    return {
        'kafka_bootstrap': os.getenv('KAFKA_BOOTSTRAP', 'localhost:9092'),
        'redis_url': os.getenv('REDIS_URL', 'redis://localhost:6379'),
        'prometheus_url': os.getenv('PROMETHEUS_URL', 'http://localhost:9090'),
        'metrics_port': int(os.getenv('METRICS_PORT', '9404')),
    }


@pytest.fixture
def sample_cdc_order_event():
    """
    Fixture providing a sample CDC order event

    Usage:
        def test_parsing(sample_cdc_order_event):
            event = CdcEvent.from_kafka_message(sample_cdc_order_event)
    """
    return {
        "schema": None,
        "payload": {
            "op": "c",
            "after": {
                "order_id": "O-TEST-001",
                "unified_customer_key": "U_TEST_001",
                "amount": 150.0,
                "product": "INSURANCE",
                "created_at": "2026-09-03T10:00:00Z"
            },
            "source": {
                "table": "orders",
                "db": "cce_platform",
                "ts_ms": 1725350400000
            },
            "ts_ms": 1725350400000
        }
    }


@pytest.fixture
def sample_cdc_cart_event():
    """Fixture providing a sample CDC cart event"""
    return {
        "schema": None,
        "payload": {
            "op": "c",
            "after": {
                "cart_id": "C-TEST-001",
                "unified_customer_key": "U_TEST_001",
                "amount": 80.0,
                "product": "CARD",
                "action": "add",
                "created_at": "2026-09-03T11:00:00Z"
            },
            "source": {
                "table": "cart_events",
                "db": "cce_platform",
                "ts_ms": 1725354000000
            },
            "ts_ms": 1725354000000
        }
    }


@pytest.fixture
def sample_feature_set():
    """Fixture providing a sample feature set"""
    return {
        'rt_order_count_1d': 3,
        'rt_order_amount_1d': 450.0,
        'rt_cart_add_count_1d': 2,
        'rt_cart_value_1d': 150.0,
        'rt_intent_score': 0.75,
        'rt_last_product': 'INSURANCE',
        'feature_source': 'cdc_stream',
        'feature_ts': '2026-09-03T12:00:00Z'
    }


# Pytest command line options
def pytest_addoption(parser):
    """Add custom command line options"""
    parser.addoption(
        "--run-slow",
        action="store_true",
        default=False,
        help="Run slow tests"
    )
    parser.addoption(
        "--run-e2e",
        action="store_true",
        default=False,
        help="Run end-to-end tests (requires full environment)"
    )


def pytest_collection_modifyitems(config, items):
    """Skip tests based on command line options"""
    if not config.getoption("--run-slow"):
        skip_slow = pytest.mark.skip(reason="need --run-slow option to run")
        for item in items:
            if "slow" in item.keywords:
                item.add_marker(skip_slow)

    if not config.getoption("--run-e2e"):
        skip_e2e = pytest.mark.skip(reason="need --run-e2e option to run")
        for item in items:
            if "e2e" in item.keywords:
                item.add_marker(skip_e2e)
