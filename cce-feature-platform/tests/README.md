# CCE Feature Platform - Test Suite

Comprehensive test suite for the CCE Feature Platform, covering unit, integration, and end-to-end tests.

## Test Structure

```
tests/
├── conftest.py              # Shared fixtures and pytest configuration
├── unit/                    # Unit tests (fast, no external dependencies)
│   ├── test_realtime_stream.py
│   └── test_online_store.py
├── integration/             # Integration tests (require Docker services)
│   └── test_kafka_redis.py
└── e2e/                     # End-to-end tests (full pipeline)
    └── test_stream_pipeline.py
```

## Test Categories

### Unit Tests (`tests/unit/`)

Fast, isolated tests with mocked dependencies.

**Run:**
```bash
pytest tests/unit/ -v
```

**Coverage:**
- `test_realtime_stream.py`: Core stream processing logic
  - CDC event parsing
  - Feature aggregation
  - Idempotency checks
  - Intent score calculation
- `test_online_store.py`: Online store abstraction
  - Redis operations
  - Mock client behavior

**No external dependencies required.**

---

### Integration Tests (`tests/integration/`)

Tests with real Kafka and Redis instances.

**Prerequisites:**
```bash
cd deploy/local
docker-compose up -d
```

**Run:**
```bash
pytest tests/integration/ -v -m integration
```

**Coverage:**
- `test_kafka_redis.py`: Kafka and Redis integration
  - Kafka connection and message sending
  - Redis read/write operations
  - OnlineStore CRUD operations
  - Idempotency SET operations

**Services required:** Kafka, Zookeeper, Redis

---

### End-to-End Tests (`tests/e2e/`)

Full pipeline tests from Kafka ingestion to Redis storage.

**Prerequisites:**
1. Start Docker services:
   ```bash
   cd deploy/local
   docker-compose up -d
   ```

2. Start Stream Job:
   ```bash
   cd ../..
   export PYTHONPATH=src
   python -m cce_platform.L2_olap.realtime_stream_job \
     --kafka-bootstrap localhost:9092 \
     --redis-url redis://localhost:6379
   ```

**Run:**
```bash
pytest tests/e2e/ -v --run-e2e
```

**Coverage:**
- `test_stream_pipeline.py`: Complete stream processing flow
  - Single event processing
  - Duplicate event handling (idempotency)
  - Multiple event aggregation
  - Prometheus metrics validation
  - Throughput testing (100 events)

**Services required:** Kafka, Redis, Stream Job

---

## Quick Start

### 1. Run Unit Tests (Fast)

No setup required:

```bash
pytest tests/unit/ -v
```

### 2. Run Integration Tests

Start Docker services first:

```bash
cd deploy/local
docker-compose up -d
cd ../..
pytest tests/integration/ -v -m integration
```

### 3. Run End-to-End Tests

Start Docker + Stream Job:

```bash
# Terminal 1: Start services
cd deploy/local
docker-compose up -d

# Terminal 2: Start Stream Job
cd ../..
export PYTHONPATH=src
python -m cce_platform.L2_olap.realtime_stream_job \
  --kafka-bootstrap localhost:9092 \
  --redis-url redis://localhost:6379

# Terminal 3: Run tests
pytest tests/e2e/ -v --run-e2e
```

---

## Running All Tests

```bash
# Run all tests (skip slow and e2e by default)
pytest

# Run all including slow tests
pytest --run-slow

# Run all including e2e tests (requires full environment)
pytest --run-e2e --run-slow
```

---

## Test Markers

Tests are categorized with pytest markers:

| Marker | Description | Command |
|--------|-------------|---------|
| `unit` | Fast unit tests | `pytest -m unit` |
| `integration` | Requires Docker services | `pytest -m integration` |
| `e2e` | Requires full pipeline | `pytest -m e2e --run-e2e` |
| `slow` | Takes > 5 seconds | `pytest -m slow --run-slow` |
| `metrics` | Verifies Prometheus metrics | `pytest -m metrics` |
| `performance` | Throughput tests | `pytest -m performance` |

**Examples:**

```bash
# Only integration tests
pytest -m integration -v

# Only fast tests (exclude slow and e2e)
pytest -m "not slow and not e2e" -v

# Only metrics tests
pytest -m metrics -v
```

---

## Test Fixtures

Common fixtures are defined in `conftest.py`:

### Environment Fixtures
- `test_env`: Test environment configuration (Kafka, Redis URLs)

### Data Fixtures
- `sample_cdc_order_event`: Sample Debezium CDC order event
- `sample_cdc_cart_event`: Sample Debezium CDC cart event
- `sample_feature_set`: Sample feature dictionary

### Service Fixtures (Integration/E2E)
- `kafka_producer`: Kafka producer instance
- `redis_client`: Redis client instance
- `online_store`: OnlineStore instance

**Usage:**
```python
def test_something(sample_cdc_order_event, redis_client):
    # Fixtures injected automatically
    event = CdcEvent.from_dict(sample_cdc_order_event)
    redis_client.hset('test_key', mapping={'field': 'value'})
```

---

## Coverage Report

Generate test coverage report:

```bash
# Install coverage
pip install pytest-cov

# Run with coverage
pytest --cov=src/cce_platform --cov-report=html

# Open report
open htmlcov/index.html  # macOS
# or
start htmlcov/index.html  # Windows
```

---

## CI/CD Integration

### GitHub Actions Example

```yaml
name: Test

on: [push, pull_request]

jobs:
  unit-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.11'
      - name: Install dependencies
        run: |
          pip install -r requirements.txt
          pip install pytest pytest-cov
      - name: Run unit tests
        run: pytest tests/unit/ -v --cov

  integration-tests:
    runs-on: ubuntu-latest
    services:
      kafka:
        image: confluentinc/cp-kafka:7.5.0
      redis:
        image: redis:7-alpine
    steps:
      - uses: actions/checkout@v3
      - name: Run integration tests
        run: pytest tests/integration/ -v -m integration

  e2e-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Start services
        run: docker-compose -f deploy/local/docker-compose.yml up -d
      - name: Start Stream Job
        run: |
          export PYTHONPATH=src
          python -m cce_platform.L2_olap.realtime_stream_job &
      - name: Run E2E tests
        run: pytest tests/e2e/ -v --run-e2e
```

---

## Troubleshooting

### Issue: Kafka connection refused

```
kafka.errors.NoBrokersAvailable: NoBrokersAvailable
```

**Solution:**
```bash
# Check Kafka is running
docker-compose -f deploy/local/docker-compose.yml ps

# Restart if needed
docker-compose -f deploy/local/docker-compose.yml restart kafka

# Wait for Kafka to be ready
bash deploy/local/scripts/setup_kafka.sh
```

### Issue: Redis connection refused

```
redis.exceptions.ConnectionError: Error 111 connecting to localhost:6379
```

**Solution:**
```bash
# Check Redis is running
docker-compose -f deploy/local/docker-compose.yml ps redis

# Restart if needed
docker-compose -f deploy/local/docker-compose.yml restart redis
```

### Issue: E2E tests timeout

```
AssertionError: Features not found for E2E_U... after 30s
```

**Solution:**
```bash
# Ensure Stream Job is running
ps aux | grep realtime_stream_job

# Check Stream Job logs for errors
# Check Prometheus metrics
curl http://localhost:9404/metrics | grep messages_processed
```

### Issue: Import errors

```
ModuleNotFoundError: No module named 'cce_platform'
```

**Solution:**
```bash
# Set PYTHONPATH
export PYTHONPATH=src

# Or install in editable mode
pip install -e .
```

---

## Performance Benchmarks

Expected test execution times:

| Test Suite | Time | Tests | Notes |
|------------|------|-------|-------|
| Unit | < 5s | ~20 | Mocked, no I/O |
| Integration | ~30s | ~15 | Docker services |
| E2E | ~90s | ~8 | Full pipeline |
| **Total** | **~2min** | **~43** | All tests |

### Throughput Test Expectations

`test_throughput_100_events`:
- **Input**: 100 CDC events
- **Expected**: < 60 seconds
- **Target Throughput**: > 1.67 events/sec (conservative)
- **Production Target**: 5,000 events/sec per pod

---

## Contributing

When adding new features:

1. **Write unit tests first** (TDD approach)
2. **Add integration tests** if touching Kafka/Redis
3. **Add E2E tests** for critical user flows
4. **Update this README** if adding new test categories

### Test Naming Convention

- `test_<function>_<scenario>`: e.g., `test_process_event_duplicate`
- Use descriptive names that explain what is being tested
- Group related tests in classes: `class TestFeatureProcessor:`

---

## Additional Resources

- **Local Testing Guide**: `docs/testing/TESTING_GUIDE.md`
- **Architecture Decisions**: `docs/ADR/ADR-005-*.md`
- **Project Structure**: `docs/PROJECT_STRUCTURE.md`
