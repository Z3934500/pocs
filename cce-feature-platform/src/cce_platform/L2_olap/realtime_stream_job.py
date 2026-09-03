"""
Production-grade Kafka Stream Job for Real-time Feature Processing

This module implements a production-ready stream processor with:
- Kafka Consumer with proper configuration
- Consumer/Worker thread separation
- Backpressure control (pause/resume)
- Idempotent processing with persistent deduplication
- Prometheus metrics
- Graceful shutdown

Related ADR:
- ADR-005-appendix-backpressure-memory-mgmt.md
- ADR-005-appendix-financial-exactly-once.md
"""

from __future__ import annotations

import json
import logging
import queue
import signal
import socket
import threading
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from kafka import KafkaConsumer, TopicPartition
from prometheus_client import Counter, Gauge, Histogram, start_http_server

from ..L0_configuration import settings
from .online_store import get_online_store
from .pipeline import resolve_unified_key

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger(__name__)

# ============================================================
# Configuration
# ============================================================
MAX_QUEUE_SIZE = 1000
QUEUE_HIGH_WATERMARK = 0.8  # Pause consumer at 80% full
QUEUE_LOW_WATERMARK = 0.3   # Resume consumer at 30% full

WORKER_THREADS = 8
MAX_POLL_RECORDS = 500
MAX_POLL_INTERVAL_MS = 600000  # 10 minutes
HEARTBEAT_INTERVAL_MS = 3000   # 3 seconds
SESSION_TIMEOUT_MS = 30000     # 30 seconds

# Idempotency: Keep processed event IDs for 24 hours
EVENT_ID_TTL_SECONDS = 86400

# ============================================================
# Prometheus Metrics
# ============================================================
queue_depth_gauge = Gauge(
    'kafka_consumer_queue_depth',
    'Current number of messages in the queue'
)
queue_capacity_gauge = Gauge(
    'kafka_consumer_queue_capacity',
    'Maximum queue capacity'
)
messages_processed_counter = Counter(
    'messages_processed_total',
    'Total messages processed successfully'
)
messages_duplicate_counter = Counter(
    'messages_duplicate_total',
    'Total duplicate messages skipped'
)
messages_failed_counter = Counter(
    'messages_failed_total',
    'Total messages that failed processing'
)
processing_time_histogram = Histogram(
    'message_processing_seconds',
    'Time to process one message',
    buckets=[0.01, 0.05, 0.1, 0.5, 1.0, 2.0, 5.0]
)
consumer_paused_gauge = Gauge(
    'kafka_consumer_paused',
    'Consumer pause state (1=paused, 0=running)'
)
consumer_lag_gauge = Gauge(
    'kafka_consumer_lag',
    'Consumer lag per partition',
    ['topic', 'partition']
)

queue_capacity_gauge.set(MAX_QUEUE_SIZE)


# ============================================================
# Data Classes
# ============================================================
@dataclass(frozen=True)
class CdcEvent:
    """CDC event from Debezium"""
    event_id: str
    table: str
    op: str  # c=create, u=update, d=delete
    event_ts: str
    after: dict[str, Any]

    @classmethod
    def from_kafka_message(cls, message) -> CdcEvent:
        """Parse Kafka message into CdcEvent"""
        value = json.loads(message.value.decode('utf-8'))

        # Debezium envelope structure
        payload = value.get('payload', {})
        after = payload.get('after', {})

        return cls(
            event_id=message.key.decode('utf-8') if message.key else str(message.offset),
            table=payload.get('source', {}).get('table', 'unknown'),
            op=payload.get('op', 'c'),
            event_ts=payload.get('ts_ms', int(time.time() * 1000)),
            after=after
        )


# ============================================================
# Feature Processor
# ============================================================
class FeatureProcessor:
    """Process CDC events and update real-time features"""

    def __init__(self, online_store):
        self.online_store = online_store
        # In-memory cache for current session (optional optimization)
        self.session_processed_events: set[str] = set()

    def is_duplicate(self, customer_id: str, event_id: str) -> bool:
        """Check if event already processed (idempotent check)"""
        # Check in-memory cache first (fast)
        if event_id in self.session_processed_events:
            return True

        # Check persistent store (Redis)
        dedup_key = f"processed_events:{customer_id}"
        try:
            # Use Redis SISMEMBER for O(1) lookup
            is_processed = self.online_store.redis_client.sismember(dedup_key, event_id)
            if is_processed:
                return True
        except Exception as e:
            logger.warning(f"Failed to check deduplication for {event_id}: {e}")
            # Fail-safe: if Redis is down, process anyway (at-least-once)
            return False

        return False

    def mark_processed(self, customer_id: str, event_id: str):
        """Mark event as processed for idempotency"""
        dedup_key = f"processed_events:{customer_id}"

        try:
            # Add to Redis set
            self.online_store.redis_client.sadd(dedup_key, event_id)
            # Set TTL (24 hours)
            self.online_store.redis_client.expire(dedup_key, EVENT_ID_TTL_SECONDS)

            # Also add to in-memory cache
            self.session_processed_events.add(event_id)

            # Limit in-memory cache size
            if len(self.session_processed_events) > 10000:
                self.session_processed_events.clear()

        except Exception as e:
            logger.error(f"Failed to mark event {event_id} as processed: {e}")

    def resolve_customer_id(self, payload: dict[str, Any]) -> str | None:
        """Resolve unified customer key from event payload"""
        if payload.get("unified_customer_key"):
            return str(payload["unified_customer_key"])

        id_type = str(payload.get("id_type", ""))
        id_value = str(payload.get("id_value", ""))

        if id_type and id_value:
            return resolve_unified_key(id_type, id_value)

        return None

    def process_event(self, event: CdcEvent) -> dict[str, Any]:
        """
        Process a single CDC event and update real-time features

        Returns:
            dict with status and details
        """
        start_time = time.time()

        try:
            # Resolve customer ID
            customer_id = self.resolve_customer_id(event.after)
            if not customer_id:
                logger.warning(f"Cannot resolve customer for event {event.event_id}")
                return {'status': 'UNRESOLVED', 'event_id': event.event_id}

            # Idempotency check
            if self.is_duplicate(customer_id, event.event_id):
                logger.debug(f"Duplicate event {event.event_id}, skipping")
                messages_duplicate_counter.inc()
                return {'status': 'DUPLICATE', 'event_id': event.event_id, 'customer_id': customer_id}

            # Extract event data
            amount = float(event.after.get("amount", 0) or 0)
            product = str(event.after.get("product", "")).upper()

            # Get current features from Redis
            feature_key = f"cce:features:realtime:{customer_id}"
            current_features = self.online_store.redis_client.hgetall(feature_key)

            # Decode bytes to strings (redis-py returns bytes)
            current_features = {
                k.decode('utf-8') if isinstance(k, bytes) else k:
                v.decode('utf-8') if isinstance(v, bytes) else v
                for k, v in current_features.items()
            }

            # Initialize if not exists
            rt_order_count = int(current_features.get('rt_order_count_1d', 0))
            rt_order_amount = float(current_features.get('rt_order_amount_1d', 0.0))
            rt_cart_count = int(current_features.get('rt_cart_add_count_1d', 0))
            rt_cart_value = float(current_features.get('rt_cart_value_1d', 0.0))

            # Update aggregates based on event type
            if event.table == "orders":
                rt_order_count += 1
                rt_order_amount = round(rt_order_amount + amount, 2)
            elif event.table == "cart_events":
                rt_cart_count += 1
                rt_cart_value = round(rt_cart_value + amount, 2)

            # Compute intent score
            intent_score = min(
                1.0,
                rt_cart_count * 0.18 + rt_order_count * 0.25 + rt_order_amount / 5000
            )

            # Prepare updated features
            updated_features = {
                'rt_order_count_1d': rt_order_count,
                'rt_order_amount_1d': rt_order_amount,
                'rt_cart_add_count_1d': rt_cart_count,
                'rt_cart_value_1d': rt_cart_value,
                'rt_intent_score': round(intent_score, 3),
                'rt_last_event_ts': event.event_ts,
                'rt_last_product': product,
                'feature_source': 'cdc_stream',
                'stream_updated_at': datetime.now(UTC).isoformat(timespec='seconds')
            }

            # Write to Redis (with pipeline for atomicity)
            pipe = self.online_store.redis_client.pipeline()
            pipe.hset(feature_key, mapping=updated_features)
            pipe.expire(feature_key, 86400)  # 24 hour TTL
            pipe.execute()

            # Mark as processed (idempotency)
            self.mark_processed(customer_id, event.event_id)

            # Record metrics
            processing_time = time.time() - start_time
            processing_time_histogram.observe(processing_time)
            messages_processed_counter.inc()

            logger.debug(
                f"Processed event {event.event_id} for {customer_id} "
                f"in {processing_time*1000:.1f}ms"
            )

            return {
                'status': 'SUCCESS',
                'event_id': event.event_id,
                'customer_id': customer_id,
                'processing_time_ms': round(processing_time * 1000, 1)
            }

        except Exception as e:
            logger.error(f"Error processing event {event.event_id}: {e}", exc_info=True)
            messages_failed_counter.inc()
            return {'status': 'ERROR', 'event_id': event.event_id, 'error': str(e)}


# ============================================================
# Stream Job
# ============================================================
class StreamJob:
    """Production-grade Kafka stream job with backpressure control"""

    def __init__(
        self,
        kafka_bootstrap_servers: str,
        kafka_topic: str,
        kafka_group_id: str,
        online_store,
    ):
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.kafka_topic = kafka_topic
        self.kafka_group_id = kafka_group_id
        self.online_store = online_store

        # Shared state
        self.message_queue: queue.Queue = queue.Queue(maxsize=MAX_QUEUE_SIZE)
        self.consumer_paused = False
        self.shutdown_flag = threading.Event()

        # Kafka consumer (created in consumer_thread)
        self.consumer: KafkaConsumer | None = None

        # Feature processor
        self.processor = FeatureProcessor(online_store)

        # Threads
        self.consumer_thread: threading.Thread | None = None
        self.worker_threads: list[threading.Thread] = []

        logger.info(f"StreamJob initialized: topic={kafka_topic}, group={kafka_group_id}")

    def _consumer_loop(self):
        """Consumer thread: poll from Kafka and feed queue"""
        logger.info("Consumer thread starting...")

        # Create Kafka consumer
        self.consumer = KafkaConsumer(
            self.kafka_topic,
            bootstrap_servers=self.kafka_bootstrap_servers,
            group_id=self.kafka_group_id,
            max_poll_records=MAX_POLL_RECORDS,
            max_poll_interval_ms=MAX_POLL_INTERVAL_MS,
            heartbeat_interval_ms=HEARTBEAT_INTERVAL_MS,
            session_timeout_ms=SESSION_TIMEOUT_MS,
            enable_auto_commit=False,  # Manual commit only
            auto_offset_reset='latest',
            key_deserializer=lambda k: k,  # Keep as bytes
            value_deserializer=lambda v: v,  # Keep as bytes
            # Connection settings
            connections_max_idle_ms=300000,
            request_timeout_ms=60000,
        )

        logger.info(f"Consumer connected, assigned partitions: {self.consumer.assignment()}")

        last_commit_time = time.time()
        commit_interval = 30  # Commit every 30 seconds

        while not self.shutdown_flag.is_set():
            try:
                # Backpressure control
                queue_depth = self.message_queue.qsize()
                queue_utilization = queue_depth / MAX_QUEUE_SIZE
                queue_depth_gauge.set(queue_depth)

                if queue_utilization > QUEUE_HIGH_WATERMARK and not self.consumer_paused:
                    logger.warning(
                        f"Queue {queue_utilization:.0%} full, PAUSING consumer "
                        f"({queue_depth}/{MAX_QUEUE_SIZE})"
                    )
                    self.consumer.pause(*self.consumer.assignment())
                    self.consumer_paused = True
                    consumer_paused_gauge.set(1)

                elif queue_utilization < QUEUE_LOW_WATERMARK and self.consumer_paused:
                    logger.info(
                        f"Queue {queue_utilization:.0%} full, RESUMING consumer "
                        f"({queue_depth}/{MAX_QUEUE_SIZE})"
                    )
                    self.consumer.resume(*self.consumer.assignment())
                    self.consumer_paused = False
                    consumer_paused_gauge.set(0)

                # Poll messages
                messages = self.consumer.poll(timeout_ms=1000)

                for topic_partition, records in messages.items():
                    for record in records:
                        try:
                            # Put in queue with timeout
                            self.message_queue.put(record, timeout=5)
                        except queue.Full:
                            logger.error("Queue full despite backpressure, blocking...")
                            self.message_queue.put(record)  # Block until space available

                # Periodic offset commit
                current_time = time.time()
                if current_time - last_commit_time > commit_interval:
                    try:
                        self.consumer.commit()
                        last_commit_time = current_time
                        logger.debug("Committed Kafka offsets")
                    except Exception as e:
                        logger.error(f"Failed to commit offsets: {e}")

                # Update lag metrics
                self._update_lag_metrics()

            except Exception as e:
                logger.error(f"Consumer loop error: {e}", exc_info=True)
                time.sleep(5)  # Back off on error

        # Final commit on shutdown
        try:
            self.consumer.commit()
            logger.info("Final offset commit on shutdown")
        except Exception as e:
            logger.error(f"Failed final offset commit: {e}")

        self.consumer.close()
        logger.info("Consumer thread stopped")

    def _worker_loop(self, worker_id: int):
        """Worker thread: process messages from queue"""
        logger.info(f"Worker {worker_id} starting...")

        while not self.shutdown_flag.is_set():
            try:
                # Get message from queue with timeout
                try:
                    record = self.message_queue.get(timeout=1)
                except queue.Empty:
                    continue

                # Parse CDC event
                try:
                    event = CdcEvent.from_kafka_message(record)
                except Exception as e:
                    logger.error(f"Failed to parse message: {e}")
                    self.message_queue.task_done()
                    continue

                # Process event
                result = self.processor.process_event(event)

                if result['status'] == 'ERROR':
                    logger.error(f"Worker {worker_id} failed to process {event.event_id}")
                    # TODO: Send to dead letter queue
                else:
                    logger.debug(f"Worker {worker_id} processed: {result}")

                self.message_queue.task_done()

            except Exception as e:
                logger.error(f"Worker {worker_id} error: {e}", exc_info=True)

        logger.info(f"Worker {worker_id} stopped")

    def _update_lag_metrics(self):
        """Update consumer lag metrics for Prometheus"""
        try:
            for partition in self.consumer.assignment():
                # Get current position
                position = self.consumer.position(partition)

                # Get high water mark (end offset)
                end_offsets = self.consumer.end_offsets([partition])
                end_offset = end_offsets.get(partition, position)

                # Calculate lag
                lag = end_offset - position

                consumer_lag_gauge.labels(
                    topic=partition.topic,
                    partition=str(partition.partition)
                ).set(lag)
        except Exception as e:
            logger.debug(f"Failed to update lag metrics: {e}")

    def start(self):
        """Start consumer and worker threads"""
        logger.info(f"Starting StreamJob with {WORKER_THREADS} workers...")

        # Start consumer thread
        self.consumer_thread = threading.Thread(
            target=self._consumer_loop,
            name="kafka-consumer",
            daemon=False
        )
        self.consumer_thread.start()

        # Start worker threads
        for i in range(WORKER_THREADS):
            worker = threading.Thread(
                target=self._worker_loop,
                args=(i,),
                name=f"worker-{i}",
                daemon=False
            )
            worker.start()
            self.worker_threads.append(worker)

        logger.info("All threads started")

    def stop(self):
        """Graceful shutdown"""
        logger.info("Shutting down StreamJob...")

        # Signal all threads to stop
        self.shutdown_flag.set()

        # Wait for consumer thread
        if self.consumer_thread:
            self.consumer_thread.join(timeout=30)
            logger.info("Consumer thread stopped")

        # Wait for worker threads
        for worker in self.worker_threads:
            worker.join(timeout=10)
        logger.info("All worker threads stopped")

        logger.info("StreamJob shutdown complete")

    def run_forever(self):
        """Run until interrupted"""
        self.start()

        # Main loop: monitor and log stats
        try:
            while True:
                time.sleep(10)
                queue_size = self.message_queue.qsize()
                processed = messages_processed_counter._value.get()
                duplicates = messages_duplicate_counter._value.get()
                failed = messages_failed_counter._value.get()

                logger.info(
                    f"[STATS] Queue: {queue_size}/{MAX_QUEUE_SIZE}, "
                    f"Paused: {self.consumer_paused}, "
                    f"Processed: {processed}, "
                    f"Duplicates: {duplicates}, "
                    f"Failed: {failed}"
                )
        except KeyboardInterrupt:
            logger.info("Received interrupt signal")
        finally:
            self.stop()


# ============================================================
# Main Entry Point
# ============================================================
def main():
    """Main entry point for production stream job"""
    import argparse

    parser = argparse.ArgumentParser(description="Real-time Feature Stream Job")
    parser.add_argument(
        '--kafka-bootstrap',
        default='localhost:9092',
        help='Kafka bootstrap servers'
    )
    parser.add_argument(
        '--kafka-topic',
        default='cce.rds.orders',
        help='Kafka topic to consume'
    )
    parser.add_argument(
        '--kafka-group',
        default='cce-realtime-feature-stream',
        help='Kafka consumer group ID'
    )
    parser.add_argument(
        '--redis-url',
        default='redis://localhost:6379',
        help='Redis URL for online store'
    )
    parser.add_argument(
        '--metrics-port',
        type=int,
        default=9404,
        help='Prometheus metrics port'
    )
    parser.add_argument(
        '--log-level',
        default='INFO',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        help='Logging level'
    )

    args = parser.parse_args()

    # Set log level
    logging.getLogger().setLevel(getattr(logging, args.log_level))

    # Start Prometheus metrics server
    start_http_server(args.metrics_port)
    logger.info(f"Prometheus metrics server started on port {args.metrics_port}")

    # Get online store
    online_store = get_online_store(redis_url=args.redis_url)
    logger.info(f"Connected to online store: {args.redis_url}")

    # Create and run stream job
    job = StreamJob(
        kafka_bootstrap_servers=args.kafka_bootstrap,
        kafka_topic=args.kafka_topic,
        kafka_group_id=args.kafka_group,
        online_store=online_store,
    )

    # Setup signal handlers for graceful shutdown
    def signal_handler(signum, frame):
        logger.info(f"Received signal {signum}")
        job.stop()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Run forever
    job.run_forever()


if __name__ == "__main__":
    main()
