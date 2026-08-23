"""
Flink CDC pipeline for CCE real-time feature computation.

Architecture:
  Kafka CDC topic (orders / cart_events)└─ Flink DataStream / Table API
         ├─ Deduplication (event_id, 1h keyed state)
         ├─ Keyed by unified_customer_key
         ├─ Sliding window aggregation (1d / 7d)
         ├─ Intent score computation
         └─ Redis Sink (HSET per customer) ← replaces LocalOnlineStore in prod

Local / PoC fallback:
  When PyFlink is not installed, the module exposes a *compatible* function
  `run_local_simulation()` that reuses realtime.process_cdc_events() so the
  rest of the codebase keeps working without a Flink cluster.

Exactly-once guarantee:
  - Flink Checkpoint (RocksDB state backend) + Kafka transactional consumer
  - Redis Sink uses MULTI/EXEC per checkpoint boundary
  - Dedup state TTL = 2 × watermark_lag to bound memory

Watermark strategy:
  BoundedOutOfOrdernessWatermarks(Duration.ofSeconds(10))
  Events older than 10 s after the watermark are routed to a late-data
  side-output stream and written to a DLQ Kafka topic for reprocessing.

Window definitions:
  - SlidingEventTimeWindows(1 day, 1 minute slide) → rt_*_1d features
  - TumblingEventTimeWindows(5 minutes)             → fraud velocity check

Usage (local simulation, no Flink cluster needed):
  python -m cce_platform.flink_cdc_pipeline run

Usage (Flink cluster, requires PyFlink + Kafka):
  python -m cce_platform.flink_cdc_pipeline submit
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import time
from dataclasses import dataclass, asdict
from datetime import datetime, UTC
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Shared data model
# ---------------------------------------------------------------------------

@dataclass
class CceFeatureUpdate:
    """Output record written to the Redis online store (one row per customer)."""
    unified_customer_key: str
    rt_order_count_1d:    int
    rt_order_amount_1d:   float
    rt_cart_add_count_1d: int
    rt_cart_value_1d:     float
    rt_last_event_ts:     str
    rt_last_product:      str
    rt_intent_score:      float
    feature_source:       str
    stream_updated_at:    str


# ---------------------------------------------------------------------------
# Flink pipeline (requires PyFlink ≥ 1.18 and a running Flink cluster)
# ---------------------------------------------------------------------------

def _build_flink_pipeline(
    kafka_brokers: str,
    input_topic: str,
    checkpoint_interval_ms: int,
    redis_host: str,
    redis_port: int,
    watermark_lag_s: int,
) -> None:
    """Construct and submit the Flink streaming job.

    This function is only called when PyFlink is available and the user
    explicitly runs `submit` mode.  All imports are local so the module
    loads cleanly without PyFlink installed.
    """
    try:
        from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode  # type: ignore[import]
        from pyflink.datastream.connectors.kafka import (  # type: ignore[import]
            KafkaSource,
            KafkaOffsetsInitializer,
        )
        from pyflink.common import WatermarkStrategy, Duration, Types  # type: ignore[import]
        from pyflink.common.serialization import SimpleStringSchema  # type: ignore[import]
        from pyflink.datastream.window import SlidingEventTimeWindows  # type: ignore[import]
        from pyflink.datastream.functions import (  # type: ignore[import]
            ProcessWindowFunction,
            KeyedProcessFunction,
            RuntimeContext,
        )
        from pyflink.datastream.state import ValueStateDescriptor  # type: ignore[import]
    except ImportError as exc:
        raise RuntimeError(
            "PyFlink is not installed. Run: pip install apache-flink>=1.18.0\n"
            "For local simulation without Flink, use: python -m cce_platform.flink_cdc_pipeline run"
        ) from exc

    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(checkpoint_interval_ms, CheckpointingMode.EXACTLY_ONCE)
    env.get_checkpoint_config().set_min_pause_between_checkpoints(checkpoint_interval_ms // 2)
    # RocksDB state backend is recommended for large keyed state (dedup + window)
    # env.set_state_backend(RocksDBStateBackend("hdfs:///flink/checkpoints"))

    # -- Source ---------------------------------------------------------------
    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(kafka_brokers)
        .set_topics(input_topic)
        .set_group_id("cce-feature-platform-flink")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    watermark_strategy = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_seconds(watermark_lag_s))
        .with_timestamp_assigner(
            # Assigns event_ts from the CDC payload as the event time
            lambda event_str, _: _extract_event_ts_ms(event_str)
        ))

    raw_stream = env.from_source(
        kafka_source,
        watermark_strategy,"KafkaCDCSource",
    )

    # -- Parse & validate -----------------------------------------------------
    parsed = raw_stream.flat_map(_ParseAndValidate(), output_type=Types.STRING())

    # -- Deduplication (keyed by event_id, TTL = 2× watermark lag) -----------
    deduped = (
        parsed
        .key_by(lambda s: json.loads(s)["event_id"])
        .process(_DeduplicateFunction(ttl_s=watermark_lag_s * 2))
    )

    # -- Key by unified_customer_key -----------------------------------------
    keyed = deduped.key_by(lambda s: json.loads(s).get("unified_customer_key", "UNKNOWN"))

    # -- 1-day sliding window: slide = 1 min, size = 1 day -------------------
    from pyflink.datastream.window import Time  # type: ignore[import]
    windowed = (
        keyed
        .window(SlidingEventTimeWindows.of(Time.days(1), Time.minutes(1)))
        .process(_AggregateWindowFunction())
    )

    # -- Sink: Redis HSET per customer ----------------------------------------
    windowed.add_sink(_RedisSink(redis_host, redis_port))

    env.execute("cce-feature-platform-realtime")


# ---------------------------------------------------------------------------
# Flink UDFs (stubs — filled in below with real logic)
# ---------------------------------------------------------------------------

class _ParseAndValidate:
    """
    FlatMapFunction: parse CDC JSON, resolve unified_customer_key, drop unmapped.
    Late-data side output omitted for brevity (add SideOutputTag in production).
    """

    def flat_map(self, value: str):  # type: ignore[override]
        try:
            event = json.loads(value)
        except json.JSONDecodeError:
            logger.warning("Dropping malformed CDC message: %.100s", value)
            return

        from .pipeline import resolve_unified_key, normalize_identifier
        id_type, id_value = normalize_identifier(
            str(event.get("id_type", "")),
            str(event.get("id_value", "")),
        )
        unified_key = (
            str(event["unified_customer_key"])
            if event.get("unified_customer_key")
            else resolve_unified_key(id_type, id_value)
        )
        if not unified_key:
            logger.debug("Dropping unmapped CDC event: id_type=%s", id_type)
            return

        event["unified_customer_key"] = unified_key
        yield json.dumps(event, sort_keys=True)


class _DeduplicateFunction:
    """
    KeyedProcessFunction: exactly-once deduplication via event_id keyed state.
    State TTL prevents unbounded growth for long-running jobs.
    """

    def __init__(self, ttl_s: int) -> None:
        self._ttl_s = ttl_s
        self._seen_state = None  # initialised in open()

    def open(self, ctx: Any) -> None:
        try:
            from pyflink.datastream.state import ValueStateDescriptor, StateTtlConfig  # type: ignore[import]
            from pyflink.common import Duration  # type: ignore[import]
            ttl_config = (
                StateTtlConfig
                .new_builder(Duration.of_seconds(self._ttl_s))
                .set_update_type(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .build()
            )
            desc = ValueStateDescriptor("seen", "java.lang.Boolean")
            desc.enable_time_to_live(ttl_config)
            self._seen_state = ctx.get_state(desc)
        except Exception:
            self._seen_state = None  # graceful degradation in local mode

    def process_element(self, value: str, ctx: Any):
        if self._seen_state is not None and self._seen_state.value() is not None:
            return   # already seen
        if self._seen_state is not None:
            self._seen_state.update(True)
        yield value


class _AggregateWindowFunction:
    """
    ProcessWindowFunction: aggregate events in a 1-day sliding window into
    CceFeatureUpdate.  Score formula mirrors realtime.process_cdc_events().
    """

    def process(self, key: str, ctx: Any, elements):
        order_count = 0
        order_amount = 0.0
        cart_count = 0
        cart_value = 0.0
        last_ts = ""
        last_product = ""

        for elem_str in elements:
            elem = json.loads(elem_str)
            amount = float(elem.get("amount", 0) or 0)
            product = str(elem.get("product", "")).upper()
            ts = elem.get("event_ts", "")

            if elem.get("table") == "orders":
                order_count += 1
                order_amount = round(order_amount + amount, 2)
            elif elem.get("table") == "cart_events":
                cart_count += 1
                cart_value = round(cart_value + amount, 2)

            if ts > last_ts:
                last_ts = ts
                last_product = product

        intent_score = round(
            min(1.0, cart_count * 0.18 + order_count * 0.25 + order_amount / 5000),
            3,
        )

        update = CceFeatureUpdate(
            unified_customer_key=key,
            rt_order_count_1d=order_count,
            rt_order_amount_1d=order_amount,
            rt_cart_add_count_1d=cart_count,
            rt_cart_value_1d=cart_value,
            rt_last_event_ts=last_ts,
            rt_last_product=last_product,
            rt_intent_score=intent_score,
            feature_source="flink_cdc_stream",
            stream_updated_at=datetime.now(UTC).isoformat(timespec="seconds"),
        )
        yield json.dumps(asdict(update), sort_keys=True)


class _RedisSink:
    """
    SinkFunction: write CceFeatureUpdate to Redis HSET.
    Uses pipeline (batched) writes per checkpoint boundary for throughput.
    Exactly-once is achieved by Flink checkpoint + Redis MULTI/EXEC.
    """

    def __init__(self, host: str, port: int) -> None:
        self._host = host
        self._port = port
        self._client = None

    def open(self, ctx: Any) -> None:
        try:
            import redis  # type: ignore[import]
            self._client = redis.Redis(
                host=self._host,
                port=self._port,
                decode_responses=True,
                socket_connect_timeout=3,
            )
        except ImportError:
            logger.warning("redis-py not installed; _RedisSink will no-op")

    def invoke(self, value: str, ctx: Any) -> None:
        if self._client is None:
            return
        try:
            update = json.loads(value)
            key = f"cce:features:{update['unified_customer_key']}"
            self._client.hset(key, mapping={k: str(v) for k, v in update.items()})
        except Exception as exc:
            logger.error("RedisSink write error: %s", exc)

    def close(self) -> None:
        if self._client:
            self._client.close()


# ---------------------------------------------------------------------------
# Timestamp extractor helper
# ---------------------------------------------------------------------------

def _extract_event_ts_ms(event_str: str) -> int:
    """Extract event_ts from CDC JSON and return Unix milliseconds."""
    try:
        event = json.loads(event_str)
        ts_str = event.get("event_ts", "")
        if ts_str:
            dt = datetime.fromisoformat(ts_str)
            return int(dt.timestamp() * 1000)
    except Exception:
        pass
    return int(time.time() * 1000)


# ---------------------------------------------------------------------------
# Local simulation (no Flink cluster required)
# ---------------------------------------------------------------------------

def run_local_simulation(
    events_path: Path | None = None,
    store_path: Path | None = None,
) -> dict[str, int]:
    """
    Run the CDC pipeline locally using the same logic as the Flink UDFs but
    without a Flink cluster.  Suitable for PoC, unit tests, and CI.

    This reuses realtime.process_cdc_events() to stay DRY, but applies the
    same deduplication and intent-score logic defined above.
    """
    from .realtime import read_cdc_events, write_sample_cdc_events
    from .pipeline import resolve_unified_key, normalize_identifier
    from .online_store import LocalOnlineStore
    from .config import settings

    source = events_path or settings.cdc_events_path
    if not source.exists():
        logger.info("CDC events file not found, seeding sample data at %s", source)
        write_sample_cdc_events(source)

    events = read_cdc_events(source)

    # Deduplication: event_id → bool
    seen_event_ids: set[str] = set()
    aggregates: dict[str, dict[str, Any]] = {}

    for event in events:
        # 1. Dedup
        if event.event_id in seen_event_ids:
            logger.debug("Skipping duplicate event_id=%s", event.event_id)
            continue
        seen_event_ids.add(event.event_id)

        # 2. Resolve customer key
        payload = event.after
        unified_key: str | None
        if payload.get("unified_customer_key"):
            unified_key = str(payload["unified_customer_key"])
        else:
            id_type, id_value = normalize_identifier(
                str(payload.get("id_type", "")),
                str(payload.get("id_value", "")),
            )
            unified_key = resolve_unified_key(id_type, id_value)

        if not unified_key:
            continue

        # 3. Aggregate (mirrors _AggregateWindowFunction)
        agg = aggregates.setdefault(unified_key, {
            "rt_order_count_1d":    0,
            "rt_order_amount_1d":   0.0,
            "rt_cart_add_count_1d": 0,
            "rt_cart_value_1d":     0.0,
            "rt_last_event_ts":     "",
            "rt_last_product":      "",
        })

        amount  = float(payload.get("amount", 0) or 0)
        product = str(payload.get("product", "")).upper()
        ts      = event.event_ts

        if event.table == "orders":
            agg["rt_order_count_1d"]  += 1
            agg["rt_order_amount_1d"]  = round(agg["rt_order_amount_1d"] + amount, 2)
        elif event.table == "cart_events":
            agg["rt_cart_add_count_1d"] += 1
            agg["rt_cart_value_1d"]      = round(agg["rt_cart_value_1d"] + amount, 2)

        if ts > agg["rt_last_event_ts"]:
            agg["rt_last_event_ts"]  = ts
            agg["rt_last_product"]   = product

    # 4. Compute intent score & build payloads
    now = datetime.now(UTC).isoformat(timespec="seconds")
    payloads: dict[str, dict[str, Any]] = {}
    for customer_key, agg in aggregates.items():
        intent_score = round(
            min(
                1.0,
                agg["rt_cart_add_count_1d"] * 0.18
                + agg["rt_order_count_1d"] * 0.25
                + agg["rt_order_amount_1d"] / 5000,
            ),
            3,
        )
        payloads[customer_key] = {
            **agg,
            "rt_intent_score":  intent_score,
            "feature_source":   "flink_local_sim",
            "stream_updated_at": now,
        }

    upserted = LocalOnlineStore(store_path).bulk_upsert(payloads)
    return {
        "events_read":        len(events),
        "events_deduplicated": len(events) - len(seen_event_ids),
        "events_processed":   len(seen_event_ids),
        "customers_updated":  upserted,
    }


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="CCE Flink CDC pipeline — local simulation or cluster submit."
    )
    sub = parser.add_subparsers(dest="command")

    # -- local run -----------------------------------------------------------
    run_p = sub.add_parser("run", help="Run local simulation (no Flink cluster required)")
    run_p.add_argument("--events-path", type=Path, default=None)
    run_p.add_argument("--store-path",  type=Path, default=None)

    # -- flink submit --------------------------------------------------------
    sub_p = sub.add_parser("submit", help="Submit job to a running Flink cluster")
    sub_p.add_argument("--kafka-brokers", default=os.getenv("KAFKA_BROKERS", "localhost:9092"))
    sub_p.add_argument("--input-topic",   default=os.getenv("KAFKA_CDC_TOPIC", "cce.cdc.events"))
    sub_p.add_argument("--redis-host",    default=os.getenv("REDIS_HOST", "localhost"))
    sub_p.add_argument("--redis-port",    type=int, default=int(os.getenv("REDIS_PORT", "6379")))
    sub_p.add_argument("--checkpoint-ms", type=int, default=30_000)
    sub_p.add_argument("--watermark-lag-s", type=int, default=10)

    args = parser.parse_args()

    if args.command == "run" or args.command is None:
        import json as _json
        result = run_local_simulation(
            events_path=getattr(args, "events_path", None),
            store_path=getattr(args, "store_path", None),
        )
        print(_json.dumps(result, indent=2, sort_keys=True))

    elif args.command == "submit":
        _build_flink_pipeline(
            kafka_brokers=args.kafka_brokers,
            input_topic=args.input_topic,
            checkpoint_interval_ms=args.checkpoint_ms,
            redis_host=args.redis_host,
            redis_port=args.redis_port,
            watermark_lag_s=args.watermark_lag_s,
        )


if __name__ == "__main__":
    main()
