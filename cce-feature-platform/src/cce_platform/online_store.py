from __future__ import annotations

import json
import logging
import os
import threading
from pathlib import Path
from typing import Any

from .config import settings

logger = logging.getLogger(__name__)

# Module-level lock: ensures atomic read-modify-write across threads.
# In production (Redis backend), this lock is not used.
_store_lock = threading.Lock()


FeaturePayload = dict[str, Any]


class LocalOnlineStore:
    """Small JSON-backed stand-in for Redis used by the local PoC."""

    def __init__(self, path: Path | None = None) -> None:
        self.path = path or settings.online_store_path

    def read_all(self) -> dict[str, FeaturePayload]:
        if not self.path.exists():
            return {}
        with self.path.open("r", encoding="utf-8") as file:
            data = json.load(file)
        return {str(key): dict(value) for key, value in data.items()}

    def get(self, customer_key: str) -> FeaturePayload | None:
        return self.read_all().get(customer_key)

    def bulk_upsert(self, payloads: dict[str, FeaturePayload], replace: bool = False) -> int:
        with _store_lock:
            current = {} if replace else self._read_locked()
            for customer_key, payload in payloads.items():
                existing = current.get(customer_key, {})
                current[customer_key] = {**existing, **payload}
            self._write(current)
        return len(payloads)

    def upsert(self, customer_key: str, payload: FeaturePayload) -> None:
        self.bulk_upsert({customer_key: payload})

    def _read_locked(self) -> dict[str, FeaturePayload]:
        """Read without acquiring the lock — caller must hold _store_lock."""
        if not self.path.exists():
            return {}
        with self.path.open("r", encoding="utf-8") as file:
            data = json.load(file)
        return {str(key): dict(value) for key, value in data.items()}

    def _write(self, data: dict[str, FeaturePayload]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = self.path.with_suffix(f"{self.path.suffix}.tmp")
        with temp_path.open("w", encoding="utf-8") as file:
            json.dump(data, file, indent=2, sort_keys=True)
        # Windows does not allow renaming over an open file (WinError 5/32).
        # Retry with brief backoff; the lock is always transient (held only
        # during the json.dump above, which is protected by _store_lock).
        import time as _time
        for attempt in range(5):
            try:
                temp_path.replace(self.path)
                return
            except OSError:
                if attempt == 4:
                    raise
                _time.sleep(0.01 * (attempt + 1))


class RedisOnlineStore:
    """Redis HASH-backed online store — the production target (ElastiCache).

    One HASH per customer under `cce:features:{unified_customer_key}`, the same
    key namespace `flink_cdc_pipeline._RedisSink` writes to. Batch (T+1 Gold) and
    stream (realtime CDC) therefore land on the same keys and merge field by
    field: `feature_source` tells you which path wrote last.
    """

    KEY_PREFIX = "cce:features:"

    def __init__(self, url: str, client: Any | None = None) -> None:
        self.url = url
        self._client = client if client is not None else self._connect(url)

    @staticmethod
    def _connect(url: str) -> Any:
        try:
            import redis  # type: ignore[import]
        except ImportError as exc:
            raise RuntimeError(
                "redis-py is not installed but a Redis online store was requested. "
                "Install redis>=5.0 (see requirements.txt extras)."
            ) from exc
        client = redis.Redis.from_url(url, decode_responses=True, socket_connect_timeout=3)
        # from_url connects lazily; probe now so a bad endpoint fails here rather
        # than midway through a bulk load that has already written half the keys.
        client.ping()
        return client

    def _key(self, customer_key: str) -> str:
        return f"{self.KEY_PREFIX}{customer_key}"

    def get(self, customer_key: str) -> FeaturePayload | None:
        payload = self._client.hgetall(self._key(customer_key))
        return payload or None

    def read_all(self) -> dict[str, FeaturePayload]:
        """SCAN the whole keyspace. Intended for tests and small PoC datasets."""
        result: dict[str, FeaturePayload] = {}
        for key in self._client.scan_iter(match=f"{self.KEY_PREFIX}*", count=500):
            result[key[len(self.KEY_PREFIX):]] = self._client.hgetall(key)
        return result

    def bulk_upsert(self, payloads: dict[str, FeaturePayload], replace: bool = False) -> int:
        """Pipelined HSET, one HASH per customer.

        `replace=True` replaces each customer's hash in this batch (DEL + HSET) so
        stale fields cannot survive a schema change. It deliberately does NOT
        sweep customers missing from the batch: that would need a SCAN + DEL over
        the shared keyspace and would delete keys the stream path had just
        written. Removing retired customers is a separate, explicit operation.
        """
        if not payloads:
            return 0
        pipe = self._client.pipeline(transaction=False)
        for customer_key, payload in payloads.items():
            key = self._key(customer_key)
            if replace:
                pipe.delete(key)
            # Redis HASH values are strings; None has no field-level
            # representation so those fields are dropped rather than written
            # as the literal "None".
            mapping = {k: str(v) for k, v in payload.items() if v is not None}
            if mapping:
                pipe.hset(key, mapping=mapping)
        pipe.execute()
        return len(payloads)

    def upsert(self, customer_key: str, payload: FeaturePayload) -> None:
        self.bulk_upsert({customer_key: payload})


def make_online_store(
    store_path: Path | None = None,
    redis_url: str | None = None,
) -> LocalOnlineStore | RedisOnlineStore:
    """Pick the online store backend: Redis when a URL is available, else local JSON.

    Mirrors the guard in cart_zset / redis_state_machine — where the deployed
    environment requires Redis, an unreachable endpoint fails fast instead of
    silently writing to a per-process file that no other pod can read.
    """
    url = redis_url or os.getenv("REDIS_URL")
    if url:
        try:
            store = RedisOnlineStore(url)
            logger.info("online store: Redis backend at %s", url)
            return store
        except Exception as exc:
            if settings.require_redis:
                raise RuntimeError(
                    f"online store: Redis at {url} is unreachable ({exc}) and "
                    f"CCE_RUNTIME_ENV={settings.runtime_env} requires it. "
                    "Set CCE_REQUIRE_REDIS=false to allow the local-file fallback."
                ) from exc
            logger.warning("online store: Redis unavailable (%s), using local JSON store", exc)
            return LocalOnlineStore(store_path)
    if settings.require_redis:
        raise RuntimeError(
            f"online store: REDIS_URL is not set but CCE_RUNTIME_ENV="
            f"{settings.runtime_env} requires Redis. "
            "Set CCE_REQUIRE_REDIS=false to allow the local-file fallback."
        )
    return LocalOnlineStore(store_path)
