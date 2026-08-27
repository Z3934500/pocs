"""Shared key-value backend plumbing for the Redis-backed modules.

Two things lived in triplicate before this module existed:

  1. The bootstrap decision — read `REDIS_URL`, try to connect, and on failure
     either raise (deployed environments) or degrade to a local file (PoC).
     `TransactionStateMachine`, `CartService` and `make_online_store` each had
     their own near-identical copy, including the error strings.

  2. The local ZSET emulation — `_LocalStateStore` and `_LocalCartStore` were
     near-verbatim copies of each other, differing only in how they decide that
     two members are "the same" for upsert and removal.

`LocalZSetStore` takes that one real difference as a parameter
(`member_identity`) instead of duplicating the class, and `make_kv_backend`
owns the bootstrap decision for all three callers.
"""

from __future__ import annotations

import json
import logging
import os
import time
from pathlib import Path
from typing import Any, Callable, TypeVar

from ..L0_configuration import settings

logger = logging.getLogger(__name__)


def _identity(member: str) -> str:
    """Default member identity: the member string itself (exact-match upsert)."""
    return member


class LocalZSetStore:
    """File-backed ZSET + HASH emulation used in PoC / unit-test mode.

    Persists everything as one JSON document. ZSETs are stored as a sorted list
    of `[score, member]` pairs; HASHes as plain objects. Not safe for concurrent
    multi-process writes, but sufficient for single-process local development.

    `member_identity` decides when two members are the same entry. The state
    machine compares whole member strings; the cart parses `item_id` out of the
    member JSON so that re-adding an item updates it in place. Everything else
    about the two stores was identical.
    """

    def __init__(
        self,
        path: Path,
        member_identity: Callable[[str], str] = _identity,
    ) -> None:
        self._path = path
        self._member_identity = member_identity

    # -- Persistence ---------------------------------------------------------

    def _load(self) -> dict[str, Any]:
        if not self._path.exists():
            return {}
        with self._path.open("r", encoding="utf-8") as fh:
            return json.load(fh)

    def _save(self, data: dict[str, Any]) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self._path.with_suffix(f"{self._path.suffix}.tmp")
        with tmp.open("w", encoding="utf-8") as fh:
            json.dump(data, fh, indent=2, sort_keys=True)
        # Windows refuses to rename over a file another handle still has open
        # (WinError 5/32). The window is short — only the dump above — so a brief
        # backoff clears it. Same retry as LocalOnlineStore._write.
        for attempt in range(5):
            try:
                tmp.replace(self._path)
                return
            except OSError:
                if attempt == 4:
                    raise
                time.sleep(0.01 * (attempt + 1))

    def _safe_identity(self, member: str) -> str:
        """Apply member_identity, falling back to the raw member on bad input.

        A member that does not parse (hand-edited store file, format change)
        must not take down an unrelated write, so it degrades to exact-match
        semantics for that entry only.
        """
        try:
            return self._member_identity(member)
        except Exception:
            return member

    # -- ZSET ops ------------------------------------------------------------

    def zadd(self, key: str, score: float, member: str) -> None:
        """Insert or update `member`. Entries with the same identity are replaced."""
        data = self._load()
        zset: list[list] = data.get(key, [])
        identity = self._safe_identity(member)
        zset = [e for e in zset if self._safe_identity(e[1]) != identity]
        zset.append([score, member])
        zset.sort(key=lambda e: e[0])
        data[key] = zset
        self._save(data)

    def zrem(self, key: str, member: str) -> int:
        """Remove by member string. Returns the number of entries removed."""
        return self.zrem_by_identity(key, self._safe_identity(member))

    def zrem_by_identity(self, key: str, identity: str) -> int:
        """Remove every entry whose member identity equals `identity`."""
        data = self._load()
        zset: list[list] = data.get(key, [])
        before = len(zset)
        zset = [e for e in zset if self._safe_identity(e[1]) != identity]
        data[key] = zset
        self._save(data)
        return before - len(zset)

    def zrange_all(self, key: str) -> list[tuple[str, float]]:
        """Return all `(member, score)` pairs, lowest score first."""
        data = self._load()
        return [(entry[1], entry[0]) for entry in data.get(key, [])]

    def zrevrange_top1(self, key: str) -> str | None:
        """Return the member with the highest score, or None if the key is empty."""
        data = self._load()
        zset = data.get(key, [])
        if not zset:
            return None
        return zset[-1][1]   # highest score = last after sort

    def zrangebyscore(
        self, key: str, min_score: float, max_score: float
    ) -> list[tuple[str, float]]:
        """Return members whose score falls in [min_score, max_score]."""
        return [
            (member, score)
            for member, score in self.zrange_all(key)
            if min_score <= score <= max_score
        ]

    def zcard(self, key: str) -> int:
        data = self._load()
        return len(data.get(key, []))

    def delete(self, key: str) -> None:
        data = self._load()
        data.pop(key, None)
        self._save(data)

    def exists(self, key: str) -> bool:
        data = self._load()
        return key in data and len(data[key]) > 0

    # -- HASH ops ------------------------------------------------------------

    def hset(self, key: str, mapping: dict[str, str]) -> None:
        data = self._load()
        data[key] = {**data.get(key, {}), **mapping}
        self._save(data)

    def hgetall(self, key: str) -> dict[str, str]:
        data = self._load()
        return data.get(key, {})

    def close(self) -> None:
        pass   # nothing to close


def make_redis_client(redis_url: str) -> Any:
    """Return a connected redis-py client, or raise if it is unusable.

    Kept as a function so every module imports cleanly without redis installed.
    `from_url` connects lazily, so this pings to fail fast on a bad endpoint
    rather than midway through the first real operation.
    """
    try:
        import redis  # type: ignore[import]
    except ImportError as exc:
        raise ImportError(
            "redis-py is required for the Redis backend. "
            "Install with: pip install redis>=5.0"
        ) from exc

    client = redis.Redis.from_url(
        redis_url,
        decode_responses=True,
        socket_connect_timeout=3,
        socket_timeout=3,
        retry_on_timeout=True,
    )
    client.ping()
    return client


LocalT = TypeVar("LocalT")
RedisT = TypeVar("RedisT")

REDIS_MODE = "redis"
LOCAL_MODE = "local"


def make_kv_backend(
    name: str,
    local_factory: Callable[[], LocalT],
    redis_factory: Callable[[str], RedisT] = make_redis_client,
    redis_url: str | None = None,
) -> tuple[LocalT | RedisT, str]:
    """Pick the Redis backend when it is available, else the local fallback.

    Returns `(backend, mode)` where mode is `"redis"` or `"local"`.

    The local fallback is per-process, so replicas sharing it would each hold
    their own divergent copy of the data. Where the deployed environment
    requires Redis (`settings.require_redis`, driven by `CCE_RUNTIME_ENV` /
    `CCE_REQUIRE_REDIS`), an unset or unreachable endpoint therefore raises here
    instead of degrading silently. `name` only identifies the caller in log and
    error messages.
    """
    url = redis_url or os.getenv("REDIS_URL")

    if url:
        try:
            backend = redis_factory(url)
            logger.info("%s: using Redis backend at %s", name, url)
            return backend, REDIS_MODE
        except Exception as exc:
            if settings.require_redis:
                raise RuntimeError(
                    f"{name}: Redis at {url} is unreachable ({exc}) and "
                    f"CCE_RUNTIME_ENV={settings.runtime_env} requires it. "
                    "Set CCE_REQUIRE_REDIS=false to allow the local-file fallback."
                ) from exc
            logger.warning(
                "%s: Redis unavailable (%s), falling back to local store", name, exc
            )
            return local_factory(), LOCAL_MODE

    if settings.require_redis:
        raise RuntimeError(
            f"{name}: REDIS_URL is not set but CCE_RUNTIME_ENV="
            f"{settings.runtime_env} requires Redis. "
            "Set CCE_REQUIRE_REDIS=false to allow the local-file fallback."
        )
    return local_factory(), LOCAL_MODE
