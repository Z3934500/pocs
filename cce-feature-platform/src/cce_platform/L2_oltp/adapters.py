"""Adapters mapping the two concrete backends onto `ports.ZSetStore`.

One class per backend, each translating the port's vocabulary into whatever the
backend actually offers. Everything the state machine used to branch on lives
here instead:

  * shape differences — `zadd(key, {member: score})` on redis-py against
    `zadd(key, score, member)` on the local store
  * return differences — `zrevrange(key, 0, 0)` yielding a list against
    `zrevrange_top1(key)` yielding a scalar
  * concurrency primitives — WATCH/MULTI/EXEC against a re-read comparison

Keeping the translation in adapters rather than in `state_machine` means the
"which backend am I on" question is answered exactly once, at construction, by
picking a class. The state machine holds a `ZSetStore` and never asks again.

Neither adapter validates transitions or invents audit content: they move members
and scores. Guarding is `state_machine`'s job, encoding is `audit`'s.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


class LocalZSetAdapter:
    """Wraps `kv_backend.LocalZSetStore` (JSON-file PoC backend).

    Single-process only. `compare_and_append` re-reads the newest member and
    compares before writing, which is sufficient under the GIL for one process
    and is not a substitute for a real optimistic lock across replicas — see the
    replica constraint in `state_machine`'s module docstring.
    """

    def __init__(self, store: Any) -> None:
        self._store = store

    def append_scored(self, key: str, score: float, member: str) -> None:
        self._store.zadd(key, score, member)

    def newest_member(self, key: str) -> str | None:
        return self._store.zrevrange_top1(key)

    def all_entries(self, key: str) -> list[tuple[str, float]]:
        return self._store.zrange_all(key)

    def compare_and_append(
        self, key: str, expected_newest: str, member: str, score: float
    ) -> bool:
        if self._store.zrevrange_top1(key) != expected_newest:
            return False
        self._store.zadd(key, score, member)
        return True

    def put_fields(self, key: str, mapping: dict[str, str]) -> None:
        self._store.hset(key, mapping=mapping)

    def get_fields(self, key: str) -> dict[str, str]:
        return self._store.hgetall(key)

    def close(self) -> None:
        self._store.close()


class RedisZSetAdapter:
    """Wraps a connected redis-py client.

    `compare_and_append` is the WATCH/MULTI/EXEC optimistic lock. A `WatchError`
    (another writer committed first) and a connection error both surface as
    False: the caller retries either way, and distinguishing them here would put
    a Redis exception type into the port's contract.
    """

    def __init__(self, client: Any) -> None:
        self._client = client

    def append_scored(self, key: str, score: float, member: str) -> None:
        self._client.zadd(key, {member: score})

    def newest_member(self, key: str) -> str | None:
        results = self._client.zrevrange(key, 0, 0)
        return results[0] if results else None

    def all_entries(self, key: str) -> list[tuple[str, float]]:
        return self._client.zrange(key, 0, -1, withscores=True)

    def compare_and_append(
        self, key: str, expected_newest: str, member: str, score: float
    ) -> bool:
        with self._client.pipeline() as pipe:
            try:
                pipe.watch(key)
                top = pipe.zrevrange(key, 0, 0)
                if top and top[0] != expected_newest:
                    pipe.unwatch()
                    return False
                pipe.multi()
                pipe.zadd(key, {member: score})
                pipe.execute()
                return True
            except Exception as exc:
                logger.debug("compare_and_append: lock miss or transport error (%s)", exc)
                return False

    def put_fields(self, key: str, mapping: dict[str, str]) -> None:
        self._client.hset(key, mapping=mapping)

    def get_fields(self, key: str) -> dict[str, str]:
        return self._client.hgetall(key)

    def close(self) -> None:
        self._client.close()
