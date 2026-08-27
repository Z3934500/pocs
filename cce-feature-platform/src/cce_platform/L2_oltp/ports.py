"""What this package needs from a key-value store, stated as a port.

The interface is declared here, by the consumer, rather than in `kv_backend`
where the implementations live. That direction matters: `kv_backend.LocalZSetStore`
is a PoC artifact — a JSON file pretending to be a ZSET — and the transaction
state machine has no business depending on it. Stated as a Protocol, the
dependency inverts: the state machine names the six operations it needs, and any
object providing them structurally satisfies the port without importing,
subclassing, or knowing about this module at all.

Two things this port deliberately fixes, both visible in the code it replaces:

  1. **One signature per operation.** The Redis client and the local store
     disagreed on shape — `zadd(key, {member: score})` versus
     `zadd(key, score, member)`, `zrevrange(key, 0, 0)` returning a list versus
     `zrevrange_top1(key)` returning a scalar. The state machine carried a
     `self._mode == REDIS_MODE` branch at every call site to absorb that. Six
     branches encoding one fact — "which backend am I on" — is the fact leaking
     into logic that does not otherwise care.

  2. **Compare-and-append as one concept.** `_advance_redis` used
     WATCH/MULTI/EXEC; `_advance_local` re-read and compared. Those are two
     implementations of a single idea: append this member only if the newest one
     is still what I read. Naming it here means the state machine expresses its
     concurrency requirement once, and each adapter satisfies it with whatever
     primitive its backend actually has.

The payoff is testability. A fake implementing this Protocol substitutes for
either backend, so an invariant like "a refused transition writes nothing" can be
asserted without a store, a file, or a Redis. `RiskEvaluator` in `risk.py` is the
same pattern applied to the risk gate.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class ZSetStore(Protocol):
    """Sorted-set + hash operations the state machine depends on.

    `runtime_checkable` supports `isinstance` for the boundary assertion in the
    test suite. That only verifies method *names* — it is a wiring check, not a
    signature check, and the harness is what actually pins behaviour.
    """

    def append_scored(self, key: str, score: float, member: str) -> None:
        """Insert `member` at `score`. Replaces an entry of the same identity."""
        ...

    def newest_member(self, key: str) -> str | None:
        """Highest-scoring member, or None when the key holds nothing."""
        ...

    def all_entries(self, key: str) -> list[tuple[str, float]]:
        """Every `(member, score)` pair, lowest score first."""
        ...

    def compare_and_append(
        self, key: str, expected_newest: str, member: str, score: float
    ) -> bool:
        """Append `member` only if `expected_newest` is still the newest entry.

        Returns False on a lost race — the caller re-reads and retries rather
        than treating it as an error. Implementations must not append when
        returning False.
        """
        ...

    def put_fields(self, key: str, mapping: dict[str, str]) -> None:
        """Merge `mapping` into the hash at `key`."""
        ...

    def get_fields(self, key: str) -> dict[str, str]:
        """Every field of the hash at `key`; empty dict when absent."""
        ...

    def close(self) -> None:
        """Release the backend. Must be idempotent."""
        ...
