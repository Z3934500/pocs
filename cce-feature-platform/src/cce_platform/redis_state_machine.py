"""
Redis ZSET-backed transaction state machine for CCE financial transactions.

Design:
  - Each transaction has a state history stored in a ZSET:key:   txn:state:{txn_id}
      score: Unix timestamp (microseconds for sub-second ordering)
      member: {state}:{event_id}   (event_id ensures uniqueness within same second)

  - Current state is ZREVRANGE key 0 0 (highest score = latest)
  - Full audit trail is ZRANGE key 0 -1 WITHSCORES
  - Optimistic concurrency via WATCH + MULTI/EXEC (no lost updates under concurrent writers)
  - Fallback to LocalOnlineStore when Redis is unavailable (PoC / CI mode)

State machine (matches CCE product types):
  PENDING → RISK_CHECK → APPROVED  → SETTLED       ↘ COMPLIANCE_HOLD → APPROVED → SETTLED
                       ↘ REJECTEDAny state → COMPENSATING → COMPENSATED   (saga rollback path)

Usage:
  from cce_platform.redis_state_machine import TransactionStateMachine, TxnState

  sm = TransactionStateMachine()# uses env REDIS_URL or falls back to local
  sm.init_transaction("TXN-001", amount=1700.0, product="PREMIUM_FINANCING")
  sm.advance("TXN-001", TxnState.RISK_CHECK, actor="risk_engine")
  sm.advance("TXN-001", TxnState.APPROVED,   actor="auto_approve")
  sm.advance("TXN-001", TxnState.SETTLED,    actor="settlement_svc")

  history = sm.get_history("TXN-001")      # full audit trail
  current = sm.get_current_state("TXN-001")
"""

from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any
from uuid import uuid4

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# State definitions
# ---------------------------------------------------------------------------

class TxnState(str, Enum):
    PENDING                  = "PENDING"
    RISK_CHECK               = "RISK_CHECK"
    COMPLIANCE_HOLD          = "COMPLIANCE_HOLD"
    APPROVED                 = "APPROVED"
    PENDING_SETTLE           = "PENDING_SETTLE"           # T+N waiting for settlement date
    SETTLEMENT_IN_PROGRESS   = "SETTLEMENT_IN_PROGRESS"   # SettlementTrigger fired
    SETTLED                  = "SETTLED"
    REJECTED                 = "REJECTED"
    COMPENSATING             = "COMPENSATING"
    COMPENSATED              = "COMPENSATED"


# Valid forward transitions.  Backward transitions are never allowed to
# prevent state machine corruption under concurrent writes.
ALLOWED_TRANSITIONS: dict[TxnState, set[TxnState]] = {
    TxnState.PENDING:                {TxnState.RISK_CHECK},
    TxnState.RISK_CHECK:             {TxnState.COMPLIANCE_HOLD, TxnState.APPROVED, TxnState.REJECTED},
    TxnState.COMPLIANCE_HOLD:        {TxnState.APPROVED, TxnState.REJECTED},
    # APPROVED → PENDING_SETTLE for T+N products (scheduled by schedule_settlement())
    # APPROVED → SETTLED directly for T+0 products (SAVINGS, CARD, TRAVEL_INSURANCE)
    TxnState.APPROVED:               {TxnState.PENDING_SETTLE, TxnState.SETTLED, TxnState.COMPENSATING},
    TxnState.PENDING_SETTLE:         {TxnState.SETTLEMENT_IN_PROGRESS, TxnState.COMPENSATING},
    TxnState.SETTLEMENT_IN_PROGRESS: {TxnState.SETTLED, TxnState.COMPENSATING},
    TxnState.SETTLED:                {TxnState.COMPENSATING},   # post-settlement reversal path
    TxnState.REJECTED:               set(),                     # terminal
    TxnState.COMPENSATING:           {TxnState.COMPENSATED},
    TxnState.COMPENSATED:            set(),                     # terminal
}

# High-value thresholds for CCE products (SGD)
COMPLIANCE_HOLD_THRESHOLD: dict[str, float] = {
    "PREMIUM_FINANCING": 1000.0,
    "INVESTMENT":        500.0,
    "INVESTMENT_LINKED": 500.0,
    "INSURANCE":         2000.0,
}


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class StateTransition:
    txn_id:     str
    from_state: TxnState | None   # None for initial PENDING
    to_state:   TxnState
    event_id:   str
    actor:      str
    reason:     str
    timestamp:  float             # Unix epoch seconds (float for microsecond precision)
    metadata:   dict[str, Any]


@dataclass
class TransactionRecord:
    txn_id:       str
    product:      str
    amount:       float
    customer_key: str
    current_state: TxnState
    history:      list[StateTransition] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class InvalidTransitionError(Exception):
    """Raised when a requested state transition is not allowed."""


class TransactionNotFoundError(Exception):
    """Raised when the transaction does not exist in the store."""


class ConcurrentModificationError(Exception):
    """Raised when optimistic lock detects a concurrent write (caller should retry)."""


# ---------------------------------------------------------------------------
# Backend: local fallback (mirrors LocalOnlineStore pattern)
# ---------------------------------------------------------------------------

class _LocalStateStore:
    """
    File-backed fallback used in PoC / unit-test mode when Redis is absent.
    Persists state as JSON.  Not safe for concurrent multi-process writes,
    but sufficient for single-process local development.
    """

    def __init__(self, path: Path) -> None:
        self._path = path

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
        tmp.replace(self._path)

    # -- ZSET semantics emulated with sorted list of (score, member) tuples --

    def zadd(self, key: str, score: float, member: str) -> None:
        data = self._load()
        zset: list[list] = data.get(key, [])
        # remove existing entry with same member (ZADD NX-like upsert)
        zset = [entry for entry in zset if entry[1] != member]
        zset.append([score, member])
        zset.sort(key=lambda e: e[0])
        data[key] = zset
        self._save(data)

    def zrevrange_top1(self, key: str) -> str | None:
        data = self._load()
        zset = data.get(key, [])
        if not zset:
            return None
        return zset[-1][1]   # highest score = last after sort

    def zrange_all(self, key: str) -> list[tuple[str, float]]:
        data = self._load()
        zset = data.get(key, [])
        return [(entry[1], entry[0]) for entry in zset]

    def exists(self, key: str) -> bool:
        data = self._load()
        return key in data and len(data[key]) > 0

    def hset(self, key: str, mapping: dict[str, str]) -> None:
        data = self._load()
        data[key] = {**data.get(key, {}), **mapping}
        self._save(data)

    def hgetall(self, key: str) -> dict[str, str]:
        data = self._load()
        return data.get(key, {})

    def close(self) -> None:
        pass   # nothing to close


# ---------------------------------------------------------------------------
# Backend: Redis (production)
# ---------------------------------------------------------------------------

def _make_redis_client(redis_url: str):
    """
    Returns a redis.Redis client, or raises ImportError if redis-py is not installed.
    Kept as a function so the module imports cleanly without redis installed.
    """
    try:
        import redis  # type: ignore[import]
    except ImportError as exc:
        raise ImportError(
            "redis-py is required for Redis backend. "
            "Install with: pip install redis>=5.0"
        ) from exc

    client = redis.Redis.from_url(
        redis_url,
        decode_responses=True,
        socket_connect_timeout=3,
        socket_timeout=3,
        retry_on_timeout=True,
    )
    client.ping()   # fail fast on bad URL
    return client


# ---------------------------------------------------------------------------
# Main state machine class
# ---------------------------------------------------------------------------

class TransactionStateMachine:
    """ZSET-backed financial transaction state machine.

    Instantiation order:
      1. If REDIS_URL env var is set and redis-py is installed → Redis backend
      2. Otherwise → local JSON file backend (PoC mode)

    All public methods are safe to call from multiple threads (GIL covers the
    local backend; Redis backend uses WATCH/MULTI/EXEC for optimistic locking).
    """

    _MAX_RETRIES = 3   # optimistic lock retry limit

    def __init__(
        self,
        redis_url: str | None = None,
        local_store_path: Path | None = None,
    ) -> None:
        url = redis_url or os.getenv("REDIS_URL")
        if url:
            try:
                self._backend = _make_redis_client(url)
                self._mode = "redis"
                logger.info("TransactionStateMachine: using Redis backend at %s", url)
            except Exception as exc:
                logger.warning(
                    "TransactionStateMachine: Redis unavailable (%s), falling back to local store", exc
                )
                self._backend = self._make_local(local_store_path)
                self._mode = "local"
        else:
            self._backend = self._make_local(local_store_path)
            self._mode = "local"

    @staticmethod
    def _make_local(path: Path | None) -> _LocalStateStore:
        from .config import settings
        default = settings.base_dir / "data" / "online" / "txn_state_machine.json"
        return _LocalStateStore(path or default)

    # -- Key helpers ---------------------------------------------------------

    @staticmethod
    def _zset_key(txn_id: str) -> str:
        return f"txn:state:{txn_id}"

    @staticmethod
    def _meta_key(txn_id: str) -> str:
        return f"txn:meta:{txn_id}"

    # -- Internal helpers ----------------------------------------------------

    def _parse_member(self, member: str) -> tuple[TxnState, str]:
        """member format: '{state}:{event_id}'"""
        state_str, event_id = member.split(":", 1)
        return TxnState(state_str), event_id

    def _make_member(self, state: TxnState, event_id: str) -> str:
        return f"{state.value}:{event_id}"

    def _get_current_member_redis(self, txn_id: str) -> str | None:
        key = self._zset_key(txn_id)
        results = self._backend.zrevrange(key, 0, 0)
        return results[0] if results else None

    def _get_current_member_local(self, txn_id: str) -> str | None:
        return self._backend.zrevrange_top1(self._zset_key(txn_id))

    def _get_current_member(self, txn_id: str) -> str | None:
        if self._mode == "redis":
            return self._get_current_member_redis(txn_id)
        return self._get_current_member_local(txn_id)

    # -- Public API ----------------------------------------------------------

    def init_transaction(
        self,
        txn_id: str,
        amount: float,
        product: str,
        customer_key: str = "",
        actor: str = "system",
        metadata: dict[str, Any] | None = None,
    ) -> StateTransition:
        """
        Create a new transaction in PENDING state.Idempotent: if the transaction already exists, returns its current transition
        without modifying state.
        """
        if self._get_current_member(txn_id) is not None:
            logger.info("init_transaction: txn %s already exists, skipping init", txn_id)
            current = self.get_current_state(txn_id)
            # return a synthetic transition for the caller
            return StateTransition(
                txn_id=txn_id,
                from_state=None,
                to_state=current,
                event_id="already_exists",
                actor=actor,
                reason="idempotent_init",
                timestamp=time.time(),
                metadata={},
            )

        event_id = str(uuid4())
        ts = time.time()
        member = self._make_member(TxnState.PENDING, event_id)

        meta = {
            "txn_id":       txn_id,
            "product":      product.upper(),
            "amount":       str(amount),
            "customer_key": customer_key,
            "created_at":   str(ts),
        }

        if self._mode == "redis":
            self._backend.zadd(self._zset_key(txn_id), {member: ts})
            self._backend.hset(self._meta_key(txn_id), mapping=meta)
        else:
            self._backend.zadd(self._zset_key(txn_id), ts, member)
            self._backend.hset(self._meta_key(txn_id), mapping=meta)

        transition = StateTransition(
            txn_id=txn_id,
            from_state=None,
            to_state=TxnState.PENDING,
            event_id=event_id,
            actor=actor,
            reason="init",
            timestamp=ts,
            metadata=metadata or {},
        )
        logger.info("init_transaction: %s → PENDING (product=%s, amount=%.2f)", txn_id, product, amount)
        return transition

    def advance(
        self,
        txn_id: str,
        to_state: TxnState,
        actor: str = "system",
        reason: str = "",
        metadata: dict[str, Any] | None = None,
    ) -> StateTransition:
        """
        Advance the transaction to the next state.

        Uses optimistic locking (WATCH/MULTI/EXEC on Redis, re-read loop on local).
        Raises:
          TransactionNotFoundError   – txn_id has never been initialised
          InvalidTransitionError     – transition is not in ALLOWED_TRANSITIONS
          ConcurrentModificationError – could not acquire optimistic lock after retries
        """
        for attempt in range(self._MAX_RETRIES):
            current_member = self._get_current_member(txn_id)
            if current_member is None:
                raise TransactionNotFoundError(f"Transaction {txn_id!r} not found")

            current_state, _ = self._parse_member(current_member)

            if to_state not in ALLOWED_TRANSITIONS.get(current_state, set()):
                raise InvalidTransitionError(
                    f"Transition {current_state} → {to_state} is not allowed for {txn_id!r}. "
                    f"Allowed: {ALLOWED_TRANSITIONS.get(current_state, set())}"
                )

            event_id = str(uuid4())
            # Use microsecond precision to preserve ordering within the same second
            ts = time.time() + attempt * 1e-6   # tiny offset avoids score collision on retry
            member = self._make_member(to_state, event_id)

            if self._mode == "redis":
                success = self._advance_redis(txn_id, current_member, member, ts)
            else:
                success = self._advance_local(txn_id, current_member, member, ts)

            if success:
                transition = StateTransition(
                    txn_id=txn_id,
                    from_state=current_state,
                    to_state=to_state,
                    event_id=event_id,
                    actor=actor,
                    reason=reason or f"{current_state}→{to_state}",
                    timestamp=ts,
                    metadata=metadata or {},
                )
                logger.info(
                    "advance: %s  %s → %s  actor=%s", txn_id, current_state, to_state, actor
                )
                return transition

            logger.debug("advance: optimistic lock miss on attempt %d for %s", attempt + 1, txn_id)

        raise ConcurrentModificationError(
            f"Could not advance {txn_id!r} to {to_state} after {self._MAX_RETRIES} retries"
        )

    def _advance_redis(
        self, txn_id: str, expected_member: str, new_member: str, ts: float
    ) -> bool:
        """WATCH / MULTI / EXEC optimistic lock pattern."""
        key = self._zset_key(txn_id)
        with self._backend.pipeline() as pipe:
            try:
                pipe.watch(key)
                # Re-read under watch to detect concurrent modification
                top = pipe.zrevrange(key, 0, 0)
                if top and top[0] != expected_member:
                    pipe.unwatch()
                    return False
                pipe.multi()
                pipe.zadd(key, {new_member: ts})
                pipe.execute()
                return True
            except Exception:  # WatchError or connection error
                return False

    def _advance_local(
        self, txn_id: str, expected_member: str, new_member: str, ts: float
    ) -> bool:
        """Re-read and compare for local backend (single-process optimistic lock)."""
        current = self._backend.zrevrange_top1(self._zset_key(txn_id))
        if current != expected_member:
            return False
        self._backend.zadd(self._zset_key(txn_id), ts, new_member)
        return True

    def get_current_state(self, txn_id: str) -> TxnState:
        member = self._get_current_member(txn_id)
        if member is None:
            raise TransactionNotFoundError(f"Transaction {txn_id!r} not found")
        state, _ = self._parse_member(member)
        return state

    def get_history(self, txn_id: str) -> list[StateTransition]:
        """Return full state history in chronological order."""
        if self._mode == "redis":
            entries = self._backend.zrange(self._zset_key(txn_id), 0, -1, withscores=True)
        else:
            entries = self._backend.zrange_all(self._zset_key(txn_id))

        history: list[StateTransition] = []
        prev_state: TxnState | None = None
        for member, score in entries:
            state, event_id = self._parse_member(member)
            history.append(StateTransition(
                txn_id=txn_id,
                from_state=prev_state,
                to_state=state,
                event_id=event_id,
                actor="",      # actor not persisted in ZSET member (kept minimal)
                reason="",
                timestamp=score,
                metadata={},
            ))
            prev_state = state
        return history

    def get_transaction_meta(self, txn_id: str) -> dict[str, str]:
        if self._mode == "redis":
            return self._backend.hgetall(self._meta_key(txn_id))
        return self._backend.hgetall(self._meta_key(txn_id))

    def should_compliance_hold(self, txn_id: str) -> bool:
        """
        Returns True if this transaction should be routed through COMPLIANCE_HOLD
        based on product type and amount thresholds (CCE-specific rule).
        """
        meta = self.get_transaction_meta(txn_id)
        product = meta.get("product", "")
        try:
            amount = float(meta.get("amount", 0))
        except (ValueError, TypeError):
            amount = 0.0
        threshold = COMPLIANCE_HOLD_THRESHOLD.get(product, float("inf"))
        return amount >= threshold

    def run_auto_advance(self, txn_id: str, actor: str = "auto_engine") -> list[StateTransition]:
        """
        Convenience method: automatically advance a freshly initialised transaction
        through RISK_CHECK → (COMPLIANCE_HOLD if threshold exceeded) → APPROVED.
        Returns the list of transitions applied.
        Used in tests and local PoC demos.
        """
        transitions: list[StateTransition] = []

        current = self.get_current_state(txn_id)
        if current != TxnState.PENDING:
            return transitions

        t1 = self.advance(txn_id, TxnState.RISK_CHECK, actor=actor, reason="auto_risk_check")
        transitions.append(t1)

        if self.should_compliance_hold(txn_id):
            t2 = self.advance(txn_id, TxnState.COMPLIANCE_HOLD, actor=actor, reason="amount_threshold")
            transitions.append(t2)

        t3 = self.advance(txn_id, TxnState.APPROVED, actor=actor, reason="auto_approved")
        transitions.append(t3)
        return transitions

    def close(self) -> None:
        self._backend.close()

    def __enter__(self) -> "TransactionStateMachine":
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()