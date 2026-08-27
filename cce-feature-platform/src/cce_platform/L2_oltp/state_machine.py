"""
Redis ZSET-backed transaction state machine for CCE financial transactions.

Design:
  - Each transaction has a state history stored in a ZSET:key:   txn:state:{txn_id}
      score: Unix timestamp (microseconds for sub-second ordering)
      member: one JSON object holding state, event_id, actor, reason, metadata
              (event_id ensures uniqueness within the same second)

    The member encoding lives in `audit.py`, not here: what an audit entry says
    is a separate decision from how transitions are guarded, and attribution has
    to be part of the same ZADD as the transition it describes. Legacy
    `{state}:{event_id}` members still decode, flagged `attributed=False`.

  - Current state is ZREVRANGE key 0 0 (highest score = latest)
  - Full audit trail is ZRANGE key 0 -1 WITHSCORES
  - Optimistic concurrency via WATCH + MULTI/EXEC (no lost updates under concurrent writers)
  - Fallback to a local JSON file when Redis is unavailable (PoC / CI mode)

Replica constraint:
  The local fallback is per-process, so transaction state is NOT shared between
  pods. Unlike the Feature API — which serves derived read-only data that every
  pod rebuilds identically from the deterministic gold pipeline — this module
  holds authoritative mutable state. Serving it from more than one replica on
  the local backend would split the state history: a transaction advanced on one
  pod would be invisible, or appear stale, on another. This module is not wired
  into api.py today. Before exposing it over HTTP in a multi-replica deployment,
  provision a real Redis and leave CCE_REQUIRE_REDIS unset so staging and
  production fail fast instead of degrading (see config.py).

State machine (matches CCE product types):
  PENDING → RISK_CHECK → APPROVED → SETTLED
                       ↘ COMPLIANCE_HOLD → APPROVED → SETTLED
                       ↘ REJECTED

  Any state → COMPENSATING → COMPENSATED   (saga rollback path)

Usage:
  from cce_platform.L2_oltp import TransactionStateMachine, TxnState

  sm = TransactionStateMachine()  # uses env REDIS_URL or falls back to local
  sm.init_transaction("TXN-001", amount=1700.0, product="PREMIUM_FINANCING")
  sm.advance("TXN-001", TxnState.RISK_CHECK, actor="risk_engine")
  sm.advance("TXN-001", TxnState.APPROVED,   actor="auto_approve")
  sm.advance("TXN-001", TxnState.SETTLED,    actor="settlement_svc")

  history = sm.get_history("TXN-001")      # full audit trail
  current = sm.get_current_state("TXN-001")
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any
from uuid import uuid4

from ..L1_mechanism import LocalZSetStore, REDIS_MODE, make_kv_backend
from .adapters import LocalZSetAdapter, RedisZSetAdapter
from .audit import decode_member, encode_member
from .ports import ZSetStore
from .risk import RiskDecision, RiskEvaluator, ThresholdRiskEvaluator

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

# High-value thresholds are business policy, loaded from config rather than
# hardcoded here — see policy.py. This module reaches them through the injected
# RiskEvaluator (L2_oltp/risk.py) rather than importing the accessor directly, so
# the RISK_CHECK gate has one substitution point.


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
    # False when read back from a member written before attribution was stored:
    # actor/reason are unknown for that entry, not empty. See L2_oltp/audit.py.
    attributed: bool = True


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
# Main state machine class
# ---------------------------------------------------------------------------
#
# This class depends on `ports.ZSetStore`, not on a concrete backend. The two
# adapters in `adapters.py` map redis-py and `kv_backend.LocalZSetStore` onto
# that port; a fake satisfying the same Protocol substitutes for either in a
# test. Members are compared whole, which is LocalZSetStore's default member
# identity.

class TransactionStateMachine:
    """ZSET-backed financial transaction state machine.

    Instantiation order:
      1. If REDIS_URL env var is set and redis-py is installed → Redis backend
      2. Otherwise → local JSON file backend (PoC mode), unless
         settings.require_redis is set, in which case construction raises

    The local backend is per-process and therefore single-replica only; see the
    module docstring for the replica constraint.

    All public methods are safe to call from multiple threads (GIL covers the
    local backend; Redis backend uses WATCH/MULTI/EXEC for optimistic locking).
    """

    _MAX_RETRIES = 3   # optimistic lock retry limit

    def __init__(
        self,
        redis_url: str | None = None,
        local_store_path: Path | None = None,
        risk_evaluator: RiskEvaluator | None = None,
        store: ZSetStore | None = None,
    ) -> None:
        # An injected store short-circuits backend selection entirely: a fake
        # satisfying ports.ZSetStore needs no file and no Redis, so invariants
        # like "a refused transition writes nothing" are assertable directly.
        if store is not None:
            self._store: ZSetStore = store
            self._mode = "injected"
        else:
            # Degrading to the per-process local store would let each replica keep
            # its own divergent transaction state, so make_kv_backend raises instead
            # wherever the environment requires Redis.
            backend, mode = make_kv_backend(
                "TransactionStateMachine",
                local_factory=lambda: self._make_local(local_store_path),
                redis_url=redis_url,
            )
            # The only place the backend's identity is consulted. Past this line
            # the state machine holds a ZSetStore and never asks again.
            self._store = (
                RedisZSetAdapter(backend) if mode == REDIS_MODE
                else LocalZSetAdapter(backend)
            )
            self._mode = mode
        # The RISK_CHECK gate is injected rather than hardcoded. The default is
        # the amount-vs-threshold rule this class has always applied; see
        # L2_oltp/risk.py for what a feature-reading evaluator would additionally
        # need from the platform.
        self._risk_evaluator: RiskEvaluator = risk_evaluator or ThresholdRiskEvaluator()

    @staticmethod
    def _make_local(path: Path | None) -> LocalZSetStore:
        from ..L0_configuration import settings
        default = settings.base_dir / "data" / "online" / "txn_state_machine.json"
        return LocalZSetStore(path or default)

    # -- Key helpers ---------------------------------------------------------

    @staticmethod
    def _zset_key(txn_id: str) -> str:
        return f"txn:state:{txn_id}"

    @staticmethod
    def _meta_key(txn_id: str) -> str:
        return f"txn:meta:{txn_id}"

    # -- Internal helpers ----------------------------------------------------

    def _parse_member(self, member: str) -> tuple[TxnState, str]:
        """Extract just the state and event id, for callers that need no attribution."""
        record = decode_member(member)
        return TxnState(record.state), record.event_id

    def _make_member(
        self,
        state: TxnState,
        event_id: str,
        actor: str = "",
        reason: str = "",
        metadata: dict[str, Any] | None = None,
    ) -> str:
        """Encode one transition, attribution included — see `audit.py` for why.

        `state.value` rather than the member itself: for a `str`-mixin Enum,
        f-string interpolation renders as `TxnState.PENDING` (3.11+), which would
        put the class name into stored audit data.
        """
        return encode_member(state.value, event_id, actor, reason, metadata)

    def _get_current_member(self, txn_id: str) -> str | None:
        return self._store.newest_member(self._zset_key(txn_id))

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
        member = self._make_member(
            TxnState.PENDING, event_id, actor=actor, reason="init", metadata=metadata or {}
        )

        meta = {
            "txn_id":       txn_id,
            "product":      product.upper(),
            "amount":       str(amount),
            "customer_key": customer_key,
            "created_at":   str(ts),
        }

        self._store.append_scored(self._zset_key(txn_id), ts, member)
        self._store.put_fields(self._meta_key(txn_id), meta)

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
            # `.value` on both sides: for a str-mixin Enum, f-string interpolation
            # renders as `TxnState.APPROVED` (3.11+), and this reason is now stored
            # audit data rather than a log line, so it has to read as the state name.
            effective_reason = reason or f"{current_state.value}→{to_state.value}"
            member = self._make_member(
                to_state, event_id,
                actor=actor, reason=effective_reason, metadata=metadata or {},
            )

            success = self._store.compare_and_append(
                self._zset_key(txn_id), current_member, member, ts
            )

            if success:
                transition = StateTransition(
                    txn_id=txn_id,
                    from_state=current_state,
                    to_state=to_state,
                    event_id=event_id,
                    actor=actor,
                    reason=effective_reason,
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

    def get_current_state(self, txn_id: str) -> TxnState:
        member = self._get_current_member(txn_id)
        if member is None:
            raise TransactionNotFoundError(f"Transaction {txn_id!r} not found")
        state, _ = self._parse_member(member)
        return state

    def get_history(self, txn_id: str) -> list[StateTransition]:
        """Return full state history in chronological order, attribution included.

        Entries written before attribution was stored come back with
        `attributed=False` and empty `actor`/`reason` — unknown, not blank. An
        audit view should render that distinction rather than an empty operator.
        """
        entries = self._store.all_entries(self._zset_key(txn_id))

        history: list[StateTransition] = []
        prev_state: TxnState | None = None
        for member, score in entries:
            record = decode_member(member)
            state = TxnState(record.state)
            history.append(StateTransition(
                txn_id=txn_id,
                from_state=prev_state,
                to_state=state,
                event_id=record.event_id,
                actor=record.actor,
                reason=record.reason,
                timestamp=score,
                metadata=dict(record.metadata),
                attributed=record.attributed,
            ))
            prev_state = state
        return history

    def get_transaction_meta(self, txn_id: str) -> dict[str, str]:
        return self._store.get_fields(self._meta_key(txn_id))

    def evaluate_risk(self, txn_id: str) -> RiskDecision:
        """Run the configured risk evaluator against this transaction.

        Returns the full decision including provenance. `should_compliance_hold`
        is the boolean-only shorthand over this.
        """
        meta = self.get_transaction_meta(txn_id)
        return self._risk_evaluator.evaluate(txn_id, meta)

    def should_compliance_hold(self, txn_id: str) -> bool:
        """
        Returns True if this transaction should be routed through COMPLIANCE_HOLD.

        Delegates to the configured RiskEvaluator, which defaults to the
        amount-vs-threshold rule using business policy loaded from config (see
        policy.py), so a regulatory change is a config edit rather than a code
        release. Products without a configured threshold are never held on
        amount alone.
        """
        return self.evaluate_risk(txn_id).hold

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

        decision = self.evaluate_risk(txn_id)
        if decision.hold:
            # reason/metadata carry the evaluator's provenance so the hold is
            # explainable. Note advance() logs but does not persist them — see
            # the audit gap in docs/ARCHITECTURE_OLTP_BOUNDARY.md.
            t2 = self.advance(
                txn_id,
                TxnState.COMPLIANCE_HOLD,
                actor=actor,
                reason=decision.reason,
                metadata=dict(decision.metadata, evaluator=decision.evaluator_version),
            )
            transitions.append(t2)

        t3 = self.advance(txn_id, TxnState.APPROVED, actor=actor, reason="auto_approved")
        transitions.append(t3)
        return transitions

    def close(self) -> None:
        self._store.close()

    def __enter__(self) -> "TransactionStateMachine":
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()