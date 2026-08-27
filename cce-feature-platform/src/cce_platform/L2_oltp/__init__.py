"""Transactional (OLTP) domain — the write-authority side of the platform.

Everything else in `cce_platform` is an analytics or serving concern: it reads
source data, derives features, and publishes them. Those outputs are
recomputable — the pipeline truncates and rebuilds them on every run, and a
lost row is a rerun away from being restored.

This package is different in kind. It owns state that no pipeline can
recompute:

  txn:state:{txn_id}    append-only transition history (Redis ZSET)
  txn:meta:{txn_id}     transaction attributes (Redis hash)
  outbox_events         events not yet delivered downstream (SQLite)
  settlement_schedule   settlement obligations not yet discharged (SQLite)

A lost analytics row is a rerun. A lost settlement obligation is money that
never moves and a finding in the next audit. That asymmetry — not the choice of
Redis, not the file layout — is why this code sits behind its own boundary.

Boundary rule
-------------
Imports may point from `cce_platform.L2_oltp` into `cce_platform.*`. They may NOT
point the other way. No analytics or serving module may import from this
package; the batch pipeline, the feature API and the online store must remain
able to run with this package absent. `tests/test_oltp.py` asserts the rule so
it degrades loudly rather than silently.

The consequence worth stating: the Gold tables and `cce:features:{key}` are a
*serving projection*, not a source of truth. For a balance, a payment or a final
order state, the authoritative answer comes from here.

Why this is a subpackage and not a separate top-level package: it is not a
separate deployable, and naming it one would be a claim the Dockerfile does not
support. Its dependency closure still runs through `cce_platform.L0_configuration`,
`cce_platform.L1_mechanism` and `cce_platform.L1_business_data`. The boundary that matters
is the enforced import direction, not the nesting depth.

See docs/ARCHITECTURE_OLTP_BOUNDARY.md for the position of this block in the
architecture, its consistency and failure model, and its linkage to
risk-control requirements.
"""

from __future__ import annotations

from .audit import AuditRecord, decode_member, encode_member
from .adapters import LocalZSetAdapter, RedisZSetAdapter
from .outbox import (
    EventPublisher,
    HolidayCalendar,
    PRODUCT_T_PLUS,
    PublishResult,
    SettlementTrigger,
    schedule_settlement,
    write_outbox_event,
)
from .ports import ZSetStore
from .risk import (
    RiskDecision,
    RiskEvaluator,
    ThresholdRiskEvaluator,
)
from .state_machine import (
    ConcurrentModificationError,
    InvalidTransitionError,
    StateTransition,
    TransactionNotFoundError,
    TransactionRecord,
    TransactionStateMachine,
    TxnState,
)
from .store import connect, init_schema, transaction

__all__ = [
    "AuditRecord",
    "ConcurrentModificationError",
    "EventPublisher",
    "HolidayCalendar",
    "InvalidTransitionError",
    "LocalZSetAdapter",
    "PRODUCT_T_PLUS",
    "PublishResult",
    "RedisZSetAdapter",
    "RiskDecision",
    "RiskEvaluator",
    "SettlementTrigger",
    "StateTransition",
    "ThresholdRiskEvaluator",
    "TransactionNotFoundError",
    "TransactionRecord",
    "TransactionStateMachine",
    "TxnState",
    "ZSetStore",
    "connect",
    "decode_member",
    "encode_member",
    "init_schema",
    "schedule_settlement",
    "transaction",
    "write_outbox_event",
]
