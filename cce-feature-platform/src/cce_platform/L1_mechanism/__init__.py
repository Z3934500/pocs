"""Mechanism layer — how a store is reached, never what is stored or why.

Layer 1: depends on `L0_configuration` for paths and runtime flags and on
`L0_schema` for the analytics table definitions, and on nothing else in
`cce_platform`. Both of those are layer 0 — they have no internal imports at all
— so this stays a one-directional reach downward. Two backends live here:

  db.py          SQLite connections — WAL journal mode, busy timeout,
                 `BEGIN IMMEDIATE` write transactions, table truncation
  kv_backend.py  the key-value backend decision (Redis when reachable, a
                 JSON-file ZSET emulation otherwise) plus that emulation

Nothing here decides anything a business would recognise. `writing_transaction`
knows that a batch must land atomically but not which tables matter;
`make_kv_backend` knows that a per-process fallback would let replicas diverge
but not what state is being stored. The decisions belong to the callers —
`L2_olap/pipeline.py`, `cce_platform.L2_oltp`, `L2_olap/cart_zset.py`.

That split is what lets both sides of the write-authority boundary share this
layer without violating the boundary: `L2_oltp` may import from here, and this
layer imports nothing back. `L2_oltp/ports.py` goes one step further and declares
the interface it needs, so the transaction state machine depends on six named
operations rather than on `kv_backend.LocalZSetStore`, which is a PoC artifact.
"""

from __future__ import annotations

from .db import connect, init_schema, reset_tables, writing_transaction
from .kv_backend import (
    LOCAL_MODE,
    REDIS_MODE,
    LocalZSetStore,
    make_kv_backend,
    make_redis_client,
)

__all__ = [
    "LOCAL_MODE",
    "LocalZSetStore",
    "REDIS_MODE",
    "connect",
    "init_schema",
    "make_kv_backend",
    "make_redis_client",
    "reset_tables",
    "writing_transaction",
]
