"""Operational (OLTP) SQLite access — separate from the analytics warehouse.

`cce_platform.L1_mechanism.db` owns the analytics warehouse: fifteen-odd tables that the
batch pipeline truncates and rebuilds on every run. This module owns the two
operational tables (`outbox_events`, `settlement_schedule`) and nothing else.

Why a second file rather than reusing `cce_platform.L1_mechanism.db.connect`:

  The analytics warehouse is disposable by design — `reset_tables()` clears it
  and the pipeline repopulates it from source. Operational rows are the
  opposite: an unpublished outbox event or an unsettled trade cannot be
  recomputed from anything. Keeping them in one file made that distinction a
  convention enforced only by `reset_tables()` iterating `ANALYTICS_TABLES`.
  Separate files make it structural. `L0_schema/ops.py` already documented this
  intent ("In production this layer lives in a separate database"); this module
  is that sentence made true in the PoC.

What this costs, stated plainly:

  The Outbox pattern requires the event write to be atomic with the business
  state change it describes. That guarantee is NOT achieved here, and the split
  is not what breaks it — it was already broken. The business state this outbox
  describes lives in Redis (`txn:state:{txn_id}`), not in SQLite, so
  `sm.advance()` and the `UPDATE settlement_schedule` in
  `SettlementTrigger.run_once()` were always two stores with no shared
  transaction. The `SKIPPED_ADVANCED` reconciliation branch there is the scar
  from that gap.

  What the split does preserve is the only real atomicity opportunity that
  exists: `schedule_settlement()` and `write_outbox_event()` both target tables
  in *this* file, so they can share one transaction. See `writing_transaction`.

  Closing the Redis/SQLite gap properly means moving transaction state into the
  same transactional store as the outbox. That is now a change to one module
  instead of a change spread across the package.
"""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from ..L0_configuration import settings
from ..L1_mechanism import writing_transaction as writing_transaction
from ..L0_schema import OPERATIONAL_SCHEMA, OPERATIONAL_TABLES

__all__ = [
    "OPERATIONAL_TABLES",
    "connect",
    "init_schema",
    "session",
    "transaction",
    "writing_transaction",
]


def connect(db_path: Path | None = None) -> sqlite3.Connection:
    """Open a connection to the operational database.

    WAL and busy_timeout are set for the same reason as in `cce_platform.L1_mechanism.db`:
    the publisher and the settlement trigger poll concurrently, and the poll
    loops depend on a writer waiting out the busy timeout rather than failing
    immediately.
    """
    path = db_path or settings.oltp_sqlite_path
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA busy_timeout = 5000")
    return conn


def init_schema(conn: sqlite3.Connection) -> None:
    """Create the operational tables only.

    DDL comes from `L0_schema/ops.py` via the registry's `OPERATIONAL_SCHEMA`, so
    the table list has exactly one definition shared with the analytics side's
    disjointness guards.
    """
    conn.executescript(OPERATIONAL_SCHEMA)
    conn.commit()


@contextmanager
def session(db_path: Path | None = None) -> Iterator[sqlite3.Connection]:
    """Open a connection and guarantee it is closed, without owning the commits.

    For the polling loops, which commit per row rather than per batch: a failed
    delivery must not roll back the rows already marked SENT beside it.

    `sqlite3.Connection` used directly as a context manager commits or rolls
    back but does NOT close, so `with connect(...) as conn:` inside a loop that
    runs every POLL_INTERVAL_S leaks one connection per poll for the lifetime of
    the process. Use this instead.
    """
    conn = connect(db_path)
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def transaction(db_path: Path | None = None) -> Iterator[sqlite3.Connection]:
    """Open a connection, run a block as one atomic write, and close it.

    This is the intended entry point for writing an outbox event together with
    the business row it describes:

        with transaction() as conn:
            schedule_settlement(conn, txn_id, key, "INVESTMENT", 2100.0)
            write_outbox_event(conn, "settlement", txn_id, "SettlementScheduled", {...})

    Both statements land in one commit or neither does.

    `sqlite3.Connection` used as a context manager commits or rolls back but
    does NOT close the connection, which leaks one connection per poll in a
    long-running loop. This wrapper closes it.
    """
    conn = connect(db_path)
    try:
        with writing_transaction(conn):
            yield conn
    finally:
        conn.close()
