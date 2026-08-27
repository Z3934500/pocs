from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from ..L0_configuration import settings
from ..L0_schema import ANALYTICS_SCHEMA, ANALYTICS_TABLES


def connect(db_path: Path | None = None) -> sqlite3.Connection:
    path = db_path or settings.sqlite_path
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    # The batch pipeline holds a write transaction for its whole run while the
    # FastAPI process serves reads from the same file. Without WAL those readers
    # get "database is locked" instead of the pre-write snapshot; without a busy
    # timeout a writer that arrives mid-read fails immediately rather than
    # waiting. journal_mode is persistent in the file, busy_timeout is per
    # connection, so both are set on every connect.
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA busy_timeout = 5000")
    return conn


def init_schema(conn: sqlite3.Connection) -> None:
    """Create the analytics tables.

    The DDL comes from the per-layer modules under `L0_schema/`. Only the analytics
    layers are created here: the two operational tables belong to a different
    database owned by `cce_platform.L2_oltp.store`, because they hold state no
    pipeline can recompute and must not share a lifecycle with tables that are
    truncated and rebuilt on every run.
    """
    conn.executescript(ANALYTICS_SCHEMA)
    conn.commit()


@contextmanager
def writing_transaction(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """Run a block as one atomic write, taking the write lock up front.

    BEGIN IMMEDIATE acquires the RESERVED lock now rather than on the first
    write. Under the default DEFERRED transaction a reader that later upgrades
    to a writer can find another writer already there and fail mid-way with
    SQLITE_BUSY, having done real work; taking the lock at the start means a
    competing writer waits out busy_timeout at the boundary instead.

    Commits on success, rolls back on any exception, so readers see either the
    whole batch or none of it.
    """
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
    except BaseException:
        conn.rollback()
        raise
    conn.commit()


def reset_tables(conn: sqlite3.Connection) -> None:
    """Truncate every analytics table. Does NOT commit.

    The table list is derived from the analytics layer modules, so a table added
    to one of them is cleared here automatically. Operational tables
    (`outbox_events`, `settlement_schedule`) are excluded by construction: they
    hold in-flight state that no pipeline can recompute, and truncating them
    would drop unpublished events and unsettled trades.

    The caller owns the transaction: truncating and repopulating have to land in
    one commit, or readers observe the empty tables in between. See run_pipeline.
    """
    for table_name in ANALYTICS_TABLES:
        conn.execute(f"DELETE FROM {table_name}")
