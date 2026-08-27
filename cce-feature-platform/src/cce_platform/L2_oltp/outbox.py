"""
Transactional Outbox + T+2 Settlement Scheduler for CCE.

=== Transactional Outbox ===

Problem (from interview doc):
  "收到支付回调 → 更新DB状态 → 发Kafka" 中间挂掉 → 事件丢失。

Solution:
  Step 1 (业务代码): 在同一个SQLite事务里同时写业务表 + outbox_events表。
  Step 2 (EventPublisher): 后台轮询 outbox_events WHERE status='PENDING'，
         发送到下游（本地PoC写文件/回调；生产写Kafka），成功后标记SENT。
  Step 3 (消费者): 用event_id做幂等去重，避免重复处理。

=== T+2 Settlement Scheduler ===

Problem:PREMIUM_FINANCING/INVESTMENT等产品成交后不能立即交收，需等T+2工作日。
  简单 time.sleep(48h) 不考虑周末/假期，会在错误时间触发。

Solution:
  - 写入 settlement_schedule 表时计算 settle_ts（考虑Holiday Calendar）。
  - SettlementTrigger 轮询 WHERE settle_ts <= now AND status='PENDING_SETTLE'，
    触发状态机 advance(SETTLEMENT_IN_PROGRESS)。
  - 结合 oltp.state_machine 的乐观锁，防止重复触发（文档追问二场景）。

Holiday Calendar:
  本地PoC用硬编码的SG/HK节假日列表。
  生产环境从Consul KV读取，支持临时休市（台风/国殇日）零代码更新。

Usage:
  from cce_platform.L2_oltp import (
      write_outbox_event, EventPublisher,
      schedule_settlement, SettlementTrigger,
      HolidayCalendar, transaction,
  )

  # 业务代码里（同一事务）:
  # transaction() 打开操作型数据库并保证 commit/rollback/close，
  # settlement_schedule 与 outbox_events 同在一个文件，故可共享一次提交。
  with transaction() as conn:
      schedule_settlement(conn, "TXN-001", "U0005", "INVESTMENT", 2100.0)
      write_outbox_event(conn, aggregate_type="settlement", aggregate_id="TXN-001",
                         event_type="SettlementScheduled", payload={"amount": 2100.0})

  # 注意：事务状态本身在 Redis，不在此库，
  # 所以 sm.advance() 与上面的写入无法共享事务 —
  # 参见 docs/ARCHITECTURE_OLTP_BOUNDARY.md 的一致性边界一节。

  # 后台服务:
  publisher = EventPublisher()
  publisher.run_once()   # 单次轮询（测试用）
  publisher.run_loop()   # 持续轮询（生产用，独立线程）
"""

from __future__ import annotations

import json
import logging
import threading
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, UTC
from pathlib import Path
from typing import Any, Callable

from ..L0_configuration import settings
from ..L0_primitives import stable_issue_id
# PRODUCT_T_PLUS is re-exported (`as` form) for callers that read the whole
# table, e.g. chaos_testing/validate_chaos.py. settlement_t_plus() is the
# accessor this module itself uses.
from ..L1_business_data import PRODUCT_T_PLUS as PRODUCT_T_PLUS
from ..L1_business_data import settlement_t_plus
from .store import init_schema, session

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Holiday Calendar
# ---------------------------------------------------------------------------

# Singapore + Hong Kong public holidays 2025-2026 (ISO dates)
# Production: load from Consul KV "cce/config/holiday_calendar"
_DEFAULT_HOLIDAYS: frozenset[date] = frozenset(map(date.fromisoformat, [
    # SG 2025
    "2025-01-01", "2025-01-29", "2025-01-30", "2025-04-18",
    "2025-05-01", "2025-05-12", "2025-08-09", "2025-10-20",
    "2025-12-25",
    # SG 2026
    "2026-01-01", "2026-02-17", "2026-02-18", "2026-04-03",
    "2026-05-01", "2026-05-31", "2026-08-10", "2026-11-09",
    "2026-12-25",
    # HK 2026 (for HKEX products)
    "2026-01-01", "2026-01-28", "2026-01-29", "2026-04-03",
    "2026-04-06", "2026-05-01", "2026-05-25", "2026-07-01",
    "2026-10-01", "2026-10-26", "2026-12-25",
]))


class HolidayCalendar:
    """
    Determines settlement dates accounting for weekends and public holidays.

    Production note: override holidays via Consul KV so typhoon closures
    or ad-hoc exchange halts can be updated without a code deploy.
    """

    def __init__(self, holidays: frozenset[date] | None = None) -> None:
        self._holidays = holidays or _DEFAULT_HOLIDAYS

    def is_business_day(self, d: date) -> bool:
        return d.weekday() < 5 and d not in self._holidays

    def next_business_day(self, d: date, n: int = 1) -> date:
        """Return the nth business day on or after d."""
        current = d
        count = 0
        while count < n:
            current += timedelta(days=1)
            if self.is_business_day(current):
                count += 1
        return current

    def settle_date(self, trade_date: date, t_plus: int = 2) -> date:
        """
        Compute settlement date for T+N products.
        T+0 = same day (if business day, else next); T+2 = standard equities.
        """
        if t_plus == 0:
            return trade_date if self.is_business_day(trade_date) else self.next_business_day(trade_date, 1)
        return self.next_business_day(trade_date, t_plus)

    def settle_ts(self, trade_date: date, t_plus: int = 2, market_open_hour: int = 9) -> float:
        """Return Unix timestamp of settlement date at market open (09:00 SGT = UTC+8)."""
        sd = self.settle_date(trade_date, t_plus)
        # SGT = UTC+8; convert to UTC for storage
        dt = datetime(sd.year, sd.month, sd.day, market_open_hour, 0, 0)
        sgt_offset = 8 * 3600
        return dt.timestamp() - sgt_offset


# Settlement cycles are market-defined business policy, loaded from config
# rather than hardcoded here — see policy.py and settlement_t_plus().


# ---------------------------------------------------------------------------
# Outbox helpers
# ---------------------------------------------------------------------------

def write_outbox_event(
    conn,
    aggregate_type: str,
    aggregate_id: str,
    event_type: str,
    payload: dict[str, Any],
    dedup_key: str | None = None,
) -> str:
    """
    Write an event to the outbox table within the caller's transaction.MUST be called before conn.commit() so the business update and the
    outbox write are atomically committed together.

    Returns the generated event_id for traceability.

    `dedup_key` makes a producer-side retry idempotent. `event_id` is the table's
    PRIMARY KEY and the insert below is `INSERT OR IGNORE`, so the guard already
    exists at the storage layer — what it needs is an id a retry can reproduce.
    Pass a key derived from the business fact (`f"{order_id}:OrderPaid"`) and the
    second write of the same fact collides and is ignored.

    Omitting it keeps the previous behaviour: the clock is mixed in, so every
    call mints a distinct id. That is the correct default rather than hashing the
    payload, because two legitimately identical business events — the same
    customer buying the same product for the same amount twice — must both
    survive. Only the caller knows which of the two situations it is in, so the
    decision stays with the caller instead of being guessed here.
    """
    seed = dedup_key if dedup_key is not None else str(time.time())
    event_id = stable_issue_id("outbox", aggregate_type, aggregate_id, event_type, seed)
    now = datetime.now(UTC).isoformat(timespec="seconds")
    conn.execute(
        """
        INSERT OR IGNORE INTO outbox_events(event_id, aggregate_type, aggregate_id, event_type, payload_json, status, created_at)
        VALUES (?, ?, ?, ?, ?, 'PENDING', ?)
        """,
        (event_id, aggregate_type, aggregate_id,
         event_type, json.dumps(payload, sort_keys=True), now),
    )
    logger.debug("outbox: queued %s %s/%s", event_type, aggregate_type, aggregate_id)
    return event_id


# ---------------------------------------------------------------------------
# Settlement schedule helpers
# ---------------------------------------------------------------------------

_calendar = HolidayCalendar()


def schedule_settlement(
    conn,
    txn_id: str,
    customer_key: str,
    product: str,
    amount: float,
    trade_date: date | None = None,
    calendar: HolidayCalendar | None = None,
) -> dict[str, Any]:
    """
    Insert a settlement schedule row for a financial transaction.

    Called in the same transaction as order state change to APPROVED,
    ensuring atomicity (Outbox pattern applied to settlement scheduling).

    Returns the schedule record for logging/audit.
    """
    cal = calendar or _calendar
    td = trade_date or date.today()
    t_plus = settlement_t_plus(product)
    sd = cal.settle_date(td, t_plus)
    sts = cal.settle_ts(td, t_plus)
    now = datetime.now(UTC).isoformat(timespec="seconds")

    conn.execute(
        """
        INSERT OR REPLACE INTO settlement_schedule
        (txn_id, unified_customer_key, product, amount,
         trade_date, settle_date, settle_ts, status, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, 'PENDING_SETTLE', ?)
        """,
        (txn_id, customer_key, product.upper(), amount,
         td.isoformat(), sd.isoformat(), sts, now),
    )

    record = {
        "txn_id":       txn_id,
        "product":      product.upper(),
        "amount":       amount,
        "trade_date":   td.isoformat(),
        "settle_date":  sd.isoformat(),
        "settle_ts":    sts,
        "t_plus":       t_plus,}
    logger.info(
        "settlement_schedule: txn=%s product=%s T+%d trade=%s → settle=%s",
        txn_id, product, t_plus, td, sd,
    )
    return record


# ---------------------------------------------------------------------------
# EventPublisher
# ---------------------------------------------------------------------------

@dataclass
class PublishResult:
    event_id: str
    success:  bool
    error:    str = ""


class EventPublisher:
    """
    Polls outbox_events WHERE status='PENDING' and forwards to downstream.

    Default downstream: local CDC events file (PoC mode).
    Production downstream: Kafka producer with acks=all.

    Guarantees:
      - At-least-once delivery (retry on failure, idempotency via event_id)
      - Marks SENT only after downstream confirms receipt- MAX_RETRY=3; after that marks FAILED for dead-letter inspection
    """

    MAX_RETRY   = 3
    BATCH_SIZE  = 50
    POLL_INTERVAL_S = 2.0

    def __init__(
        self,
        downstream: Callable[[dict[str, Any]], bool] | None = None,
        db_path: Path | None = None,
    ) -> None:
        self._downstream = downstream or self._default_downstream
        self._db_path = db_path
        self._stop_event = threading.Event()
        self._schema_ready = False

    def _ensure_schema(self, conn) -> None:
        """Create the operational tables once per instance, not once per poll.

        `executescript()` issues an implicit COMMIT before running, so calling
        it on every poll committed whatever transaction happened to be open and
        re-ran the DDL for the lifetime of the loop. The statements are
        IF NOT EXISTS so re-running was harmless, but the implicit commit was
        not.
        """
        if not self._schema_ready:
            init_schema(conn)
            self._schema_ready = True

    @staticmethod
    def _default_downstream(event: dict[str, Any]) -> bool:
        """PoC downstream: append event to the CDC events file so
        flink_cdc_pipeline can pick it up on next run.
        """
        try:
            target = settings.cdc_events_path
            target.parent.mkdir(parents=True, exist_ok=True)
            with target.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(event, sort_keys=True) + "\n")
            return True
        except Exception as exc:
            logger.error("EventPublisher default downstream error: %s", exc)
            return False

    def run_once(self) -> list[PublishResult]:
        """
        Process one batch of pending outbox events.
        Returns list of PublishResult for observability / testing.
        """
        results: list[PublishResult] = []
        with session(self._db_path) as conn:
            self._ensure_schema(conn)
            rows = conn.execute(
                """
                SELECT event_id, aggregate_type, aggregate_id, event_type,
                       payload_json, retry_count
                FROM outbox_events
                WHERE status = 'PENDING'
                ORDER BY created_at
                LIMIT ?
                """,
                (self.BATCH_SIZE,),
            ).fetchall()

            for row in rows:
                event_id    = row["event_id"]
                payload     = json.loads(row["payload_json"])
                retry_count = row["retry_count"]

                # Enrich with standard fields for downstream idempotency
                envelope = {
                    "event_id":      event_id,
                    "event_type":    row["event_type"],
                    "aggregate_type": row["aggregate_type"],
                    "aggregate_id":  row["aggregate_id"],
                    "payload":       payload,
                    "published_at":  datetime.now(UTC).isoformat(timespec="seconds"),
                }

                success = False
                try:
                    success = self._downstream(envelope)
                except Exception as exc:
                    logger.warning("EventPublisher send error event=%s: %s", event_id, exc)

                now = datetime.now(UTC).isoformat(timespec="seconds")
                if success:
                    conn.execute(
                        "UPDATE outbox_events SET status='SENT', sent_at=? WHERE event_id=?",
                        (now, event_id),
                    )
                    logger.info("outbox: SENT event_id=%s type=%s", event_id, row["event_type"])
                else:
                    new_retry = retry_count + 1
                    new_status = "FAILED" if new_retry >= self.MAX_RETRY else "PENDING"
                    conn.execute(
                        "UPDATE outbox_events SET status=?, retry_count=? WHERE event_id=?",
                        (new_status, new_retry, event_id),
                    )
                    if new_status == "FAILED":
                        logger.error(
                            "outbox: FAILED (max retries) event_id=%s type=%s",
                            event_id, row["event_type"],
                        )

                conn.commit()
                results.append(PublishResult(event_id=event_id, success=success))

        return results

    def run_loop(self, poll_interval_s: float | None = None) -> None:
        """
        Run the publisher in a blocking loop until stop() is called.Intended to be run in a daemon thread.
        """
        interval = poll_interval_s or self.POLL_INTERVAL_S
        logger.info("EventPublisher: starting loop (interval=%.1fs)", interval)
        while not self._stop_event.is_set():
            try:
                results = self.run_once()
                if results:
                    sent    = sum(1 for r in results if r.success)
                    failed  = len(results) - sent
                    logger.debug("EventPublisher: batch sent=%d failed=%d", sent, failed)
            except Exception as exc:
                logger.error("EventPublisher loop error: %s", exc)
            self._stop_event.wait(interval)

    def start_background(self, poll_interval_s: float | None = None) -> threading.Thread:
        """Launch publisher in a background daemon thread."""
        t = threading.Thread(
            target=self.run_loop,
            args=(poll_interval_s,),
            daemon=True,
            name="outbox-publisher",
        )
        t.start()
        return t

    def stop(self) -> None:
        self._stop_event.set()


# ---------------------------------------------------------------------------
# SettlementTrigger
# ---------------------------------------------------------------------------

class SettlementTrigger:
    """
    Polls settlement_schedule WHERE settle_ts <= now AND status='PENDING_SETTLE'
    and advances the TransactionStateMachine to SETTLEMENT_IN_PROGRESS.

    Exactly-once safety:
      - State machine WATCH/MULTI/EXEC prevents double-trigger (乐观锁)
      - settle_ts index makes polling O(log n) even with millions of rows
      - On service restart: rows still PENDING_SETTLE are re-queried and
        re-triggered; idempotency is guaranteed by the state machine

    T+2 example timeline (SGT):
      Monday 14:30  → trade executed (APPROVED)
      Monday 14:30  → schedule_settlement() writes settle_ts = Wednesday 09:00 SGT
      Wednesday 09:00 → SettlementTrigger fires → SETTLEMENT_IN_PROGRESS
      Wednesday 09:05 → settlement completes → SETTLED
    """

    BATCH_SIZE      = 100
    POLL_INTERVAL_S = 10.0

    def __init__(
        self,
        state_machine=None,
        db_path: Path | None = None,
    ) -> None:
        self._sm = state_machine
        self._db_path = db_path
        self._stop_event = threading.Event()
        self._schema_ready = False

    def _ensure_schema(self, conn) -> None:
        """Create the operational tables once per instance — see
        EventPublisher._ensure_schema for why this is not per-poll."""
        if not self._schema_ready:
            init_schema(conn)
            self._schema_ready = True

    def _get_state_machine(self):
        # Cached after first construction. TransactionStateMachine() builds a
        # connection pool and pings Redis on every instantiation, and this
        # helper is called once per run_once() plus once per
        # complete_settlement() — i.e. every POLL_INTERVAL_S seconds for the
        # lifetime of the loop. Constructing it per call also moves a Redis
        # outage from "fails once at startup" to "raises on every poll".
        if self._sm is None:
            from .state_machine import TransactionStateMachine
            self._sm = TransactionStateMachine()
        return self._sm

    def run_once(self) -> list[str]:
        """
        Trigger all due settlements. Returns list of txn_ids triggered.
        """
        now_ts = time.time()
        triggered: list[str] = []

        with session(self._db_path) as conn:
            self._ensure_schema(conn)
            rows = conn.execute(
                """
                SELECT txn_id, unified_customer_key, product, amount, settle_date
                FROM settlement_schedule
                WHERE settle_ts <= ? AND status = 'PENDING_SETTLE'
                ORDER BY settle_ts
                LIMIT ?
                """,
                (now_ts, self.BATCH_SIZE),
            ).fetchall()

            sm = self._get_state_machine()
            for row in rows:
                txn_id = row["txn_id"]
                try:
                    from .state_machine import TxnState, TransactionNotFoundError, InvalidTransitionError
                    try:
                        sm.advance(
                            txn_id,
                            TxnState.SETTLEMENT_IN_PROGRESS,
                            actor="settlement_trigger",
                                        reason=f"T+2 settle_date={row['settle_date']}",
                            metadata={
                                "product":      row["product"],
                                "amount":       row["amount"],
                                "customer_key": row["unified_customer_key"],
                            },
                        )
                        # Mark as in-progress in the schedule table too
                        conn.execute(
                            "UPDATE settlement_schedule SET status='SETTLEMENT_IN_PROGRESS' WHERE txn_id=?",
                            (txn_id,),
                        )
                        conn.commit()
                        triggered.append(txn_id)
                        logger.info(
                            "settlement_trigger: fired txn=%s product=%s amount=%.2f settle_date=%s",
                            txn_id, row["product"], row["amount"], row["settle_date"],
                        )
                    except TransactionNotFoundError:
                        # Transaction was never registered in state machine
                        # (e.g. created before state machine was deployed)
                        # Mark as skipped to avoid infinite retry
                        conn.execute(
                            "UPDATE settlement_schedule SET status='SKIPPED_NOT_FOUND' WHERE txn_id=?",
                            (txn_id,),
                        )
                        conn.commit()
                        logger.warning("settlement_trigger: txn=%s not in state machine, skipped", txn_id)
                    except InvalidTransitionError as exc:
                        # Already advanced past PENDING_SETTLE (concurrent trigger)
                        conn.execute(
                            "UPDATE settlement_schedule SET status='SKIPPED_ADVANCED' WHERE txn_id=?",
                            (txn_id,),
                        )
                        conn.commit()
                        logger.info(
                            "settlement_trigger: txn=%s already advanced (%s), skipped", txn_id, exc
                        )
                except Exception as exc:
                    logger.error("settlement_trigger: error on txn=%s: %s", txn_id, exc)

        return triggered

    def complete_settlement(self, txn_id: str) -> bool:
        """
        Mark a settlement as SETTLED in both the state machine and the schedule table.
        Called by the settlement worker after fund transfer confirmation.
        """
        try:
            from .state_machine import TxnState
            sm = self._get_state_machine()
            sm.advance(txn_id, TxnState.SETTLED, actor="settlement_worker", reason="funds_transferred")
            with session(self._db_path) as conn:
                now = datetime.now(UTC).isoformat(timespec="seconds")
                conn.execute(
                    "UPDATE settlement_schedule SET status='SETTLED', settled_at=? WHERE txn_id=?",
                    (now, txn_id),
                )
                conn.commit()
            logger.info("settlement: SETTLED txn=%s", txn_id)
            return True
        except Exception as exc:
            logger.error("settlement: complete_settlement error txn=%s: %s", txn_id, exc)
            return False

    def run_loop(self, poll_interval_s: float | None = None) -> None:
        interval = poll_interval_s or self.POLL_INTERVAL_S
        logger.info("SettlementTrigger: starting loop (interval=%.1fs)", interval)
        while not self._stop_event.is_set():
            try:
                triggered = self.run_once()
                if triggered:
                    logger.info("SettlementTrigger: triggered %d settlements", len(triggered))
            except Exception as exc:
                logger.error("SettlementTrigger loop error: %s", exc)
            self._stop_event.wait(interval)

    def start_background(self, poll_interval_s: float | None = None) -> threading.Thread:
        t = threading.Thread(
            target=self.run_loop,
            args=(poll_interval_s,),
            daemon=True,
            name="settlement-trigger",
        )
        t.start()
        return t

    def stop(self) -> None:
        self._stop_event.set()