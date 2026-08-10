#!/usr/bin/env bash
# [kb-land] Source: SCHEDULED_SCALING_AND_RDS.md + ORDER_PAYMENT_FLOW.md
# Pattern: Pre-stop safety check — run BEFORE stopping RDS / scaling to zero.
#          Blocks the stop if any of the following conditions are true:
#            1. Outbox table still has un-relayed events (depth > threshold)
#            2. Oldest pending outbox event is too recent (relay hasn't caught up)
#            3. Any order is in PAYMENT_UNKNOWN state (timeout window still open)
#
# Usage:
#   ./scripts/pre-stop-check.sh [--dry-run]
#   Exit 0 = safe to stop.  Exit 1 = NOT safe, inspect output.
#
# Required env vars:
#   DB_HOST, DB_PORT (default 5432), DB_NAME, DB_USER, DB_PASSWORD
#   OUTBOX_DEPTH_THRESHOLD  (default 10)
#   OUTBOX_MAX_AGE_SECONDS  (default 120)  oldest un-relayed event must be older than this
#   PSQL                    (default psql)

set -euo pipefail

DRY_RUN=false
for arg in "$@"; do
  [[ "$arg" == "--dry-run" ]] && DRY_RUN=true
done

DB_HOST="${DB_HOST:-localhost}"
DB_PORT="${DB_PORT:-5432}"
DB_NAME="${DB_NAME:-oms_order}"
DB_USER="${DB_USER:-oms}"
OUTBOX_DEPTH_THRESHOLD="${OUTBOX_DEPTH_THRESHOLD:-10}"
OUTBOX_MAX_AGE_SECONDS="${OUTBOX_MAX_AGE_SECONDS:-120}"
PSQL="${PSQL:-psql}"

FAIL=0

log()  { echo "[pre-stop-check] $*"; }
fail() { echo "[pre-stop-check] FAIL: $*" >&2; FAIL=1; }

# ── 1. Outbox depth ──────────────────────────────────────────────────────────
log "Checking outbox depth (threshold=${OUTBOX_DEPTH_THRESHOLD})…"
OUTBOX_DEPTH=$("$PSQL" \
  "host=${DB_HOST} port=${DB_PORT} dbname=${DB_NAME} user=${DB_USER}" \
  --tuples-only --no-align \
  -c "SELECT COUNT(*) FROM order_outbox_event WHERE status IN ('PENDING','IN_FLIGHT');")

log "  outbox depth = ${OUTBOX_DEPTH}"
if (( OUTBOX_DEPTH > OUTBOX_DEPTH_THRESHOLD )); then
  fail "Outbox depth ${OUTBOX_DEPTH} exceeds threshold ${OUTBOX_DEPTH_THRESHOLD}." \
       "Wait for relay to drain before stopping."
fi

# ── 2. Oldest un-relayed event age ───────────────────────────────────────────
log "Checking age of oldest pending outbox event (max_age=${OUTBOX_MAX_AGE_SECONDS}s)…"
OLDEST_AGE_SECONDS=$("$PSQL" \
  "host=${DB_HOST} port=${DB_PORT} dbname=${DB_NAME} user=${DB_USER}" \
  --tuples-only --no-align \
  -c "SELECT COALESCE(
         EXTRACT(EPOCH FROM (NOW() - MIN(created_at)))::int,
         -1
       )
       FROM order_outbox_event
       WHERE status IN ('PENDING','IN_FLIGHT');")

log "  oldest pending event age = ${OLDEST_AGE_SECONDS}s"
if (( OLDEST_AGE_SECONDS != -1 && OLDEST_AGE_SECONDS < OUTBOX_MAX_AGE_SECONDS )); then
  fail "Oldest pending outbox event is only ${OLDEST_AGE_SECONDS}s old." \
       "Relay may not have processed it yet."
fi

# ── 3. PAYMENT_UNKNOWN orders ─────────────────────────────────────────────────
# [kb-land] Source: ORDER_PAYMENT_FLOW.md
# Pattern: PAYMENT_UNKNOWN means a capture timed out and no status-query has resolved it.
#          Never stop RDS while any order is stuck here — the reconciliation job needs DB.
log "Checking for PAYMENT_UNKNOWN orders…"
UNKNOWN_COUNT=$("$PSQL" \
  "host=${DB_HOST} port=${DB_PORT} dbname=${DB_NAME} user=${DB_USER}" \
  --tuples-only --no-align \
  -c "SELECT COUNT(*) FROM orders WHERE status = 'PAYMENT_UNKNOWN';")

log "  PAYMENT_UNKNOWN orders = ${UNKNOWN_COUNT}"
if (( UNKNOWN_COUNT > 0 )); then
  fail "Found ${UNKNOWN_COUNT} order(s) in PAYMENT_UNKNOWN state." \
       "Run payment reconciliation job first."
fi

# ── Result ────────────────────────────────────────────────────────────────────
if (( FAIL == 1 )); then
  log "PRE-STOP CHECK FAILED — do not stop RDS until all issues above are resolved."
  exit 1
fi

log "PRE-STOP CHECK PASSED — safe to stop RDS / scale to zero."
if [[ "$DRY_RUN" == "true" ]]; then
  log "(dry-run mode — no action taken)"
fi
exit 0
