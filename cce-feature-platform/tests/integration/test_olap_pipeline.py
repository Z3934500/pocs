from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from cce_platform.L1_mechanism import connect
from cce_platform.L2_olap.batch_importer import export_gold_features_to_online_store
from cce_platform.L2_olap.online_store import LocalOnlineStore
from cce_platform.L2_olap.pipeline import normalize_identifier, resolve_unified_key, run_pipeline
from cce_platform.L2_olap.realtime import (
    process_cdc_events,
    sample_cdc_events,
    write_sample_cdc_events,
)


class CdcIdempotencyTest(unittest.TestCase):
    """At-least-once delivery must not inflate the real-time aggregates.

    A CDC stream redelivers on consumer restart, rebalance or an unacknowledged
    offset. `event_id` is already a deterministic uuid5 over
    (table, op, key, event_ts), so a redelivery is byte-identical and *is*
    detectable — nothing was consuming that identity.

    This is the failure mode the numbers cannot survive silently: the aggregate
    is a sum, so a duplicate does not surface as an error anywhere. It surfaces
    as a customer whose order count and spend are simply wrong, which then feeds
    `rt_intent_score` and campaign eligibility.
    """

    def _run(self, events: list, tmp: Path) -> dict:
        path = tmp / "cdc.jsonl"
        with path.open("w", encoding="utf-8") as file:
            for event in events:
                file.write(json.dumps(event.__dict__, sort_keys=True) + "\n")
        store = tmp / "store.json"
        if store.exists():
            store.unlink()
        process_cdc_events(events_path=path, store_path=store)
        return json.loads(store.read_text(encoding="utf-8"))

    def test_redelivered_event_does_not_double_count(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            tmp = Path(raw)
            event = sample_cdc_events()[0]
            once = self._run([event], tmp)
            twice = self._run([event, event], tmp)

        key = next(iter(once))
        self.assertEqual(
            twice[key]["rt_order_count_1d"], once[key]["rt_order_count_1d"],
            "a redelivered event with an identical event_id must be ignored; "
            "counting it twice corrupts the aggregate with no error surfaced",
        )
        self.assertEqual(
            twice[key]["rt_order_amount_1d"], once[key]["rt_order_amount_1d"],
            "duplicate delivery must not inflate the summed amount",
        )

    def test_distinct_events_still_accumulate(self) -> None:
        """The guard must dedupe, not collapse genuinely distinct events."""
        with tempfile.TemporaryDirectory() as raw:
            tmp = Path(raw)
            orders = [e for e in sample_cdc_events() if e.table == "orders"]
            result = self._run(orders, tmp)

        total = sum(row["rt_order_count_1d"] for row in result.values())
        self.assertEqual(
            total, len(orders),
            "every distinct order event must be counted exactly once",
        )


class CcePipelineTest(unittest.TestCase):
    def test_identity_normalization(self) -> None:
        self.assertEqual(normalize_identifier(" passport ", " e-7788990 "), ("PASSPORT", "E7788990"))
        self.assertEqual(resolve_unified_key("passport", "E7788990"), "U0001")

    def test_pipeline_builds_gold_features(self) -> None:
        counts = run_pipeline()
        self.assertGreaterEqual(counts["customers"], 6)
        self.assertGreaterEqual(counts["features"], 6)
        self.assertGreaterEqual(counts["policies"], 3)
        self.assertGreaterEqual(counts["policy_features"], 3)
        self.assertGreaterEqual(counts["identity_candidates"], 2)
        self.assertEqual(counts["model_scores"], counts["features"])
        self.assertGreaterEqual(counts["drift_checks"], 4)
        self.assertGreaterEqual(counts["dq_issues"], 1)
        with connect() as conn:
            priority_count = conn.execute(
                "SELECT COUNT(*) FROM gold_customer_features WHERE segment_name = 'Priority'"
            ).fetchone()[0]
            candidate = conn.execute(
                """
                SELECT resolution_action
                FROM silver_identity_candidates
                WHERE right_ref = 'AJO-3344' OR left_ref = 'AJO-3344'
                """
            ).fetchone()
            model_run_count = conn.execute("SELECT COUNT(*) FROM ml_model_runs").fetchone()[0]
        self.assertGreaterEqual(priority_count, 1)
        self.assertIsNotNone(candidate)
        self.assertEqual(candidate["resolution_action"], "review_attach_to_known_customer")
        self.assertEqual(model_run_count, 1)

    def test_batch_importer_and_realtime_stream_update_online_store(self) -> None:
        temp_dir = ROOT / "data" / "test_runtime"
        store_path = temp_dir / "feature_store_test.json"
        events_path = temp_dir / "cdc_events_test.jsonl"

        run_pipeline()
        batch_result = export_gold_features_to_online_store(store_path=store_path, replace=True)
        self.assertGreaterEqual(batch_result["customers_exported"], 6)

        write_sample_cdc_events(events_path)
        stream_result = process_cdc_events(events_path=events_path, store_path=store_path)
        self.assertEqual(stream_result["events_read"], 6)
        self.assertEqual(stream_result["unresolved_events"], 0)
        self.assertGreaterEqual(stream_result["customers_updated"], 4)

        u0001 = LocalOnlineStore(store_path).get("U0001")
        self.assertIsNotNone(u0001)
        assert u0001 is not None
        self.assertIn("monetary_30d", u0001)
        self.assertIn("propensity_score", u0001)
        self.assertGreaterEqual(u0001["rt_order_count_1d"], 1)
        self.assertGreater(u0001["rt_intent_score"], 0)


class PipelineCliTest(unittest.TestCase):
    """The CLI must not advertise a mode it does not have.

    `main()` parsed `choices=["run", "reset"]` and echoed the chosen name back in
    its JSON output, but called `run_pipeline(reset=True)` either way. The two
    commands were byte-identical in effect while the surface implied that `run`
    leaves the tables alone and `reset` clears them.

    C6 in `docs/ARCHITECTURE_OLTP_BOUNDARY.md` is what settles which side was
    wrong: "Gold is truncated and rebuilt on every pipeline run -- by design, not
    a defect." That invariant is load-bearing; it is the stated reason the
    analytics tables are separated from `outbox_events` and
    `settlement_schedule`, whose rows are irreplaceable. So truncation is
    unconditional, and a second command promising otherwise was the defect.

    The `reset=False` branch of `run_pipeline` was the same defect one layer
    down: no caller passed it, and passing it kept rows no builder reproduces --
    measured, an upstream record deleted between two runs survived as current
    data. Dead code that quietly breaks a documented invariant is a trap, so it
    is gone rather than guarded.
    """

    GHOST = ("GHOST_CLI", 0, 0, 0.0, 0, 0, 0, "stale", 0.0, "1970-01-01T00:00:00Z")
    COUNT = (
        "SELECT COUNT(*) FROM gold_customer_features "
        "WHERE unified_customer_key = 'GHOST_CLI'"
    )

    def tearDown(self) -> None:
        """Leave the shared warehouse as the other tests expect to find it."""
        run_pipeline()

    def _plant_ghost(self) -> None:
        with connect() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO gold_customer_features "
                "VALUES (?,?,?,?,?,?,?,?,?,?)",
                self.GHOST,
            )
            conn.commit()

    def _ghost_rows(self) -> int:
        with connect() as conn:
            return conn.execute(self.COUNT).fetchone()[0]

    def _cli(self, command: str | None = None) -> dict:
        done = self._cli_raw(command)
        self.assertEqual(done.returncode, 0, done.stderr)
        return json.loads(done.stdout)

    def _cli_raw(self, command: str | None) -> subprocess.CompletedProcess[str]:
        env = {**os.environ, "PYTHONPATH": str(ROOT / "src")}
        argv = [sys.executable, "-m", "cce_platform.L2_olap.pipeline"]
        if command is not None:
            argv.append(command)
        return subprocess.run(argv, cwd=ROOT, env=env, capture_output=True, text=True)

    def test_run_truncates_orphan_rows(self) -> None:
        """C6: truncate-and-rebuild is unconditional."""
        self._plant_ghost()
        self.assertEqual(self._ghost_rows(), 1, "fixture did not plant the row")
        self._cli("run")
        self.assertEqual(
            self._ghost_rows(),
            0,
            "a row no builder reproduces must not survive a rebuild; it would be "
            "stale data served as if it were current",
        )

    def test_reset_is_not_advertised_as_a_separate_mode(self) -> None:
        """A rejected argument is honest; an accepted no-op is not.

        `reset` used to be accepted and echoed back while doing exactly what
        `run` does. Failing loudly is the only outcome that cannot mislead a
        caller into believing it selected different behaviour.
        """
        done = self._cli_raw("reset")
        self.assertNotEqual(
            done.returncode,
            0,
            "`reset` must not be silently accepted as if it selected a mode "
            "distinct from `run`",
        )

    def test_default_invocation_matches_explicit_run(self) -> None:
        """The k8s CronJob passes `run`; a bare call must not diverge from it."""
        self.assertEqual(self._cli(None)["counts"], self._cli("run")["counts"])

    def test_run_reports_the_command_and_builds_the_full_gold_layer(self) -> None:
        """The counts are the pipeline's receipt: no mode may skip work."""
        payload = self._cli("run")
        self.assertEqual(payload["command"], "run")
        self.assertGreaterEqual(payload["counts"]["features"], 6)
        self.assertGreaterEqual(payload["counts"]["customers"], 6)


if __name__ == "__main__":
    unittest.main()
