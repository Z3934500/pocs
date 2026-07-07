from __future__ import annotations

import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from cce_platform.db import connect
from cce_platform.batch_importer import export_gold_features_to_online_store
from cce_platform.online_store import LocalOnlineStore
from cce_platform.pipeline import normalize_identifier, resolve_unified_key, run_pipeline
from cce_platform.realtime import process_cdc_events, write_sample_cdc_events


class CcePipelineTest(unittest.TestCase):
    def test_identity_normalization(self) -> None:
        self.assertEqual(normalize_identifier(" passport ", " e-7788990 "), ("PASSPORT", "E7788990"))
        self.assertEqual(resolve_unified_key("passport", "E7788990"), "U0001")

    def test_pipeline_builds_gold_features(self) -> None:
        counts = run_pipeline(reset=True)
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

        run_pipeline(reset=True)
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


if __name__ == "__main__":
    unittest.main()
