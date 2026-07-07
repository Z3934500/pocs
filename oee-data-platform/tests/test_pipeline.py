from __future__ import annotations

import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from oee_platform.db import connect
from oee_platform.pipeline import normalize_event, run_pipeline


class OeePipelineTest(unittest.TestCase):
    def test_normalize_kunshan_event(self) -> None:
        normalized = normalize_event(
            "KS",
            {
                "shiftDate": "2026-06-24",
                "shift": "DAY",
                "machineNo": "ks-press-01",
                "outputQty": 100,
                "okQty": 98,
                "runtime_minutes": 450,
                "downtime": {"minutes": 30, "reason": "Material Wait"},
            },
        )
        self.assertEqual(normalized["machine_number"], "KS-PRESS-01")
        self.assertEqual(normalized["shift_code"], "D")

    def test_pipeline_builds_oee_and_alerts(self) -> None:
        counts = run_pipeline(reset=True)
        self.assertGreaterEqual(counts["machine_events"], 20)
        self.assertGreaterEqual(counts["oee_daily"], 20)
        self.assertGreaterEqual(counts["dq_issues"], 2)
        with connect() as conn:
            avg_oee = conn.execute("SELECT AVG(oee) FROM fact_oee_daily").fetchone()[0]
        self.assertGreater(avg_oee, 0.3)


if __name__ == "__main__":
    unittest.main()
