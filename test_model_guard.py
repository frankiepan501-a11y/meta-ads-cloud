import tempfile
import unittest
from pathlib import Path

from model_guard import ModelCallGuard


class ModelCallGuardTests(unittest.TestCase):
    def test_daily_budget_blocks_additional_calls(self):
        with tempfile.TemporaryDirectory() as tmp:
            guard = ModelCallGuard(Path(tmp) / "state.json", daily_limit=2, failure_threshold=2)
            self.assertEqual((True, "ok"), guard.reserve())
            guard.record_success()
            self.assertEqual((True, "ok"), guard.reserve())
            guard.record_success()
            self.assertEqual((False, "daily_budget_exhausted"), guard.reserve())

    def test_two_consecutive_failures_open_circuit_across_instances(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "state.json"
            first = ModelCallGuard(path, daily_limit=10, failure_threshold=2)
            self.assertEqual((True, "ok"), first.reserve())
            first.record_failure()
            self.assertEqual((True, "ok"), first.reserve())
            first.record_failure()
            second = ModelCallGuard(path, daily_limit=10, failure_threshold=2)
            self.assertEqual((False, "circuit_open"), second.reserve())


if __name__ == "__main__":
    unittest.main()
