"""Tests for the stdlib-only degradation-curve checker."""

from __future__ import annotations

import subprocess
import sys
import unittest
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
FIXTURES = SCRIPT_DIR / "fixtures"
SCRIPT = SCRIPT_DIR / "degradation.py"
sys.path.insert(0, str(SCRIPT_DIR))

import degradation  # noqa: E402


class DegradationCheckerTests(unittest.TestCase):
    def test_pass_fixture_is_sorted_descending_and_has_no_cliffs(self) -> None:
        points = degradation.load_curve(FIXTURES / "pass.csv")

        self.assertEqual(
            [point.fraction for point in points],
            [1.0, 0.8, 0.6, 0.4, 0.2, 0.1],
        )
        checks = degradation.check_curve(points)
        self.assertTrue(checks)
        self.assertFalse(any(check.failed for check in checks))
        self.assertAlmostEqual(checks[0].throughput_ratio, 2.0)
        self.assertAlmostEqual(checks[0].p99_ratio, 1.5)

    def test_throughput_cliff_uses_high_over_next_lower(self) -> None:
        checks = degradation.check_curve(
            degradation.load_curve(FIXTURES / "throughput_cliff.csv")
        )

        self.assertEqual(len(checks), 1)
        self.assertAlmostEqual(checks[0].throughput_ratio, 1000.0 / 300.0)
        self.assertTrue(checks[0].throughput_failed)
        self.assertFalse(checks[0].p99_failed)

    def test_p99_cliff_uses_next_lower_over_high(self) -> None:
        checks = degradation.check_curve(
            degradation.load_curve(FIXTURES / "p99_cliff.csv")
        )

        self.assertEqual(len(checks), 1)
        self.assertAlmostEqual(checks[0].p99_ratio, 4.5)
        self.assertFalse(checks[0].throughput_failed)
        self.assertTrue(checks[0].p99_failed)

    def test_ratios_at_limits_are_accepted(self) -> None:
        high = degradation.CurvePoint(1.0, 300.0, 10.0, 100.0, 1.0)
        lower = degradation.CurvePoint(0.8, 100.0, 20.0, 400.0, 0.8)

        check = degradation.check_curve([lower, high])[0]
        self.assertEqual(check.throughput_ratio, 3.0)
        self.assertEqual(check.p99_ratio, 4.0)
        self.assertFalse(check.failed)

    def test_invalid_schema_and_data_are_rejected(self) -> None:
        invalid_fixtures = (
            "invalid_schema.csv",
            "invalid_nonpositive.csv",
            "invalid_duplicate.csv",
            "invalid_nonfinite.csv",
        )

        for filename in invalid_fixtures:
            with self.subTest(filename=filename):
                with self.assertRaises(degradation.CurveDataError):
                    degradation.load_curve(FIXTURES / filename)

    def test_cli_exit_codes_and_diagnostics(self) -> None:
        passing = self.run_cli("pass.csv")
        self.assertEqual(passing.returncode, 0, passing.stderr)
        self.assertIn("degradation curve passed", passing.stdout)
        self.assertEqual(passing.stderr, "")

        throughput_failure = self.run_cli("throughput_cliff.csv")
        self.assertEqual(throughput_failure.returncode, 1)
        self.assertIn("FAIL 1 -> 0.8", throughput_failure.stdout)
        self.assertIn("degradation cliff detected", throughput_failure.stderr)

        p99_failure = self.run_cli("p99_cliff.csv")
        self.assertEqual(p99_failure.returncode, 1)
        self.assertIn("p99 ratio 4.5", p99_failure.stdout)

        for fixture_name in (
            "invalid_schema.csv",
            "invalid_nonpositive.csv",
            "invalid_duplicate.csv",
            "invalid_nonfinite.csv",
        ):
            with self.subTest(fixture_name=fixture_name):
                invalid = self.run_cli(fixture_name)
                self.assertEqual(invalid.returncode, 2)
                self.assertIn("invalid degradation curve", invalid.stderr)

    def test_missing_csv_is_invalid_cli_input(self) -> None:
        result = subprocess.run(
            [sys.executable, str(SCRIPT), str(FIXTURES / "does-not-exist.csv")],
            check=False,
            capture_output=True,
            text=True,
        )

        self.assertEqual(result.returncode, 2)
        self.assertIn("could not read CSV", result.stderr)

    @staticmethod
    def run_cli(fixture_name: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, str(SCRIPT), str(FIXTURES / fixture_name)],
            check=False,
            capture_output=True,
            text=True,
        )


if __name__ == "__main__":
    unittest.main()
