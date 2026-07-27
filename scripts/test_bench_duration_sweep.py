"""Tests for the duration-sweep benchmark wrapper."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

import bench_duration_sweep as sweep  # noqa: E402


class DurationSweepTests(unittest.TestCase):
    def test_default_dry_run_contains_four_duration_cases(self) -> None:
        parser = sweep.build_parser()
        config = sweep.config_from_args(parser.parse_args(["--dry-run"]))

        self.assertEqual(config.durations, (5.0, 10.0, 30.0, 60.0))
        self.assertEqual(
            [sweep.csv_path_for(config.output_dir, value).name for value in config.durations],
            ["curve-5s.csv", "curve-10s.csv", "curve-30s.csv", "curve-60s.csv"],
        )

    def test_benchmark_command_uses_explicit_duration_and_output(self) -> None:
        parser = sweep.build_parser()
        config = sweep.config_from_args(
            parser.parse_args(["--durations", "10", "--dataset-mib", "64", "--dry-run"])
        )

        command = sweep.build_benchmark_command(config, 10.0, Path("out.csv"))

        self.assertIn("--release", command)
        self.assertIn("--measure-secs", command)
        self.assertIn("10", command)
        self.assertIn("--dataset-mib", command)
        self.assertIn("64", command)
        self.assertEqual(command[-2:], ["--output", "out.csv"])

    def test_dashboard_command_labels_each_duration(self) -> None:
        parser = sweep.build_parser()
        config = sweep.config_from_args(parser.parse_args(["--dry-run"]))

        command = sweep.build_dashboard_command(
            config,
            [Path("curve-5s.csv"), Path("curve-10s.csv"), Path("curve-30s.csv"), Path("curve-60s.csv")],
        )

        self.assertIn("scripts/visualize_bench.py", command)
        self.assertIn("--labels", command)
        self.assertIn("5s", command)
        self.assertIn("60s", command)


if __name__ == "__main__":
    unittest.main()
