"""Tests for the standalone benchmark HTML renderer."""

from __future__ import annotations

import hashlib
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
FIXTURES = SCRIPT_DIR / "fixtures"
SCRIPT = SCRIPT_DIR / "visualize_bench.py"
sys.path.insert(0, str(SCRIPT_DIR))

import visualize_bench  # noqa: E402


class BenchmarkVisualizationTests(unittest.TestCase):
    def test_report_without_stats_matches_existing_snapshot(self) -> None:
        runs = visualize_bench.load_runs(
            [FIXTURES / "pass.csv"], ["fixture run"]
        )

        report = visualize_bench.render_report(runs)

        self.assertEqual(
            hashlib.sha256(report.encode("utf-8")).hexdigest(),
            "f1aacdb6e851634c54a397eef156674838da8603909894eac1c80acd95c97b56",
        )
        self.assertNotIn("S3 requests, transfer, and compression", report)

    def test_render_report_uses_english_labels(self) -> None:
        runs = visualize_bench.load_runs([FIXTURES / "pass.csv"], ["fixture run"])

        report = visualize_bench.render_report(
            runs,
            hotpath=[
                visualize_bench.HotPathEstimate("shared fix", 54.3),
                visualize_bench.HotPathEstimate("safe optimistic snapshot", 2454.7),
            ],
        )

        self.assertIn("<title>tierbuf benchmark dashboard</title>", report)
        self.assertIn("Throughput retention vs. all-DRAM", report)
        self.assertIn("p99 latency multiplier vs. all-DRAM", report)
        self.assertIn("Capacity cost saving", report)
        self.assertIn("Hot-path Criterion", report)
        self.assertIn("DRAM resident fraction", report)

    def test_cli_writes_html_report(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            output = Path(temp_dir) / "curve.html"
            result = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(FIXTURES / "pass.csv"),
                    "--output",
                    str(output),
                ],
                check=False,
                capture_output=True,
                text=True,
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertTrue(output.exists())
            self.assertIn("wrote", result.stdout)
            self.assertIn("tierbuf benchmark dashboard", output.read_text(encoding="utf-8"))

    def test_stats_json_renders_s3_request_and_compression_panel(self) -> None:
        runs = visualize_bench.load_runs(
            [FIXTURES / "pass.csv"], ["S3 fixture"]
        )
        stats = visualize_bench.load_s3_stats(
            [FIXTURES / "s3_stats.json"], runs
        )

        report = visualize_bench.render_report(runs, s3_stats=stats)

        self.assertIn("S3 requests, transfer, and compression", report)
        self.assertIn("GET requests", report)
        self.assertIn("Uploaded", report)
        self.assertIn("Downloaded", report)
        self.assertIn("Stored / logical", report)
        self.assertIn("3,600", report)
        self.assertIn("150.00 MiB", report)
        self.assertIn("0.500x", report)

    def test_cli_accepts_one_stats_json_per_csv(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            output = Path(temp_dir) / "curve.html"
            result = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(FIXTURES / "pass.csv"),
                    "--stats",
                    str(FIXTURES / "s3_stats.json"),
                    "--labels",
                    "S3 fixture",
                    "--output",
                    str(output),
                ],
                check=False,
                capture_output=True,
                text=True,
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn(
                "S3 requests, transfer, and compression",
                output.read_text(encoding="utf-8"),
            )

    def test_cli_rejects_corrupted_stats_json(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            stats = Path(temp_dir) / "stats.json"
            stats.write_text('{"runs": [', encoding="utf-8")
            output = Path(temp_dir) / "curve.html"

            result = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(FIXTURES / "pass.csv"),
                    "--stats",
                    str(stats),
                    "--output",
                    str(output),
                ],
                check=False,
                capture_output=True,
                text=True,
            )

            self.assertEqual(result.returncode, 2)
            self.assertIn("could not read benchmark stats JSON", result.stderr)
            self.assertFalse(output.exists())

    def test_extended_csv_renders_operation_and_tier_hit_charts(self) -> None:
        runs = visualize_bench.load_runs(
            [FIXTURES / "extended.csv"], ["extended run"]
        )

        report = visualize_bench.render_report(runs)

        self.assertIn("Operation throughput by type", report)
        self.assertIn("Demand hit rate by tier", report)
        self.assertIn("DRAM hit rate by operation type", report)
        self.assertIn('"dramHitRate":0.95', report)
        self.assertIn('"pointThroughput":700.0', report)
        self.assertIn('"pointDramHitRate":0.98', report)

    def test_label_count_must_match_csv_count(self) -> None:
        with self.assertRaisesRegex(Exception, "--labels expected 2 value"):
            visualize_bench.load_runs(
                [FIXTURES / "pass.csv", FIXTURES / "pass.csv"],
                ["only one"],
            )


if __name__ == "__main__":
    unittest.main()
