"""Tests for the reproducible S3 degradation-curve runner."""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

import s3_cliff_demo as demo  # noqa: E402


class S3CliffDemoTests(unittest.TestCase):
    def parse(self, *arguments: str) -> demo.DemoConfig:
        parser = demo.build_parser()
        return demo.config_from_args(parser.parse_args(arguments))

    def test_fixed_fraction_list_includes_one_gib_for_32_gib(self) -> None:
        fractions = demo.fractions_for_demo()

        self.assertEqual(
            fractions,
            (1.0, 0.5, 0.25, 0.125, 0.0625, 0.03125),
        )
        self.assertEqual(
            [demo.fraction_label(value) for value in fractions],
            ["1.0", "0.5", "0.25", "0.125", "0.0625", "0.03125"],
        )
        self.assertEqual(int(32 * 1024 * fractions[-1]), 1024)

    def test_local_and_aws_defaults(self) -> None:
        local = self.parse(
            "--local",
            "--endpoint",
            "http://127.0.0.1:9000",
            "--bucket",
            "tierbuf-demo",
        )
        aws = self.parse("--bucket", "tierbuf-demo")

        self.assertEqual((local.dataset_mib, local.dram_mib), (2048, 256))
        self.assertEqual((aws.dataset_mib, aws.dram_mib), (32768, 1024))
        self.assertEqual(aws.region, "us-east-1")
        self.assertIsNone(aws.endpoint)

    def test_local_requires_endpoint_and_target_must_be_swept(self) -> None:
        parser = demo.build_parser()
        with self.assertRaisesRegex(argparse_type_error(), "--local requires"):
            demo.config_from_args(
                parser.parse_args(["--local", "--bucket", "tierbuf-demo"])
            )
        with self.assertRaisesRegex(argparse_type_error(), "must match a swept fraction"):
            demo.config_from_args(
                parser.parse_args(
                    [
                        "--bucket",
                        "tierbuf-demo",
                        "--dataset-mib",
                        "100",
                        "--dram-mib",
                        "10",
                    ]
                )
            )

    def test_benchmark_command_has_s3_fraction_and_output_flags(self) -> None:
        config = self.parse(
            "--local",
            "--endpoint",
            "http://127.0.0.1:9000",
            "--bucket",
            "tierbuf-demo",
            "--yes",
        )
        variant = demo.variants_for(config)[0]

        command = demo.build_benchmark_command(
            config,
            variant,
            0.125,
            Path("fraction.csv"),
            Path("fraction.json"),
        )

        self.assertEqual(command[:6], ["cargo", "run", "-p", "tierbuf-bench", "--release", "--"])
        self.assertIn("--fraction", command)
        self.assertEqual(command[command.index("--fraction") + 1], "0.125")
        self.assertEqual(
            command[command.index("--s3-endpoint") + 1],
            "http://127.0.0.1:9000",
        )
        self.assertEqual(command[command.index("--s3-bucket") + 1], "tierbuf-demo")
        self.assertEqual(command[command.index("--prefetch-workers") + 1], "64")
        self.assertIn("--prefetch-scan", command)
        self.assertIn("--scan-only", command)
        self.assertEqual(command[command.index("--output") + 1], "fraction.csv")
        self.assertEqual(
            command[command.index("--stats-output") + 1],
            "fraction.json",
        )

    def test_compare_prefetch_dashboard_uses_two_labeled_curves(self) -> None:
        config = self.parse(
            "--bucket",
            "tierbuf-demo",
            "--compare-prefetch",
            "--yes",
        )
        variants = demo.variants_for(config)
        artifacts = [
            demo.VariantArtifacts(
                variant,
                demo.combined_csv_path_for(config.output_dir, variant),
                demo.combined_stats_path_for(config.output_dir, variant),
            )
            for variant in variants
        ]

        command = demo.build_dashboard_command(config, artifacts)

        self.assertEqual(
            [(variant.label, variant.workers) for variant in variants],
            [("prefetch-4", 4), ("prefetch-64", 64)],
        )
        self.assertIn("--stats", command)
        self.assertIn("--labels", command)
        labels_index = command.index("--labels")
        self.assertEqual(
            command[labels_index + 1 : labels_index + 3],
            ["prefetch-4", "prefetch-64"],
        )
        self.assertEqual(command[-2:], ["--output", str(config.dashboard)])

    def test_cost_estimate_matches_32_gib_plan(self) -> None:
        estimate = demo.estimate_s3_cost(32768)

        self.assertEqual(estimate.put_requests, 3_145_734)
        self.assertEqual(estimate.get_requests_low, 2_113_536)
        self.assertEqual(estimate.get_requests_high, 10_567_680)
        self.assertAlmostEqual(estimate.put_cost_usd, 15.72867)
        self.assertAlmostEqual(estimate.get_cost_low_usd, 0.8454144)
        self.assertAlmostEqual(estimate.get_cost_high_usd, 4.227072)
        self.assertAlmostEqual(estimate.storage_cost_usd, 6 * 32 * 0.023 / 30)
        compared = demo.estimate_s3_cost(32768, sweep_count=2)
        self.assertEqual(compared.put_requests, estimate.put_requests * 2)
        self.assertAlmostEqual(compared.total_high_usd, estimate.total_high_usd * 2)

    def test_confirmation_defaults_to_no(self) -> None:
        self.assertFalse(demo.confirm_run(lambda _: ""))
        self.assertFalse(demo.confirm_run(lambda _: "no"))
        self.assertTrue(demo.confirm_run(lambda _: "YES"))

    @mock.patch.object(subprocess, "run")
    def test_run_command_uses_checked_subprocess(self, run: mock.Mock) -> None:
        command = ["cargo", "run", "--", "--fraction", "0.5"]

        demo.run_command(command, dry_run=False)

        run.assert_called_once_with(command, check=True)

    def test_merge_outputs_and_summarize_s3_stats(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            csv_paths = []
            stats_paths = []
            for index, fraction in enumerate(("1.0", "0.5")):
                csv_path = root / f"{index}.csv"
                csv_path.write_text(
                    "fraction,throughput_ops\n"
                    f"{fraction},{1000 - index * 100}\n",
                    encoding="utf-8",
                )
                stats_path = root / f"{index}.json"
                stats_path.write_text(
                    json.dumps(
                        {
                            "schema_version": 1,
                            "runs": [
                                {
                                    "fraction": fraction,
                                    "s3": {
                                        "get_requests": index + 1,
                                        "put_requests": 10,
                                        "delete_requests": 2,
                                        "request_failures": 0,
                                        "stored_bytes": 25,
                                        "logical_bytes": 100,
                                        "bytes_uploaded": 30,
                                        "bytes_downloaded": 40,
                                    },
                                }
                            ],
                        }
                    ),
                    encoding="utf-8",
                )
                csv_paths.append(csv_path)
                stats_paths.append(stats_path)

            combined_csv = root / "combined.csv"
            combined_stats = root / "combined.json"
            demo.merge_csv_files(csv_paths, combined_csv)
            demo.merge_stats_files(stats_paths, combined_stats)
            summary = demo.load_s3_summary(combined_stats)

            self.assertEqual(
                combined_csv.read_text(encoding="utf-8").splitlines(),
                [
                    "fraction,throughput_ops",
                    "1.0,1000",
                    "0.5,900",
                ],
            )
            self.assertEqual(len(json.loads(combined_stats.read_text())["runs"]), 2)
            self.assertEqual(summary.records, 2)
            self.assertEqual(summary.get_requests, 3)
            self.assertEqual(summary.put_requests, 20)
            self.assertEqual(summary.bytes_downloaded, 80)
            self.assertEqual(summary.compression_ratio, 0.25)


def argparse_type_error() -> type[Exception]:
    """Return argparse's validation exception without broad test matching."""

    import argparse

    return argparse.ArgumentTypeError


if __name__ == "__main__":
    unittest.main()
