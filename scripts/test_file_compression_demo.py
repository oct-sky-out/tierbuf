"""Tests for the FileTier compression crossover runner."""

from __future__ import annotations

import argparse
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

import file_compression_demo as demo  # noqa: E402


def variant(
    compressibility_pct: int,
    throughput_off: float,
    throughput_on: float,
    ratio: float | None = None,
) -> demo.VariantResult:
    """Build one measured sweep point for crossover assertions."""

    return demo.VariantResult(
        compressibility_pct=compressibility_pct,
        sampled_compression_ratio=ratio,
        throughput_off=throughput_off,
        throughput_on=throughput_on,
    )


class ArgumentTests(unittest.TestCase):
    def parse(self, *arguments: str) -> demo.DemoConfig:
        parser = demo.build_parser()
        return demo.config_from_args(parser.parse_args(arguments))

    def test_defaults_target_the_aws_nvme_mount(self) -> None:
        config = self.parse()

        self.assertEqual(config.tier_path, demo.DEFAULT_TIER_PATH)
        self.assertEqual(config.dataset_mib, demo.DEFAULT_DATASET_MIB)
        self.assertEqual(config.fraction, demo.DEFAULT_FRACTION)
        self.assertEqual(config.percentages, demo.COMPRESSIBILITY_PCTS)
        self.assertEqual(config.summary, demo.DEFAULT_OUTPUT_DIR / "crossover.csv")
        self.assertFalse(config.dry_run)

    def test_overrides_are_applied(self) -> None:
        config = self.parse(
            "--tier-path",
            "/mnt/nvme/tier/custom.bin",
            "--dataset-mib",
            "128",
            "--fraction",
            "0.125",
            "--compressibility",
            "0,50,100",
            "--measure-secs",
            "2.5",
            "--output-dir",
            "results/custom",
            "--dry-run",
        )

        self.assertEqual(config.tier_path, Path("/mnt/nvme/tier/custom.bin"))
        self.assertEqual(config.dataset_mib, 128)
        self.assertEqual(config.fraction, "0.125")
        self.assertEqual(config.percentages, (0, 50, 100))
        self.assertEqual(config.measure_secs, 2.5)
        self.assertEqual(config.summary, Path("results/custom/crossover.csv"))
        self.assertTrue(config.dry_run)

    def test_compressibility_list_is_validated(self) -> None:
        self.assertEqual(demo.parse_percentages("100, 0 ,50"), (0, 50, 100))

        for invalid in ["", "50", "0,101", "0,-1", "0,half", "0,50,50"]:
            with self.subTest(invalid=invalid):
                with self.assertRaises(argparse.ArgumentTypeError):
                    demo.parse_percentages(invalid)

    def test_invalid_sizing_is_rejected(self) -> None:
        for arguments in [
            ("--dataset-mib", "0"),
            ("--measure-secs", "0"),
            ("--warmup-secs", "-1"),
            ("--workers", "0"),
        ]:
            with self.subTest(arguments=arguments):
                with self.assertRaises(demo.DemoDataError):
                    self.parse(*arguments)


class CommandTests(unittest.TestCase):
    def config(self, **overrides: object) -> demo.DemoConfig:
        base = dict(
            tier_path=Path("/mnt/nvme/tier/tierbuf.bin"),
            dataset_mib=256,
            fraction="0.25",
            percentages=(0, 100),
            warmup_secs=1.0,
            measure_secs=5.0,
            workers=4,
            output_dir=Path("results/file-compression"),
            summary=Path("results/file-compression/crossover.csv"),
            dry_run=False,
        )
        base.update(overrides)
        return demo.DemoConfig(**base)  # type: ignore[arg-type]

    def test_command_pairs_the_mode_with_the_payload(self) -> None:
        command = demo.build_benchmark_command(
            self.config(),
            "on",
            75,
            Path("results/x.csv"),
            Path("results/x.json"),
        )

        self.assertIn("--file-tier", command)
        self.assertEqual(
            command[command.index("--file-compression") + 1],
            "on",
        )
        self.assertEqual(
            command[command.index("--payload-compressibility") + 1],
            "75",
        )
        self.assertEqual(command[command.index("--fraction") + 1], "0.25")
        self.assertEqual(command[command.index("--output") + 1], "results/x.csv")

    def test_unknown_mode_is_rejected(self) -> None:
        with self.assertRaises(demo.DemoDataError):
            demo.build_benchmark_command(
                self.config(),
                "maybe",
                50,
                Path("results/x.csv"),
                Path("results/x.json"),
            )

    def test_variant_stem_sorts_by_payload(self) -> None:
        stems = [demo.variant_stem("on", pct) for pct in (0, 25, 100)]

        self.assertEqual(stems, sorted(stems))
        self.assertEqual(stems[0], "compression-on-pct-000")

    def test_sweep_runs_both_modes_for_every_payload(self) -> None:
        config = self.config(percentages=(0, 100), dry_run=True)
        with mock.patch.object(demo, "run_command") as runner:
            results = demo.run_sweep(config)

        self.assertEqual(results, [])
        modes = [
            call.args[0][call.args[0].index("--file-compression") + 1]
            for call in runner.call_args_list
        ]
        payloads = [
            call.args[0][call.args[0].index("--payload-compressibility") + 1]
            for call in runner.call_args_list
        ]
        self.assertEqual(modes, ["off", "on", "off", "on"])
        self.assertEqual(payloads, ["0", "0", "100", "100"])


class ArtifactTests(unittest.TestCase):
    def test_throughput_and_ratio_are_read_back(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            csv_path = root / "curve.csv"
            csv_path.write_text(
                "fraction,throughput_ops,p50_us\n0.25,1234.5,10.0\n",
                encoding="utf-8",
            )
            stats_path = root / "stats.json"
            stats_path.write_text(
                json.dumps(
                    {
                        "runs": [
                            {
                                "payload": {
                                    "compressibility_pct": 50,
                                    "file_compression": True,
                                    "sampled_compression_ratio": 0.51,
                                }
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )

            self.assertEqual(demo.read_throughput(csv_path), 1234.5)
            self.assertEqual(demo.read_sampled_ratio(stats_path), 0.51)

    def test_missing_ratio_is_reported_as_none(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            stats_path = Path(directory) / "stats.json"
            stats_path.write_text(
                json.dumps({"runs": [{"payload": {"compressibility_pct": 0}}]}),
                encoding="utf-8",
            )

            self.assertIsNone(demo.read_sampled_ratio(stats_path))

    def test_malformed_artifacts_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            multi_row = root / "multi.csv"
            multi_row.write_text(
                "fraction,throughput_ops\n1.0,10.0\n0.5,20.0\n", encoding="utf-8"
            )
            zero = root / "zero.csv"
            zero.write_text("fraction,throughput_ops\n0.25,0\n", encoding="utf-8")
            no_runs = root / "empty.json"
            no_runs.write_text(json.dumps({"runs": []}), encoding="utf-8")
            no_payload = root / "no-payload.json"
            no_payload.write_text(json.dumps({"runs": [{}]}), encoding="utf-8")

            with self.assertRaises(demo.DemoDataError):
                demo.read_throughput(multi_row)
            with self.assertRaises(demo.DemoDataError):
                demo.read_throughput(zero)
            with self.assertRaises(demo.DemoDataError):
                demo.read_sampled_ratio(no_runs)
            with self.assertRaises(demo.DemoDataError):
                demo.read_sampled_ratio(no_payload)

    def test_summary_csv_round_trips(self) -> None:
        results = [
            variant(0, 1000.0, 800.0, ratio=1.0005),
            variant(100, 1000.0, 1500.0, ratio=0.0046),
        ]
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "crossover.csv"
            demo.write_summary_csv(results, output)
            lines = output.read_text(encoding="utf-8").splitlines()

        self.assertEqual(lines[0], ",".join(demo.SUMMARY_HEADER))
        self.assertTrue(lines[1].startswith("0,1.000500,1000.000,800.000,0.800000"))
        self.assertEqual(len(lines), 3)


class CrossoverTests(unittest.TestCase):
    def test_interpolates_between_bracketing_points(self) -> None:
        results = [
            variant(0, 1000.0, 800.0),
            variant(100, 1000.0, 1200.0),
        ]

        crossover = demo.crossover_point(results)

        self.assertIsNotNone(crossover.compressibility_pct)
        # speedup rises 0.8 -> 1.2 across 0 -> 100, so it reaches 1.0 at 50.
        self.assertAlmostEqual(crossover.compressibility_pct, 50.0)

    def test_reports_when_compression_always_wins_or_always_loses(self) -> None:
        always_wins = demo.crossover_point(
            [variant(0, 1000.0, 1100.0), variant(100, 1000.0, 1500.0)]
        )
        always_loses = demo.crossover_point(
            [variant(0, 1000.0, 500.0), variant(100, 1000.0, 900.0)]
        )

        self.assertIsNone(always_wins.compressibility_pct)
        self.assertIn("won at every", always_wins.verdict)
        self.assertIsNone(always_loses.compressibility_pct)
        self.assertIn("lost at every", always_loses.verdict)

    def test_zero_baseline_throughput_is_rejected(self) -> None:
        with self.assertRaises(demo.DemoDataError):
            variant(0, 0.0, 100.0).speedup

    def test_single_point_cannot_produce_a_crossover(self) -> None:
        with self.assertRaises(demo.DemoDataError):
            demo.crossover_point([variant(0, 1000.0, 900.0)])

    def test_table_lists_every_point(self) -> None:
        table = demo.format_summary_table(
            [
                variant(100, 1000.0, 1500.0, ratio=0.0046),
                variant(0, 1000.0, 800.0, ratio=1.0005),
            ]
        )
        lines = table.splitlines()

        self.assertEqual(len(lines), 4)
        self.assertIn("0.800", lines[2])
        self.assertIn("1.500", lines[3])


if __name__ == "__main__":
    unittest.main()
