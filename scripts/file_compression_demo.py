#!/usr/bin/env python3
"""Measure where FileTier LZ4 compression starts paying for itself.

Fixed 64 KiB slots mean compression never adds capacity to a file tier, so its
only wins are fewer transferred bytes and less SSD wear. Against those wins it
pays two costs: LZ4 CPU time, and the loss of the io_uring read path, because a
compression-capable tier withholds its raw descriptor so envelope decoding
cannot be bypassed.

This runner sweeps payload compressibility with compression off and on, then
reports the compressibility at which the two throughputs break even.
"""

from __future__ import annotations

import argparse
import csv
import json
import shlex
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

COMPRESSIBILITY_PCTS = (0, 25, 50, 75, 90, 100)
COMPRESSION_MODES = ("off", "on")
DEFAULT_OUTPUT_DIR = Path("results/file-compression")
DEFAULT_SUMMARY_NAME = "crossover.csv"
DEFAULT_TIER_PATH = Path("/mnt/nvme/tier/tierbuf-compression.bin")
DEFAULT_DATASET_MIB = 4096
DEFAULT_FRACTION = "0.25"
DEFAULT_WARMUP_SECS = 5.0
DEFAULT_MEASURE_SECS = 30.0
DEFAULT_WORKERS = 4
SUMMARY_HEADER = (
    "compressibility_pct",
    "sampled_compression_ratio",
    "throughput_off_ops",
    "throughput_on_ops",
    "speedup",
)


class DemoDataError(Exception):
    """Raised when benchmark output cannot be read or summarized."""


@dataclass(frozen=True)
class DemoConfig:
    """Validated settings for one FileTier compression sweep."""

    tier_path: Path
    dataset_mib: int
    fraction: str
    percentages: tuple[int, ...]
    warmup_secs: float
    measure_secs: float
    workers: int
    output_dir: Path
    summary: Path
    dry_run: bool


@dataclass(frozen=True)
class VariantResult:
    """Throughput and sampled compression ratio for one sweep point."""

    compressibility_pct: int
    sampled_compression_ratio: float | None
    throughput_off: float
    throughput_on: float

    @property
    def speedup(self) -> float:
        """Return compression-on throughput divided by compression-off throughput."""

        if self.throughput_off <= 0.0:
            raise DemoDataError(
                f"compressibility {self.compressibility_pct}: "
                "compression-off throughput must be greater than zero"
            )
        return self.throughput_on / self.throughput_off


@dataclass(frozen=True)
class Crossover:
    """Break-even compressibility, or why the sweep never crossed it."""

    compressibility_pct: float | None
    verdict: str


def parse_percentages(value: str) -> tuple[int, ...]:
    """Parse a comma-separated compressibility list accepted by tierbuf-bench."""

    percentages = []
    for item in value.split(","):
        text = item.strip()
        if not text:
            continue
        try:
            percent = int(text)
        except ValueError as error:
            raise argparse.ArgumentTypeError(
                f"compressibility must be a whole percentage, got '{text}'"
            ) from error
        if not 0 <= percent <= 100:
            raise argparse.ArgumentTypeError(
                f"compressibility must be within 0..=100, got '{percent}'"
            )
        percentages.append(percent)

    if len(percentages) < 2:
        raise argparse.ArgumentTypeError(
            "at least two compressibility values are required to find a crossover"
        )
    unique = tuple(sorted(set(percentages)))
    if len(unique) != len(percentages):
        raise argparse.ArgumentTypeError("compressibility values must be unique")
    return unique


def variant_stem(mode: str, compressibility_pct: int) -> str:
    """Return the artifact filename stem for one compression mode and payload."""

    return f"compression-{mode}-pct-{compressibility_pct:03d}"


def build_benchmark_command(
    config: DemoConfig,
    mode: str,
    compressibility_pct: int,
    csv_path: Path,
    stats_path: Path,
) -> list[str]:
    """Build one tierbuf-bench command line for a compression mode and payload."""

    if mode not in COMPRESSION_MODES:
        raise DemoDataError(f"unsupported compression mode '{mode}'")

    return [
        "cargo",
        "run",
        "-p",
        "tierbuf-bench",
        "--release",
        "--",
        "--dataset-mib",
        str(config.dataset_mib),
        "--fraction",
        config.fraction,
        "--warmup-secs",
        f"{config.warmup_secs:g}",
        "--measure-secs",
        f"{config.measure_secs:g}",
        "--workers",
        str(config.workers),
        "--file-tier",
        str(config.tier_path),
        "--file-compression",
        mode,
        "--payload-compressibility",
        str(compressibility_pct),
        "--output",
        str(csv_path),
        "--stats-output",
        str(stats_path),
    ]


def run_command(command: Sequence[str], dry_run: bool) -> None:
    """Run one command, or print it without executing in dry-run mode."""

    printable = shlex.join(command)
    if dry_run:
        print(printable)
        return
    print(f"+ {printable}", flush=True)
    subprocess.run(command, check=True)


def read_throughput(path: Path) -> float:
    """Return the single measured throughput recorded in a benchmark CSV."""

    try:
        with path.open(newline="", encoding="utf-8") as source:
            rows = list(csv.DictReader(source))
    except OSError as error:
        raise DemoDataError(f"{path}: could not read benchmark CSV") from error

    if len(rows) != 1:
        raise DemoDataError(
            f"{path}: expected exactly one measured fraction, found {len(rows)}"
        )
    raw = rows[0].get("throughput_ops")
    if raw is None:
        raise DemoDataError(f"{path}: benchmark CSV has no throughput_ops column")
    try:
        throughput = float(raw)
    except ValueError as error:
        raise DemoDataError(f"{path}: throughput_ops '{raw}' is not a number") from error
    if throughput <= 0.0:
        raise DemoDataError(f"{path}: throughput_ops must be greater than zero")
    return throughput


def read_sampled_ratio(path: Path) -> float | None:
    """Return the sampled LZ4 envelope ratio recorded in a benchmark stats file."""

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise DemoDataError(f"{path}: could not read benchmark stats JSON") from error

    runs = payload.get("runs") if isinstance(payload, dict) else None
    if not isinstance(runs, list) or not runs:
        raise DemoDataError(f"{path}: stats JSON must contain a non-empty runs array")
    first = runs[0]
    if not isinstance(first, dict):
        raise DemoDataError(f"{path}: stats JSON run entries must be objects")
    descriptor = first.get("payload")
    if not isinstance(descriptor, dict):
        raise DemoDataError(f"{path}: stats JSON run has no payload descriptor")

    ratio = descriptor.get("sampled_compression_ratio")
    if ratio is None:
        return None
    if not isinstance(ratio, (int, float)) or isinstance(ratio, bool):
        raise DemoDataError(f"{path}: sampled_compression_ratio must be a number")
    return float(ratio)


def crossover_point(results: Sequence[VariantResult]) -> Crossover:
    """Return the interpolated compressibility where compression breaks even."""

    if len(results) < 2:
        raise DemoDataError("a crossover needs at least two compressibility points")

    points = sorted(results, key=lambda result: result.compressibility_pct)
    speedups = [point.speedup for point in points]
    if all(speedup >= 1.0 for speedup in speedups):
        return Crossover(None, "compression won at every sampled compressibility")
    if all(speedup < 1.0 for speedup in speedups):
        return Crossover(None, "compression lost at every sampled compressibility")

    for lower, upper in zip(points, points[1:]):
        if lower.speedup < 1.0 <= upper.speedup:
            span = upper.speedup - lower.speedup
            low_pct = float(lower.compressibility_pct)
            high_pct = float(upper.compressibility_pct)
            if span <= 0.0:
                return Crossover(high_pct, "compression breaks even")
            weight = (1.0 - lower.speedup) / span
            return Crossover(
                low_pct + weight * (high_pct - low_pct),
                "compression breaks even",
            )

    return Crossover(None, "speedup is not monotonic; inspect the table directly")


def format_summary_table(results: Sequence[VariantResult]) -> str:
    """Return a fixed-width table of the sweep for terminal output."""

    lines = [
        f"{'payload%':>8}  {'lz4 ratio':>9}  {'off ops/s':>12}  "
        f"{'on ops/s':>12}  {'speedup':>8}",
        f"{'-' * 8}  {'-' * 9}  {'-' * 12}  {'-' * 12}  {'-' * 8}",
    ]
    for result in sorted(results, key=lambda item: item.compressibility_pct):
        ratio = (
            "n/a"
            if result.sampled_compression_ratio is None
            else f"{result.sampled_compression_ratio:.4f}"
        )
        lines.append(
            f"{result.compressibility_pct:>8}  {ratio:>9}  "
            f"{result.throughput_off:>12,.0f}  {result.throughput_on:>12,.0f}  "
            f"{result.speedup:>8.3f}"
        )
    return "\n".join(lines)


def write_summary_csv(results: Sequence[VariantResult], output: Path) -> None:
    """Write the machine-readable sweep summary next to the raw artifacts."""

    try:
        with output.open("w", newline="", encoding="utf-8") as destination:
            writer = csv.writer(destination, lineterminator="\n")
            writer.writerow(SUMMARY_HEADER)
            for result in sorted(results, key=lambda item: item.compressibility_pct):
                ratio = result.sampled_compression_ratio
                writer.writerow(
                    [
                        result.compressibility_pct,
                        "" if ratio is None else f"{ratio:.6f}",
                        f"{result.throughput_off:.3f}",
                        f"{result.throughput_on:.3f}",
                        f"{result.speedup:.6f}",
                    ]
                )
    except OSError as error:
        raise DemoDataError(f"{output}: could not write the summary CSV") from error


def run_sweep(config: DemoConfig) -> list[VariantResult]:
    """Run every compression mode and payload, returning the measured points."""

    results: list[VariantResult] = []
    for compressibility_pct in config.percentages:
        throughputs: dict[str, float] = {}
        ratio: float | None = None
        for mode in COMPRESSION_MODES:
            stem = variant_stem(mode, compressibility_pct)
            csv_path = config.output_dir / f"{stem}.csv"
            stats_path = config.output_dir / f"{stem}.json"
            run_command(
                build_benchmark_command(
                    config, mode, compressibility_pct, csv_path, stats_path
                ),
                config.dry_run,
            )
            if config.dry_run:
                continue
            throughputs[mode] = read_throughput(csv_path)
            if mode == "on":
                ratio = read_sampled_ratio(stats_path)

        if config.dry_run:
            continue
        results.append(
            VariantResult(
                compressibility_pct=compressibility_pct,
                sampled_compression_ratio=ratio,
                throughput_off=throughputs["off"],
                throughput_on=throughputs["on"],
            )
        )
    return results


def build_parser() -> argparse.ArgumentParser:
    """Return the command-line parser for the compression crossover demo."""

    parser = argparse.ArgumentParser(
        description=(
            "Sweep payload compressibility with FileTier compression off and on, "
            "then report the break-even point."
        )
    )
    parser.add_argument(
        "--tier-path",
        type=Path,
        default=DEFAULT_TIER_PATH,
        help=f"FileTier backing file, reused by every run (default: {DEFAULT_TIER_PATH})",
    )
    parser.add_argument(
        "--dataset-mib",
        type=int,
        default=DEFAULT_DATASET_MIB,
        help=f"Logical dataset size in MiB (default: {DEFAULT_DATASET_MIB})",
    )
    parser.add_argument(
        "--fraction",
        default=DEFAULT_FRACTION,
        help=(
            "DRAM fraction held resident; keep it below 1.0 so the file tier is "
            f"actually exercised (default: {DEFAULT_FRACTION})"
        ),
    )
    parser.add_argument(
        "--compressibility",
        type=parse_percentages,
        default=COMPRESSIBILITY_PCTS,
        help=(
            "Comma-separated payload compressibility percentages "
            f"(default: {','.join(str(value) for value in COMPRESSIBILITY_PCTS)})"
        ),
    )
    parser.add_argument(
        "--warmup-secs",
        type=float,
        default=DEFAULT_WARMUP_SECS,
        help=f"Warmup duration per run (default: {DEFAULT_WARMUP_SECS:g})",
    )
    parser.add_argument(
        "--measure-secs",
        type=float,
        default=DEFAULT_MEASURE_SECS,
        help=f"Measurement duration per run (default: {DEFAULT_MEASURE_SECS:g})",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_WORKERS,
        help=f"Benchmark worker threads (default: {DEFAULT_WORKERS})",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help=f"Directory for per-run artifacts (default: {DEFAULT_OUTPUT_DIR})",
    )
    parser.add_argument(
        "--summary",
        type=Path,
        default=None,
        help="Summary CSV path (default: <output-dir>/crossover.csv)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the benchmark commands without running them",
    )
    return parser


def config_from_args(args: argparse.Namespace) -> DemoConfig:
    """Validate parsed arguments and return the sweep configuration."""

    if args.dataset_mib <= 0:
        raise DemoDataError("--dataset-mib must be greater than zero")
    if args.warmup_secs <= 0.0:
        raise DemoDataError("--warmup-secs must be greater than zero")
    if args.measure_secs <= 0.0:
        raise DemoDataError("--measure-secs must be greater than zero")
    if args.workers <= 0:
        raise DemoDataError("--workers must be greater than zero")
    if not str(args.tier_path):
        raise DemoDataError("--tier-path must not be empty")

    output_dir = args.output_dir
    summary = args.summary if args.summary is not None else output_dir / DEFAULT_SUMMARY_NAME
    return DemoConfig(
        tier_path=args.tier_path,
        dataset_mib=args.dataset_mib,
        fraction=args.fraction,
        percentages=tuple(args.compressibility),
        warmup_secs=args.warmup_secs,
        measure_secs=args.measure_secs,
        workers=args.workers,
        output_dir=output_dir,
        summary=summary,
        dry_run=args.dry_run,
    )


def main(argv: Sequence[str] | None = None) -> int:
    """Run the sweep and print the measured crossover."""

    args = build_parser().parse_args(argv)
    try:
        config = config_from_args(args)
    except DemoDataError as error:
        print(f"file_compression_demo: {error}", file=sys.stderr)
        return 2

    if not config.dry_run:
        config.output_dir.mkdir(parents=True, exist_ok=True)
        config.tier_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        results = run_sweep(config)
    except subprocess.CalledProcessError as error:
        print(f"file_compression_demo: benchmark failed: {error}", file=sys.stderr)
        return 1
    except DemoDataError as error:
        print(f"file_compression_demo: {error}", file=sys.stderr)
        return 1

    if config.dry_run:
        return 0

    try:
        write_summary_csv(results, config.summary)
        crossover = crossover_point(results)
    except DemoDataError as error:
        print(f"file_compression_demo: {error}", file=sys.stderr)
        return 1

    print()
    print(
        f"FileTier compression sweep: dataset {config.dataset_mib} MiB, "
        f"DRAM fraction {config.fraction}, tier {config.tier_path}"
    )
    print(format_summary_table(results))
    print()
    if crossover.compressibility_pct is None:
        print(f"Crossover: {crossover.verdict}")
    else:
        print(
            f"Crossover: {crossover.verdict} at about "
            f"{crossover.compressibility_pct:.1f}% payload compressibility"
        )
    print(f"Summary written to {config.summary}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
