#!/usr/bin/env python3
"""Validate and optionally plot a tierbuf degradation curve.

Exit status 0 means every adjacent pair passed, 1 means valid input contained
a performance cliff, and 2 means the CSV, CLI input, or optional plotting
environment was invalid.
"""

from __future__ import annotations

import argparse
import csv
import math
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

THROUGHPUT_RATIO_LIMIT = 3.0
P99_RATIO_LIMIT = 4.0
REQUIRED_COLUMNS = (
    "fraction",
    "throughput_ops",
    "p50_us",
    "p99_us",
    "cost_usd_per_1e6ops",
)


class CurveDataError(ValueError):
    """Raised when a curve CSV cannot be validated."""


class PlotDependencyError(RuntimeError):
    """Raised when plotting was requested but matplotlib is unavailable."""


@dataclass(frozen=True)
class CurvePoint:
    """One measured DRAM fraction and its benchmark values."""

    fraction: float
    throughput_ops: float
    p50_us: float
    p99_us: float
    cost_usd_per_1e6ops: float


@dataclass(frozen=True)
class AdjacentCheck:
    """Ratios and pass/fail state for one adjacent fraction pair."""

    high_fraction: float
    lower_fraction: float
    throughput_ratio: float
    p99_ratio: float
    throughput_failed: bool
    p99_failed: bool

    @property
    def failed(self) -> bool:
        """Return whether either cliff limit was exceeded."""

        return self.throughput_failed or self.p99_failed


def load_curve(path: Path) -> list[CurvePoint]:
    """Load, validate, and sort curve points by descending DRAM fraction."""

    try:
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            fieldnames = reader.fieldnames
            if fieldnames is None:
                raise CurveDataError(f"{path}: CSV header is missing")
            if len(fieldnames) != len(set(fieldnames)):
                raise CurveDataError(f"{path}: CSV header contains duplicate columns")

            missing_columns = [
                column for column in REQUIRED_COLUMNS if column not in fieldnames
            ]
            if missing_columns:
                joined = ", ".join(missing_columns)
                raise CurveDataError(f"{path}: missing required column(s): {joined}")

            points: list[CurvePoint] = []
            fractions: set[float] = set()
            for row in reader:
                line_number = reader.line_num
                if None in row:
                    raise CurveDataError(
                        f"{path}:{line_number}: row has more values than the header"
                    )
                values = {
                    column: _parse_positive_number(
                        row.get(column), path, line_number, column
                    )
                    for column in REQUIRED_COLUMNS
                }

                fraction = values["fraction"]
                if fraction > 1.0:
                    raise CurveDataError(
                        f"{path}:{line_number}: fraction must be in (0, 1], "
                        f"got {fraction!r}"
                    )
                if fraction in fractions:
                    raise CurveDataError(
                        f"{path}:{line_number}: duplicate fraction {fraction:g}"
                    )
                fractions.add(fraction)
                points.append(CurvePoint(**values))
    except CurveDataError:
        raise
    except (OSError, csv.Error) as error:
        raise CurveDataError(f"{path}: could not read CSV: {error}") from error

    if len(points) < 2:
        raise CurveDataError(
            f"{path}: at least two distinct fraction rows are required"
        )
    return sorted(points, key=lambda point: point.fraction, reverse=True)


def check_curve(points: Sequence[CurvePoint]) -> list[AdjacentCheck]:
    """Check each adjacent pair after sorting fractions from high to low.

    For adjacent fractions ``high > lower``:

    * ``throughput(high) / throughput(lower)`` must be at most 3.0.
    * ``p99(lower) / p99(high)`` must be at most 4.0.
    """

    ordered = sorted(points, key=lambda point: point.fraction, reverse=True)
    checks: list[AdjacentCheck] = []
    for high, lower in zip(ordered, ordered[1:]):
        throughput_ratio = high.throughput_ops / lower.throughput_ops
        p99_ratio = lower.p99_us / high.p99_us
        checks.append(
            AdjacentCheck(
                high_fraction=high.fraction,
                lower_fraction=lower.fraction,
                throughput_ratio=throughput_ratio,
                p99_ratio=p99_ratio,
                throughput_failed=throughput_ratio > THROUGHPUT_RATIO_LIMIT,
                p99_failed=p99_ratio > P99_RATIO_LIMIT,
            )
        )
    return checks


def plot_curve(points: Sequence[CurvePoint], output_path: Path) -> None:
    """Render throughput, latency, and cost panels to ``output_path``.

    Matplotlib is imported only when this function is called.
    """

    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError as error:
        raise PlotDependencyError(
            "plotting requires matplotlib; install it or omit --plot"
        ) from error

    ordered = sorted(points, key=lambda point: point.fraction, reverse=True)
    fractions = [point.fraction for point in ordered]

    figure, axes = plt.subplots(3, 1, figsize=(8, 10), sharex=True)
    axes[0].plot(
        fractions,
        [point.throughput_ops for point in ordered],
        marker="o",
    )
    axes[0].set_ylabel("throughput (ops/s)")
    axes[0].grid(True, alpha=0.3)

    axes[1].plot(
        fractions,
        [point.p50_us for point in ordered],
        marker="o",
        label="p50",
    )
    axes[1].plot(
        fractions,
        [point.p99_us for point in ordered],
        marker="o",
        label="p99",
    )
    axes[1].set_ylabel("latency (µs)")
    axes[1].legend()
    axes[1].grid(True, alpha=0.3)

    axes[2].plot(
        fractions,
        [point.cost_usd_per_1e6ops for point in ordered],
        marker="o",
    )
    axes[2].set_ylabel("USD / 1M ops")
    axes[2].set_xlabel("DRAM fraction")
    axes[2].grid(True, alpha=0.3)
    axes[2].invert_xaxis()

    figure.suptitle("tierbuf degradation curve")
    figure.tight_layout()
    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        figure.savefig(output_path)
    except OSError:
        plt.close(figure)
        raise
    plt.close(figure)


def build_parser() -> argparse.ArgumentParser:
    """Build the command-line parser."""

    parser = argparse.ArgumentParser(
        description="check a tierbuf degradation-curve CSV for adjacent cliffs"
    )
    parser.add_argument("csv_path", type=Path, help="curve CSV to validate")
    parser.add_argument(
        "--plot",
        type=Path,
        metavar="PATH",
        help="optionally render a plot (requires matplotlib)",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run the command-line checker and return its process exit status."""

    arguments = build_parser().parse_args(argv)
    try:
        points = load_curve(arguments.csv_path)
        checks = check_curve(points)
        if arguments.plot is not None:
            plot_curve(points, arguments.plot)
    except (CurveDataError, PlotDependencyError, OSError) as error:
        print(f"invalid degradation curve: {error}", file=sys.stderr)
        return 2

    for check in checks:
        status = "FAIL" if check.failed else "PASS"
        print(
            f"{status} {check.high_fraction:g} -> {check.lower_fraction:g}: "
            f"throughput ratio {check.throughput_ratio:.6g} "
            f"(limit {THROUGHPUT_RATIO_LIMIT:g}), "
            f"p99 ratio {check.p99_ratio:.6g} (limit {P99_RATIO_LIMIT:g})"
        )

    failures = [check for check in checks if check.failed]
    if failures:
        print(
            f"degradation cliff detected in {len(failures)} adjacent pair(s)",
            file=sys.stderr,
        )
        return 1

    print("degradation curve passed: no adjacent cliff detected")
    return 0


def _parse_positive_number(
    raw_value: str | None,
    path: Path,
    line_number: int,
    column: str,
) -> float:
    if raw_value is None or not raw_value.strip():
        raise CurveDataError(f"{path}:{line_number}: {column} is missing")
    try:
        value = float(raw_value)
    except ValueError as error:
        raise CurveDataError(
            f"{path}:{line_number}: {column} is not a number: {raw_value!r}"
        ) from error
    if not math.isfinite(value):
        raise CurveDataError(
            f"{path}:{line_number}: {column} must be finite, got {raw_value!r}"
        )
    if value <= 0.0:
        raise CurveDataError(
            f"{path}:{line_number}: {column} must be positive, got {value!r}"
        )
    return value


if __name__ == "__main__":
    raise SystemExit(main())
