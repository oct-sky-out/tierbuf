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
EXTENDED_COLUMNS = (
    "point_ops",
    "point_throughput_ops",
    "point_p50_us",
    "point_p99_us",
    "scan_ops",
    "scan_throughput_ops",
    "scan_p50_us",
    "scan_p99_us",
    "dram_hit_rate",
    "lower_tier_hit_rate",
    "point_dram_hit_rate",
    "scan_dram_hit_rate",
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
    point_ops: float | None = None
    point_throughput_ops: float | None = None
    point_p50_us: float | None = None
    point_p99_us: float | None = None
    scan_ops: float | None = None
    scan_throughput_ops: float | None = None
    scan_p50_us: float | None = None
    scan_p99_us: float | None = None
    dram_hit_rate: float | None = None
    lower_tier_hit_rate: float | None = None
    point_dram_hit_rate: float | None = None
    scan_dram_hit_rate: float | None = None

    @property
    def has_extended_metrics(self) -> bool:
        """Return whether operation and tier-hit columns were loaded."""

        return all(
            value is not None
            for value in (
                self.point_ops,
                self.point_throughput_ops,
                self.point_p50_us,
                self.point_p99_us,
                self.scan_ops,
                self.scan_throughput_ops,
                self.scan_p50_us,
                self.scan_p99_us,
                self.dram_hit_rate,
                self.lower_tier_hit_rate,
                self.point_dram_hit_rate,
                self.scan_dram_hit_rate,
            )
        )


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
            extended_present = [
                column for column in EXTENDED_COLUMNS if column in fieldnames
            ]
            if extended_present and len(extended_present) != len(EXTENDED_COLUMNS):
                missing_extended = [
                    column
                    for column in EXTENDED_COLUMNS
                    if column not in extended_present
                ]
                joined = ", ".join(missing_extended)
                raise CurveDataError(
                    f"{path}: incomplete extended metrics; missing column(s): {joined}"
                )
            has_extended_metrics = len(extended_present) == len(EXTENDED_COLUMNS)

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
                extended_values = (
                    _parse_extended_metrics(row, path, line_number)
                    if has_extended_metrics
                    else {}
                )

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
                points.append(CurvePoint(**values, **extended_values))
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

    has_extended_metrics = all(point.has_extended_metrics for point in ordered)
    panel_count = 6 if has_extended_metrics else 3
    figure, axes = plt.subplots(
        panel_count,
        1,
        figsize=(8, 18 if has_extended_metrics else 10),
        sharex=True,
    )
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
    axes[2].grid(True, alpha=0.3)

    if has_extended_metrics:
        axes[3].plot(
            fractions,
            [point.point_throughput_ops for point in ordered],
            marker="o",
            label="point",
        )
        axes[3].plot(
            fractions,
            [point.scan_throughput_ops for point in ordered],
            marker="o",
            label="scan",
        )
        axes[3].set_ylabel("op throughput (ops/s)")
        axes[3].legend()
        axes[3].grid(True, alpha=0.3)

        axes[4].plot(
            fractions,
            [100.0 * point.dram_hit_rate for point in ordered],
            marker="o",
            label="DRAM",
        )
        axes[4].plot(
            fractions,
            [100.0 * point.lower_tier_hit_rate for point in ordered],
            marker="o",
            label="lower tier",
        )
        axes[4].set_ylabel("demand hit rate (%)")
        axes[4].set_ylim(0.0, 100.0)
        axes[4].legend()
        axes[4].grid(True, alpha=0.3)

        axes[5].plot(
            fractions,
            [100.0 * point.point_dram_hit_rate for point in ordered],
            marker="o",
            label="point",
        )
        axes[5].plot(
            fractions,
            [100.0 * point.scan_dram_hit_rate for point in ordered],
            marker="o",
            label="scan",
        )
        axes[5].set_ylabel("DRAM hit rate by op (%)")
        axes[5].set_ylim(0.0, 100.0)
        axes[5].legend()
        axes[5].grid(True, alpha=0.3)

    axes[-1].set_xlabel("DRAM fraction")
    axes[-1].invert_xaxis()

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


def _parse_extended_metrics(
    row: dict[str | None, str | list[str] | None],
    path: Path,
    line_number: int,
) -> dict[str, float]:
    """Parse and cross-check operation and demand-hit metrics."""

    values = {
        column: _parse_nonnegative_number(
            row.get(column), path, line_number, column
        )
        for column in EXTENDED_COLUMNS
    }
    for operation_type in ("point", "scan"):
        operation_count = values[f"{operation_type}_ops"]
        if not operation_count.is_integer():
            raise CurveDataError(
                f"{path}:{line_number}: {operation_type}_ops must be an integer, "
                f"got {operation_count!r}"
            )
        metric_columns = (
            f"{operation_type}_throughput_ops",
            f"{operation_type}_p50_us",
            f"{operation_type}_p99_us",
        )
        if operation_count == 0.0:
            if any(values[column] != 0.0 for column in metric_columns):
                raise CurveDataError(
                    f"{path}:{line_number}: zero {operation_type}_ops requires "
                    "zero throughput and latency metrics"
                )
        elif any(values[column] <= 0.0 for column in metric_columns):
            raise CurveDataError(
                f"{path}:{line_number}: nonzero {operation_type}_ops requires "
                "positive throughput and latency metrics"
            )

    for column in (
        "dram_hit_rate",
        "lower_tier_hit_rate",
        "point_dram_hit_rate",
        "scan_dram_hit_rate",
    ):
        if values[column] > 1.0:
            raise CurveDataError(
                f"{path}:{line_number}: {column} must be in [0, 1], "
                f"got {values[column]!r}"
            )
    hit_rate_sum = values["dram_hit_rate"] + values["lower_tier_hit_rate"]
    if not math.isclose(hit_rate_sum, 1.0, rel_tol=0.0, abs_tol=1e-6):
        raise CurveDataError(
            f"{path}:{line_number}: tier hit rates must sum to 1, "
            f"got {hit_rate_sum!r}"
        )
    return values


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


def _parse_nonnegative_number(
    raw_value: str | list[str] | None,
    path: Path,
    line_number: int,
    column: str,
) -> float:
    if not isinstance(raw_value, str) or not raw_value.strip():
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
    if value < 0.0:
        raise CurveDataError(
            f"{path}:{line_number}: {column} must be nonnegative, got {value!r}"
        )
    return value


if __name__ == "__main__":
    raise SystemExit(main())
