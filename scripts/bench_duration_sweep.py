#!/usr/bin/env python3
"""Run tierbuf benchmark curves at 5s, 10s, 30s, and 60s durations."""

from __future__ import annotations

import argparse
import shlex
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

DEFAULT_DURATIONS = (5.0, 10.0, 30.0, 60.0)
DEFAULT_OUTPUT_DIR = Path("results/duration-sweep")
DEFAULT_DASHBOARD = DEFAULT_OUTPUT_DIR / "dashboard.html"


@dataclass(frozen=True)
class SweepConfig:
    """User-selected duration-sweep settings."""

    durations: tuple[float, ...]
    output_dir: Path
    dashboard: Path
    dataset_mib: int
    warmup_secs: float
    workers: int
    mock_latency_us: int
    prefetch_scan: bool
    scan_only: bool
    file_tier: Path | None
    release: bool
    dry_run: bool


def duration_label(seconds: float) -> str:
    """Return a stable label for a measurement duration."""

    if seconds.is_integer():
        return f"{int(seconds)}s"
    return f"{seconds:g}s"


def csv_path_for(output_dir: Path, seconds: float) -> Path:
    """Return the CSV path for one duration."""

    return output_dir / f"curve-{duration_label(seconds)}.csv"


def build_benchmark_command(config: SweepConfig, seconds: float, output: Path) -> list[str]:
    """Build the tierbuf-bench command for one duration."""

    command = ["cargo", "run", "-p", "tierbuf-bench"]
    if config.release:
        command.append("--release")
    command.extend(
        [
            "--",
            "--dataset-mib",
            str(config.dataset_mib),
            "--warmup-secs",
            f"{config.warmup_secs:g}",
            "--measure-secs",
            f"{seconds:g}",
            "--workers",
            str(config.workers),
            "--mock-latency-us",
            str(config.mock_latency_us),
            "--output",
            str(output),
        ]
    )
    if config.prefetch_scan:
        command.append("--prefetch-scan")
    if config.scan_only:
        command.append("--scan-only")
    if config.file_tier is not None:
        command.extend(["--file-tier", str(config.file_tier)])
    return command


def build_dashboard_command(config: SweepConfig, csv_paths: Sequence[Path]) -> list[str]:
    """Build the visualize_bench command for the completed sweep."""

    labels = [duration_label(duration) for duration in config.durations]
    return [
        sys.executable,
        "scripts/visualize_bench.py",
        *[str(path) for path in csv_paths],
        "--labels",
        *labels,
        "--output",
        str(config.dashboard),
    ]


def run_command(command: Sequence[str], dry_run: bool) -> None:
    """Run or print one command."""

    printable = shlex.join(command)
    if dry_run:
        print(printable)
        return
    print(f"+ {printable}", flush=True)
    subprocess.run(command, check=True)


def run_sweep(config: SweepConfig) -> None:
    """Run every configured duration and render the dashboard."""

    csv_paths = [csv_path_for(config.output_dir, duration) for duration in config.durations]
    if not config.dry_run:
        config.output_dir.mkdir(parents=True, exist_ok=True)
        config.dashboard.parent.mkdir(parents=True, exist_ok=True)

    for duration, output in zip(config.durations, csv_paths):
        run_command(build_benchmark_command(config, duration, output), config.dry_run)
    run_command(build_dashboard_command(config, csv_paths), config.dry_run)


def parse_duration_list(values: Sequence[str]) -> tuple[float, ...]:
    """Parse positive second values from the CLI."""

    durations = []
    for value in values:
        try:
            seconds = float(value)
        except ValueError as error:
            raise argparse.ArgumentTypeError(
                f"duration must be a number of seconds, got {value!r}"
            ) from error
        if not seconds > 0.0:
            raise argparse.ArgumentTypeError("durations must be greater than zero")
        durations.append(seconds)
    return tuple(durations)


def build_parser() -> argparse.ArgumentParser:
    """Build the command-line parser."""

    parser = argparse.ArgumentParser(
        description="run tierbuf degradation curves for 5s, 10s, 30s, and 60s"
    )
    parser.add_argument(
        "--durations",
        nargs="+",
        default=[f"{duration:g}" for duration in DEFAULT_DURATIONS],
        metavar="SECONDS",
        help="measurement durations in seconds (default: 5 10 30 60)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help=f"directory for per-duration CSV files (default: {DEFAULT_OUTPUT_DIR})",
    )
    parser.add_argument(
        "--dashboard",
        type=Path,
        default=DEFAULT_DASHBOARD,
        help=f"HTML dashboard path (default: {DEFAULT_DASHBOARD})",
    )
    parser.add_argument(
        "--dataset-mib",
        type=int,
        default=256,
        help="logical dataset size in MiB (default: 256)",
    )
    parser.add_argument(
        "--warmup-secs",
        type=float,
        default=1.0,
        help="warmup per fraction in seconds (default: 1)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="worker threads (default: 4)",
    )
    parser.add_argument(
        "--mock-latency-us",
        type=int,
        default=80,
        help="MockTier read/write delay in microseconds (default: 80)",
    )
    parser.add_argument("--prefetch-scan", action="store_true")
    parser.add_argument("--scan-only", action="store_true")
    parser.add_argument("--file-tier", type=Path)
    parser.add_argument(
        "--debug",
        action="store_true",
        help="run the debug build instead of cargo --release",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="print commands without running them",
    )
    return parser


def config_from_args(arguments: argparse.Namespace) -> SweepConfig:
    """Convert parsed CLI arguments into a validated config."""

    if arguments.dataset_mib <= 0:
        raise argparse.ArgumentTypeError("--dataset-mib must be greater than zero")
    if arguments.warmup_secs <= 0.0:
        raise argparse.ArgumentTypeError("--warmup-secs must be greater than zero")
    if arguments.workers <= 0:
        raise argparse.ArgumentTypeError("--workers must be greater than zero")
    if arguments.mock_latency_us < 0:
        raise argparse.ArgumentTypeError("--mock-latency-us must be non-negative")

    return SweepConfig(
        durations=parse_duration_list(arguments.durations),
        output_dir=arguments.output_dir,
        dashboard=arguments.dashboard,
        dataset_mib=arguments.dataset_mib,
        warmup_secs=arguments.warmup_secs,
        workers=arguments.workers,
        mock_latency_us=arguments.mock_latency_us,
        prefetch_scan=arguments.prefetch_scan,
        scan_only=arguments.scan_only,
        file_tier=arguments.file_tier,
        release=not arguments.debug,
        dry_run=arguments.dry_run,
    )


def main(argv: Sequence[str] | None = None) -> int:
    """Run the duration sweep CLI."""

    parser = build_parser()
    try:
        config = config_from_args(parser.parse_args(argv))
        run_sweep(config)
    except argparse.ArgumentTypeError as error:
        parser.error(str(error))
    except subprocess.CalledProcessError as error:
        print(f"duration sweep failed: {error}", file=sys.stderr)
        return error.returncode or 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
