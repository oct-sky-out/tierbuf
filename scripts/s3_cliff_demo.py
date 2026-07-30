#!/usr/bin/env python3
"""Run the reproducible tierbuf S3 degradation-curve demonstration."""

from __future__ import annotations

import argparse
import csv
import json
import math
import shlex
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Mapping, Sequence

MIB = 1024 * 1024
GIB = 1024 * MIB
PAGE_SIZE = 64 * 1024
DRAM_FRACTIONS = (1.0, 0.5, 0.25, 0.125, 0.0625, 0.03125)
DEFAULT_OUTPUT_DIR = Path("results/s3-demo")
DEFAULT_DASHBOARD = DEFAULT_OUTPUT_DIR / "dashboard.html"
DEFAULT_REGION = "us-east-1"
LOCAL_DATASET_MIB = 2 * 1024
LOCAL_DRAM_MIB = 256
AWS_DATASET_MIB = 32 * 1024
AWS_DRAM_MIB = 1024
DEFAULT_PREFETCH_WORKERS = 64
DEFAULT_PREFETCH_IN_FLIGHT = 256
SLOW_PREFETCH_WORKERS = 4
S3_GET_COST_USD = 4.0e-7
S3_PUT_COST_USD = 5.0e-6
S3_STORAGE_GIB_MONTH_USD = 0.023
GET_PASSES_HIGH = 5


class DemoDataError(Exception):
    """Raised when benchmark output cannot be combined or summarized."""


@dataclass(frozen=True)
class DemoConfig:
    """Validated settings for one S3 cliff demonstration."""

    local: bool
    endpoint: str | None
    bucket: str
    region: str
    dataset_mib: int
    dram_mib: int
    output_dir: Path
    dashboard: Path
    compare_prefetch: bool
    prefetch_in_flight: int
    yes: bool
    dry_run: bool


@dataclass(frozen=True)
class PrefetchVariant:
    """One prefetch-concurrency setting included in the demonstration."""

    label: str
    workers: int


@dataclass(frozen=True)
class VariantArtifacts:
    """Combined curve and stats files produced for one prefetch variant."""

    variant: PrefetchVariant
    csv_path: Path
    stats_path: Path


@dataclass(frozen=True)
class CostEstimate:
    """Estimated S3 request and one-day storage cost for a demonstration."""

    put_requests: int
    get_requests_low: int
    get_requests_high: int
    put_cost_usd: float
    get_cost_low_usd: float
    get_cost_high_usd: float
    storage_cost_usd: float
    total_low_usd: float
    total_high_usd: float


@dataclass(frozen=True)
class S3Summary:
    """S3 request, transfer, and compression counters from benchmark stats."""

    records: int
    get_requests: int
    put_requests: int
    delete_requests: int
    request_failures: int
    stored_bytes: int
    logical_bytes: int
    bytes_uploaded: int
    bytes_downloaded: int

    @property
    def compression_ratio(self) -> float | None:
        """Return stored divided by logical bytes, if logical bytes were recorded."""

        if self.logical_bytes == 0:
            return None
        return self.stored_bytes / self.logical_bytes


def fractions_for_demo() -> tuple[float, ...]:
    """Return the fixed DRAM fractions used by the S3 cliff demonstration."""

    return DRAM_FRACTIONS


def fraction_label(fraction: float) -> str:
    """Return the spelling accepted by tierbuf-bench for a DRAM fraction."""

    if fraction == 1.0:
        return "1.0"
    return f"{fraction:g}"


def fraction_file_label(fraction: float) -> str:
    """Return a filesystem-safe label for a DRAM fraction."""

    return fraction_label(fraction).replace(".", "p")


def variants_for(config: DemoConfig) -> tuple[PrefetchVariant, ...]:
    """Return the normal run, or the low/high prefetch comparison variants."""

    if config.compare_prefetch:
        return (
            PrefetchVariant("prefetch-4", SLOW_PREFETCH_WORKERS),
            PrefetchVariant("prefetch-64", DEFAULT_PREFETCH_WORKERS),
        )
    return (PrefetchVariant("prefetch-64", DEFAULT_PREFETCH_WORKERS),)


def csv_path_for(
    output_dir: Path,
    variant: PrefetchVariant,
    fraction: float,
) -> Path:
    """Return the per-fraction CSV path for one prefetch variant."""

    return output_dir / f"curve-{variant.label}-{fraction_file_label(fraction)}.csv"


def stats_path_for(
    output_dir: Path,
    variant: PrefetchVariant,
    fraction: float,
) -> Path:
    """Return the per-fraction stats JSON path for one prefetch variant."""

    return output_dir / f"stats-{variant.label}-{fraction_file_label(fraction)}.json"


def combined_csv_path_for(output_dir: Path, variant: PrefetchVariant) -> Path:
    """Return the combined curve CSV path for one prefetch variant."""

    return output_dir / f"curve-{variant.label}.csv"


def combined_stats_path_for(output_dir: Path, variant: PrefetchVariant) -> Path:
    """Return the combined stats JSON path for one prefetch variant."""

    return output_dir / f"stats-{variant.label}.json"


def build_benchmark_command(
    config: DemoConfig,
    variant: PrefetchVariant,
    fraction: float,
    csv_path: Path,
    stats_path: Path,
) -> list[str]:
    """Build one fraction's tierbuf-bench command line."""

    command = [
        "cargo",
        "run",
        "-p",
        "tierbuf-bench",
        "--release",
        "--",
        "--dataset-mib",
        str(config.dataset_mib),
        "--fraction",
        fraction_label(fraction),
        "--s3-bucket",
        config.bucket,
        "--s3-region",
        config.region,
        "--s3-compression",
        "on",
        "--s3-capacity-mib",
        str(config.dataset_mib * 2),
        "--prefetch-workers",
        str(variant.workers),
        "--prefetch-in-flight",
        str(config.prefetch_in_flight),
        "--prefetch-scan",
        "--scan-only",
        "--output",
        str(csv_path),
        "--stats-output",
        str(stats_path),
    ]
    if config.endpoint is not None:
        endpoint_index = command.index("--s3-region")
        command[endpoint_index:endpoint_index] = [
            "--s3-endpoint",
            config.endpoint,
        ]
    return command


def build_dashboard_command(
    config: DemoConfig,
    artifacts: Sequence[VariantArtifacts],
) -> list[str]:
    """Build the visualize_bench command for completed variant curves."""

    command = [
        sys.executable,
        "scripts/visualize_bench.py",
        *[str(artifact.csv_path) for artifact in artifacts],
        "--stats",
        *[str(artifact.stats_path) for artifact in artifacts],
        "--labels",
        *[artifact.variant.label for artifact in artifacts],
        "--output",
        str(config.dashboard),
    ]
    return command


def estimate_s3_cost(
    dataset_mib: int,
    fractions: Sequence[float] = DRAM_FRACTIONS,
    sweep_count: int = 1,
) -> CostEstimate:
    """Estimate S3 Standard request and one-day storage costs.

    Every fraction runs in a fresh process with a fresh logical dataset. Its
    initialization and first scan can persist every page once, and startup
    performs one additional smoke PUT. The low GET estimate is one lower-tier
    read pass per fraction. The high estimate uses five passes. Storage is a
    conservative uncompressed one-day total for every isolated fraction
    prefix. EC2 instance cost and retry requests are intentionally omitted.
    """

    if dataset_mib <= 0:
        raise ValueError("dataset_mib must be greater than zero")
    if sweep_count <= 0:
        raise ValueError("sweep_count must be greater than zero")
    if not fractions:
        raise ValueError("at least one fraction is required")
    if any(not math.isfinite(value) or value <= 0.0 or value > 1.0 for value in fractions):
        raise ValueError("fractions must be finite and in (0, 1]")

    page_count = math.ceil(dataset_mib * MIB / PAGE_SIZE)
    run_count = len(fractions) * sweep_count
    put_requests = (page_count + 1) * run_count
    get_requests_low = (
        sum(math.ceil(page_count * (1.0 - fraction)) for fraction in fractions)
        * sweep_count
    )
    get_requests_high = get_requests_low * GET_PASSES_HIGH
    put_cost_usd = put_requests * S3_PUT_COST_USD
    get_cost_low_usd = get_requests_low * S3_GET_COST_USD
    get_cost_high_usd = get_requests_high * S3_GET_COST_USD
    storage_cost_usd = (
        dataset_mib
        * MIB
        / GIB
        * S3_STORAGE_GIB_MONTH_USD
        / 30.0
        * run_count
    )
    return CostEstimate(
        put_requests=put_requests,
        get_requests_low=get_requests_low,
        get_requests_high=get_requests_high,
        put_cost_usd=put_cost_usd,
        get_cost_low_usd=get_cost_low_usd,
        get_cost_high_usd=get_cost_high_usd,
        storage_cost_usd=storage_cost_usd,
        total_low_usd=put_cost_usd + get_cost_low_usd + storage_cost_usd,
        total_high_usd=put_cost_usd + get_cost_high_usd + storage_cost_usd,
    )


def print_cost_estimate(config: DemoConfig, estimate: CostEstimate) -> None:
    """Print the estimated request count and S3 charge before execution."""

    target_fraction = config.dram_mib / config.dataset_mib
    sweep_word = "sweeps" if config.compare_prefetch else "sweep"
    print("Estimated S3 cost (EC2 instance cost excluded):")
    print(
        f"  dataset: {config.dataset_mib:,} MiB; target DRAM: "
        f"{config.dram_mib:,} MiB ({target_fraction:.5%})"
    )
    print(
        f"  PUT: {estimate.put_requests:,} requests "
        f"(${estimate.put_cost_usd:.2f})"
    )
    print(
        f"  GET: {estimate.get_requests_low:,}--"
        f"{estimate.get_requests_high:,} requests "
        f"(${estimate.get_cost_low_usd:.2f}--"
        f"${estimate.get_cost_high_usd:.2f})"
    )
    print(f"  one-day storage: ${estimate.storage_cost_usd:.2f}")
    print(
        f"  estimated total for {len(variants_for(config))} {sweep_word}: "
        f"${estimate.total_low_usd:.2f}--${estimate.total_high_usd:.2f}"
    )


def confirm_run(input_fn: Callable[[str], str] = input) -> bool:
    """Ask for confirmation and return whether the demonstration should run."""

    try:
        response = input_fn("Proceed with the S3 benchmark? [y/N] ")
    except EOFError:
        return False
    return response.strip().lower() in {"y", "yes"}


def run_command(command: Sequence[str], dry_run: bool) -> None:
    """Run one command, or print it without executing in dry-run mode."""

    printable = shlex.join(command)
    if dry_run:
        print(printable)
        return
    print(f"+ {printable}", flush=True)
    subprocess.run(command, check=True)


def merge_csv_files(paths: Sequence[Path], output: Path) -> None:
    """Merge compatible per-fraction benchmark CSV files into one curve."""

    if not paths:
        raise DemoDataError("cannot merge an empty CSV path list")

    header: list[str] | None = None
    rows: list[list[str]] = []
    for path in paths:
        try:
            with path.open(newline="", encoding="utf-8") as source:
                reader = csv.reader(source)
                current_header = next(reader, None)
                current_rows = list(reader)
        except OSError as error:
            raise DemoDataError(f"{path}: could not read benchmark CSV") from error
        if current_header is None:
            raise DemoDataError(f"{path}: benchmark CSV is empty")
        if header is None:
            header = current_header
        elif current_header != header:
            raise DemoDataError(f"{path}: benchmark CSV header does not match the sweep")
        if not current_rows:
            raise DemoDataError(f"{path}: benchmark CSV contains no data rows")
        rows.extend(current_rows)

    try:
        with output.open("w", newline="", encoding="utf-8") as destination:
            writer = csv.writer(destination, lineterminator="\n")
            writer.writerow(header)
            writer.writerows(rows)
    except OSError as error:
        raise DemoDataError(f"{output}: could not write combined benchmark CSV") from error


def merge_stats_files(paths: Sequence[Path], output: Path) -> None:
    """Merge per-fraction tierbuf stats JSON documents into one document."""

    if not paths:
        raise DemoDataError("cannot merge an empty stats path list")

    runs: list[object] = []
    schema_version: object = 1
    capture_point: object = "after_measurement_before_shutdown"
    for index, path in enumerate(paths):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            raise DemoDataError(f"{path}: could not read benchmark stats JSON") from error
        if not isinstance(payload, dict) or not isinstance(payload.get("runs"), list):
            raise DemoDataError(f"{path}: stats JSON must contain a runs array")
        if index == 0:
            schema_version = payload.get("schema_version", schema_version)
            capture_point = payload.get("capture_point", capture_point)
        runs.extend(payload["runs"])

    combined = {
        "schema_version": schema_version,
        "capture_point": capture_point,
        "runs": runs,
    }
    try:
        output.write_text(
            json.dumps(combined, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    except OSError as error:
        raise DemoDataError(f"{output}: could not write combined stats JSON") from error


def _s3_records(value: object) -> list[Mapping[str, object]]:
    """Return all S3 counter records embedded in a stats JSON value."""

    records: list[Mapping[str, object]] = []
    if isinstance(value, dict):
        named_s3 = value.get("s3")
        if isinstance(named_s3, dict):
            records.append(named_s3)
        elif value.get("name") == "s3" and any(
            key in value
            for key in (
                "get_requests",
                "put_requests",
                "bytes_uploaded",
                "bytes_downloaded",
            )
        ):
            records.append(value)
        for key, child in value.items():
            if key != "s3":
                records.extend(_s3_records(child))
    elif isinstance(value, list):
        for child in value:
            records.extend(_s3_records(child))
    return records


def _counter(record: Mapping[str, object], key: str) -> int:
    """Read one non-negative integer counter, treating a missing value as zero."""

    value = record.get(key, 0)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise DemoDataError(f"S3 stats field {key!r} must be numeric")
    if not math.isfinite(float(value)) or value < 0 or int(value) != value:
        raise DemoDataError(f"S3 stats field {key!r} must be a non-negative integer")
    return int(value)


def load_s3_summary(path: Path) -> S3Summary:
    """Load and total S3 request, transfer, and storage counters."""

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise DemoDataError(f"{path}: could not read S3 stats summary") from error
    records = _s3_records(payload)
    counter_names = (
        "get_requests",
        "put_requests",
        "delete_requests",
        "request_failures",
        "stored_bytes",
        "logical_bytes",
        "bytes_uploaded",
        "bytes_downloaded",
    )
    totals = {
        key: sum(_counter(record, key) for record in records)
        for key in counter_names
    }
    return S3Summary(records=len(records), **totals)


def _format_gib(byte_count: int) -> str:
    """Format a byte count as GiB with two decimal places."""

    return f"{byte_count / GIB:.2f}"


def print_s3_summary(artifacts: Sequence[VariantArtifacts]) -> None:
    """Print a compact table of actual S3 requests, transfer, and compression."""

    summaries = [
        (artifact.variant.label, load_s3_summary(artifact.stats_path))
        for artifact in artifacts
    ]
    print("\nObserved logical S3 summary (client retries excluded):")
    headings = (
        "variant",
        "GET",
        "PUT",
        "DELETE",
        "failures",
        "upload GiB",
        "download GiB",
        "stored/logical",
    )
    rows = []
    for label, summary in summaries:
        ratio = (
            f"{summary.compression_ratio:.3f}"
            if summary.compression_ratio is not None
            else "n/a"
        )
        if summary.records == 0:
            ratio = "n/a"
        rows.append(
            (
                label,
                f"{summary.get_requests:,}",
                f"{summary.put_requests:,}",
                f"{summary.delete_requests:,}",
                f"{summary.request_failures:,}",
                _format_gib(summary.bytes_uploaded),
                _format_gib(summary.bytes_downloaded),
                ratio,
            )
        )
    widths = [
        max(len(headings[index]), *(len(row[index]) for row in rows))
        for index in range(len(headings))
    ]
    print("  ".join(value.ljust(widths[index]) for index, value in enumerate(headings)))
    print("  ".join("-" * width for width in widths))
    for row in rows:
        print("  ".join(value.ljust(widths[index]) for index, value in enumerate(row)))


def run_demo(
    config: DemoConfig,
    input_fn: Callable[[str], str] = input,
) -> bool:
    """Run all fractions, render the dashboard, and print actual S3 totals."""

    variants = variants_for(config)
    estimate = estimate_s3_cost(
        config.dataset_mib,
        sweep_count=len(variants),
    )
    print_cost_estimate(config, estimate)
    if not config.yes and not config.dry_run and not confirm_run(input_fn):
        print("S3 benchmark cancelled.")
        return False

    if not config.dry_run:
        config.output_dir.mkdir(parents=True, exist_ok=True)
        config.dashboard.parent.mkdir(parents=True, exist_ok=True)

    artifacts = []
    for variant in variants:
        fraction_csv_paths = []
        fraction_stats_paths = []
        for fraction in fractions_for_demo():
            csv_path = csv_path_for(config.output_dir, variant, fraction)
            stats_path = stats_path_for(config.output_dir, variant, fraction)
            fraction_csv_paths.append(csv_path)
            fraction_stats_paths.append(stats_path)
            run_command(
                build_benchmark_command(
                    config,
                    variant,
                    fraction,
                    csv_path,
                    stats_path,
                ),
                config.dry_run,
            )

        combined_csv = combined_csv_path_for(config.output_dir, variant)
        combined_stats = combined_stats_path_for(config.output_dir, variant)
        if not config.dry_run:
            merge_csv_files(fraction_csv_paths, combined_csv)
            merge_stats_files(fraction_stats_paths, combined_stats)
        artifacts.append(VariantArtifacts(variant, combined_csv, combined_stats))

    run_command(build_dashboard_command(config, artifacts), config.dry_run)
    if config.dry_run:
        print("Actual S3 summary is available after a non-dry run.")
    else:
        print_s3_summary(artifacts)
        print(f"\nwrote {config.dashboard}")
    return True


def build_parser() -> argparse.ArgumentParser:
    """Build the S3 cliff demonstration command-line parser."""

    parser = argparse.ArgumentParser(
        description=(
            "run tierbuf's S3 degradation curve and render a benchmark dashboard"
        )
    )
    parser.add_argument(
        "--local",
        action="store_true",
        help="use local defaults: 2 GiB dataset and 256 MiB target DRAM",
    )
    parser.add_argument(
        "--endpoint",
        help="custom S3 endpoint (required with --local, for example MinIO)",
    )
    parser.add_argument("--bucket", required=True, help="S3 bucket name")
    parser.add_argument(
        "--region",
        default=DEFAULT_REGION,
        help=f"S3 region (default: {DEFAULT_REGION})",
    )
    parser.add_argument(
        "--dataset-mib",
        type=int,
        help="logical dataset size in MiB (local: 2048; AWS: 32768)",
    )
    parser.add_argument(
        "--dram-mib",
        type=int,
        help="target DRAM size represented in the fixed fraction sweep",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help=f"CSV and stats output directory (default: {DEFAULT_OUTPUT_DIR})",
    )
    parser.add_argument(
        "--dashboard",
        type=Path,
        default=DEFAULT_DASHBOARD,
        help=f"HTML dashboard path (default: {DEFAULT_DASHBOARD})",
    )
    parser.add_argument(
        "--compare-prefetch",
        action="store_true",
        help="compare prefetch worker counts 4 and 64",
    )
    parser.add_argument(
        "--prefetch-in-flight",
        type=int,
        default=DEFAULT_PREFETCH_IN_FLIGHT,
        help=(
            "maximum queued-plus-running prefetches "
            f"(default: {DEFAULT_PREFETCH_IN_FLIGHT})"
        ),
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="accept the estimated S3 cost without prompting",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="print commands without creating files or contacting S3",
    )
    return parser


def config_from_args(arguments: argparse.Namespace) -> DemoConfig:
    """Convert parsed arguments into a validated demo configuration."""

    default_dataset = LOCAL_DATASET_MIB if arguments.local else AWS_DATASET_MIB
    default_dram = LOCAL_DRAM_MIB if arguments.local else AWS_DRAM_MIB
    dataset_mib = (
        arguments.dataset_mib
        if arguments.dataset_mib is not None
        else default_dataset
    )
    if arguments.dram_mib is not None:
        dram_mib = arguments.dram_mib
    elif arguments.dataset_mib is None:
        dram_mib = default_dram
    else:
        divisor = 8 if arguments.local else 32
        dram_mib = max(1, dataset_mib // divisor)

    if arguments.local and not arguments.endpoint:
        raise argparse.ArgumentTypeError("--local requires --endpoint")
    if arguments.endpoint is not None and not arguments.endpoint.strip():
        raise argparse.ArgumentTypeError("--endpoint must not be empty")
    if not arguments.bucket.strip():
        raise argparse.ArgumentTypeError("--bucket must not be empty")
    if not arguments.region.strip():
        raise argparse.ArgumentTypeError("--region must not be empty")
    if dataset_mib <= 0:
        raise argparse.ArgumentTypeError("--dataset-mib must be greater than zero")
    if dram_mib <= 0:
        raise argparse.ArgumentTypeError("--dram-mib must be greater than zero")
    if dram_mib > dataset_mib:
        raise argparse.ArgumentTypeError("--dram-mib must not exceed --dataset-mib")
    target_fraction = dram_mib / dataset_mib
    if not any(
        math.isclose(target_fraction, fraction, rel_tol=0.0, abs_tol=1.0e-12)
        for fraction in fractions_for_demo()
    ):
        choices = ", ".join(fraction_label(value) for value in fractions_for_demo())
        raise argparse.ArgumentTypeError(
            "--dram-mib / --dataset-mib must match a swept fraction "
            f"({choices}); got {target_fraction:g}"
        )
    if arguments.prefetch_in_flight < DEFAULT_PREFETCH_WORKERS:
        raise argparse.ArgumentTypeError(
            f"--prefetch-in-flight must be at least {DEFAULT_PREFETCH_WORKERS}"
        )
    if arguments.prefetch_in_flight > 4096:
        raise argparse.ArgumentTypeError(
            "--prefetch-in-flight must not exceed 4096"
        )
    if not str(arguments.output_dir):
        raise argparse.ArgumentTypeError("--output-dir must not be empty")
    if not str(arguments.dashboard):
        raise argparse.ArgumentTypeError("--dashboard must not be empty")

    return DemoConfig(
        local=arguments.local,
        endpoint=arguments.endpoint,
        bucket=arguments.bucket,
        region=arguments.region,
        dataset_mib=dataset_mib,
        dram_mib=dram_mib,
        output_dir=arguments.output_dir,
        dashboard=arguments.dashboard,
        compare_prefetch=arguments.compare_prefetch,
        prefetch_in_flight=arguments.prefetch_in_flight,
        yes=arguments.yes,
        dry_run=arguments.dry_run,
    )


def main(argv: Sequence[str] | None = None) -> int:
    """Run the S3 cliff demonstration CLI."""

    parser = build_parser()
    try:
        config = config_from_args(parser.parse_args(argv))
        run_demo(config)
    except argparse.ArgumentTypeError as error:
        parser.error(str(error))
    except subprocess.CalledProcessError as error:
        print(f"S3 cliff demonstration failed: {error}", file=sys.stderr)
        return error.returncode or 1
    except (DemoDataError, OSError) as error:
        print(f"S3 cliff demonstration failed: {error}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        print("\nS3 cliff demonstration interrupted.", file=sys.stderr)
        return 130
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
