#!/usr/bin/env python3
"""Render tierbuf benchmark CSV files as the interactive HTML dashboard."""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from string import Template
from typing import Sequence

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import degradation  # noqa: E402

DEFAULT_OUTPUT = Path("results/curve.html")
DEFAULT_SHARED_ESTIMATES = Path(
    "crates/tierbuf/target/criterion/hot_fix/shared/new/estimates.json"
)
DEFAULT_SNAPSHOT_ESTIMATES = Path(
    "crates/tierbuf/target/criterion/hot_fix/safe_optimistic_snapshot/new/estimates.json"
)
DRAM_PRICE_GIB_MONTH = 4.5
LOWER_PRICE_GIB_MONTH = 0.08


@dataclass(frozen=True)
class Run:
    """One named degradation-curve benchmark run."""

    label: str
    points: tuple[degradation.CurvePoint, ...]

    @property
    def base(self) -> degradation.CurvePoint:
        """Return the all-DRAM baseline point."""

        for point in self.points:
            if point.fraction == 1.0:
                return point
        raise degradation.CurveDataError(f"{self.label}: missing fraction 1.0 baseline")


@dataclass(frozen=True)
class HotPathEstimate:
    """One Criterion mean estimate rendered in the hot-path panel."""

    label: str
    mean_ns: float


def load_runs(paths: Sequence[Path], labels: Sequence[str] | None) -> list[Run]:
    """Load and label one or more benchmark CSV files."""

    if labels is not None and len(labels) != len(paths):
        raise degradation.CurveDataError(
            f"--labels expected {len(paths)} value(s), got {len(labels)}"
        )

    runs = []
    for index, path in enumerate(paths):
        label = labels[index] if labels is not None else default_label(path, index)
        runs.append(Run(label=label, points=tuple(degradation.load_curve(path))))
    return runs


def default_label(path: Path, index: int) -> str:
    """Return a stable human-readable label for a CSV path."""

    stem = path.stem.replace("_", " ").replace("-", " ").strip()
    return stem or f"run {index + 1}"


def load_hotpath_estimates(
    shared_path: Path | None,
    snapshot_path: Path | None,
) -> list[HotPathEstimate]:
    """Load optional Criterion hot-path means when both estimates exist."""

    if shared_path is None or snapshot_path is None:
        return []
    if not shared_path.exists() or not snapshot_path.exists():
        return []
    return [
        HotPathEstimate("shared fix", read_mean_ns(shared_path)),
        HotPathEstimate("safe optimistic snapshot", read_mean_ns(snapshot_path)),
    ]


def read_mean_ns(path: Path) -> float:
    """Read Criterion's mean point estimate from an estimates.json file."""

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        mean = float(payload["mean"]["point_estimate"])
    except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
        raise degradation.CurveDataError(
            f"{path}: could not read Criterion mean point_estimate"
        ) from error
    if mean <= 0.0:
        raise degradation.CurveDataError(f"{path}: mean point_estimate must be positive")
    return mean


def render_report(
    runs: Sequence[Run],
    dram_price: float = DRAM_PRICE_GIB_MONTH,
    lower_price: float = LOWER_PRICE_GIB_MONTH,
    hotpath: Sequence[HotPathEstimate] = (),
) -> str:
    """Render a full standalone HTML report using the chat dashboard layout."""

    run_payload = [
        {
            "name": run.label,
            "values": [
                {
                    "fraction": point.fraction,
                    "throughput": point.throughput_ops,
                    "p50": point.p50_us,
                    "p99": point.p99_us,
                    "cost": point.cost_usd_per_1e6ops,
                    "pointOps": point.point_ops,
                    "pointThroughput": point.point_throughput_ops,
                    "pointP50": point.point_p50_us,
                    "pointP99": point.point_p99_us,
                    "scanOps": point.scan_ops,
                    "scanThroughput": point.scan_throughput_ops,
                    "scanP50": point.scan_p50_us,
                    "scanP99": point.scan_p99_us,
                    "dramHitRate": point.dram_hit_rate,
                    "lowerTierHitRate": point.lower_tier_hit_rate,
                    "pointDramHitRate": point.point_dram_hit_rate,
                    "scanDramHitRate": point.scan_dram_hit_rate,
                }
                for point in run.points
            ],
        }
        for run in runs
    ]
    hotpath_payload = [
        {"label": estimate.label, "meanNs": estimate.mean_ns} for estimate in hotpath
    ]

    hotpath_section = ""
    if hotpath_payload:
        hotpath_section = """
      <section>
        <h2>Hot-path Criterion</h2>
        <p>Mean latency for resident shared fix and safe optimistic snapshot.</p>
        <svg id="hotpath-chart" viewBox="0 0 680 300" role="img" aria-labelledby="hotpath-title hotpath-desc">
          <title id="hotpath-title">Hot-path Criterion mean latency</title>
          <desc id="hotpath-desc">Compares resident shared fix latency with safe optimistic snapshot latency.</desc>
        </svg>
      </section>"""

    has_extended_metrics = all(
        point.has_extended_metrics for run in runs for point in run.points
    )
    extended_sections = ""
    if has_extended_metrics:
        extended_sections = """
      <section>
        <h2>Operation throughput by type</h2>
        <p>Solid lines are point operations; dashed lines are scan operations.</p>
        <svg id="operation-chart" viewBox="0 0 680 330" role="img" aria-labelledby="operation-title operation-desc">
          <title id="operation-title">Point and scan throughput by DRAM resident fraction</title>
          <desc id="operation-desc">Separates completed operation throughput into point and scan traffic for every run.</desc>
        </svg>
      </section>
      <section>
        <h2>Demand hit rate by tier</h2>
        <p>Solid lines are DRAM hits; dashed lines are lower-tier demand restores. Prefetch I/O is excluded.</p>
        <svg id="hit-rate-chart" viewBox="0 0 680 330" role="img" aria-labelledby="hit-rate-title hit-rate-desc">
          <title id="hit-rate-title">DRAM and lower-tier demand hit rates</title>
          <desc id="hit-rate-desc">Shows the share of completed demand fixes served from DRAM and restored from a lower tier.</desc>
        </svg>
      </section>
      <section>
        <h2>DRAM hit rate by operation type</h2>
        <p>Solid lines are point operations; dashed lines are scan operations.</p>
        <svg id="operation-hit-rate-chart" viewBox="0 0 680 330" role="img" aria-labelledby="operation-hit-rate-title operation-hit-rate-desc">
          <title id="operation-hit-rate-title">Point and scan DRAM hit rates</title>
          <desc id="operation-hit-rate-desc">Separates DRAM hit rates for point and scan operations at every resident fraction.</desc>
        </svg>
      </section>"""

    return HTML_TEMPLATE.substitute(
        runs_json=json.dumps(run_payload, separators=(",", ":")),
        hotpath_json=json.dumps(hotpath_payload, separators=(",", ":")),
        has_extended_metrics_json=json.dumps(has_extended_metrics),
        dram_price=f"{dram_price:.2f}",
        lower_price=f"{lower_price:.2f}",
        throughput_limit=degradation.THROUGHPUT_RATIO_LIMIT,
        p99_limit=degradation.P99_RATIO_LIMIT,
        hotpath_section=hotpath_section,
        extended_sections=extended_sections,
    )


HTML_TEMPLATE = Template(
    """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>tierbuf benchmark dashboard</title>
  <style>
    :root {
      color-scheme: light dark;
      --background: Canvas;
      --foreground: CanvasText;
      --card: color-mix(in srgb, CanvasText 4%, Canvas 96%);
      --muted: color-mix(in srgb, CanvasText 66%, Canvas 34%);
      --border: color-mix(in srgb, CanvasText 18%, Canvas 82%);
      --series-1: #2563eb;
      --series-2: #16a34a;
      --series-3: #dc2626;
      --series-4: #9333ea;
      --series-5: #0891b2;
      --series-6: #ca8a04;
    }
    body {
      margin: 0;
      color: var(--foreground);
      background: var(--background);
      font-family: ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }
    main {
      position: relative;
      max-width: 1180px;
      margin: 0 auto;
      padding: 32px 20px 48px;
    }
    h1 {
      margin: 0 0 8px;
      font-size: 28px;
      line-height: 1.2;
      font-weight: 650;
      letter-spacing: 0;
    }
    h2 {
      margin: 0 0 6px;
      font-size: 16px;
      line-height: 1.25;
      font-weight: 650;
      letter-spacing: 0;
    }
    p {
      margin: 0;
      color: var(--muted);
    }
    .legend {
      display: flex;
      flex-wrap: wrap;
      gap: 12px;
      margin-top: 16px;
      color: var(--muted);
      font-size: 13px;
    }
    .legend span {
      display: inline-flex;
      align-items: center;
      gap: 6px;
    }
    .swatch {
      width: 10px;
      height: 10px;
      border-radius: 999px;
      background: currentColor;
    }
    .grid {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 28px;
      margin-top: 28px;
      align-items: start;
    }
    .wide {
      grid-column: 1 / -1;
    }
    svg {
      width: 100%;
      height: auto;
      display: block;
      margin-top: 10px;
      overflow: visible;
    }
    .axis,
    .grid-line {
      stroke: var(--border);
      stroke-width: 1;
      vector-effect: non-scaling-stroke;
    }
    .line {
      fill: none;
      stroke-width: 2.25;
      vector-effect: non-scaling-stroke;
    }
    .point {
      stroke: var(--background);
      stroke-width: 1.5;
      vector-effect: non-scaling-stroke;
    }
    .band {
      fill: var(--series-1);
      opacity: 0.12;
    }
    .target {
      stroke: var(--foreground);
      stroke-width: 1.5;
      stroke-dasharray: 6 6;
      opacity: 0.65;
      vector-effect: non-scaling-stroke;
    }
    .tick,
    .label {
      fill: var(--muted);
      font-size: 12px;
    }
    .value {
      fill: var(--foreground);
      font-size: 12px;
      font-weight: 650;
    }
    table {
      width: 100%;
      border-collapse: collapse;
      margin-top: 10px;
      font-size: 14px;
    }
    th,
    td {
      border-bottom: 1px solid var(--border);
      padding: 9px 8px;
      text-align: right;
      vertical-align: top;
    }
    th:first-child,
    td:first-child {
      text-align: left;
    }
    th {
      color: var(--muted);
      font-weight: 650;
    }
    .tooltip {
      position: absolute;
      pointer-events: none;
      opacity: 0;
      max-width: 260px;
      padding: 8px 10px;
      border: 1px solid var(--border);
      border-radius: 8px;
      color: var(--foreground);
      background: var(--card);
      box-shadow: 0 12px 28px color-mix(in srgb, CanvasText 18%, transparent);
      font-size: 12px;
      line-height: 1.45;
    }
    @media (max-width: 760px) {
      main {
        padding-inline: 14px;
      }
      .grid {
        grid-template-columns: 1fr;
      }
    }
  </style>
</head>
<body>
  <main id="dashboard">
    <h1>tierbuf benchmark dashboard</h1>
    <p>Normalized degradation curves from benchmark CSV files, rendered with the same dashboard shape used in the chat visualization.</p>
    <div id="legend" class="legend" aria-label="Benchmark run legend"></div>
    <div class="grid">
      <section>
        <h2>Throughput retention vs. all-DRAM</h2>
        <p>Higher is better. The dashed line marks the ${throughput_limit}x degradation limit.</p>
        <svg id="throughput-chart" viewBox="0 0 680 330" role="img" aria-labelledby="throughput-title throughput-desc">
          <title id="throughput-title">Throughput retention by DRAM resident fraction</title>
          <desc id="throughput-desc">Shows throughput retained at each DRAM resident fraction relative to each run's all-DRAM baseline.</desc>
        </svg>
      </section>
      <section>
        <h2>p99 latency multiplier vs. all-DRAM</h2>
        <p>Lower is better. The dashed line marks the ${p99_limit}x latency limit.</p>
        <svg id="p99-chart" viewBox="0 0 680 330" role="img" aria-labelledby="p99-title p99-desc">
          <title id="p99-title">p99 latency multiplier by DRAM resident fraction</title>
          <desc id="p99-desc">Shows p99 latency growth at each DRAM resident fraction relative to each run's all-DRAM baseline.</desc>
        </svg>
      </section>
      <section>
        <h2>Capacity cost saving</h2>
        <p>Uses DRAM $$${dram_price}/GiB-month and lower tier $$${lower_price}/GiB-month.</p>
        <svg id="savings-chart" viewBox="0 0 680 300" role="img" aria-labelledby="savings-title savings-desc">
          <title id="savings-title">Capacity cost saving by DRAM resident fraction</title>
          <desc id="savings-desc">Shows estimated capacity-cost saving relative to an all-DRAM pool.</desc>
        </svg>
      </section>
$extended_sections
$hotpath_section
      <section class="wide">
        <h2>Summary by DRAM resident fraction</h2>
        <table>
          <thead>
            <tr>
              <th>DRAM resident</th>
              <th>Throughput retention range</th>
              <th>p99 multiplier range</th>
              <th>Capacity cost saving</th>
              <th>Unit work cost range</th>
            </tr>
          </thead>
          <tbody id="summary-body"></tbody>
        </table>
      </section>
    </div>
    <div id="tooltip" class="tooltip" role="status" aria-live="polite"></div>
  </main>
  <script>
    (function () {
      const runs = $runs_json;
      const hotPath = $hotpath_json;
      const hasExtendedMetrics = $has_extended_metrics_json;
      const dramPrice = Number("$dram_price");
      const lowerPrice = Number("$lower_price");
      const throughputLimit = Number("$throughput_limit");
      const p99Limit = Number("$p99_limit");
      const colors = ["var(--series-1)", "var(--series-2)", "var(--series-3)", "var(--series-4)", "var(--series-5)", "var(--series-6)"];
      const root = document.getElementById("dashboard");
      const tooltip = document.getElementById("tooltip");

      function pct(value, digits) {
        return value.toFixed(digits === undefined ? 0 : digits) + "%";
      }

      function mult(value) {
        return value.toFixed(2) + "x";
      }

      function median(values) {
        const sorted = values.slice().sort((a, b) => a - b);
        const mid = Math.floor(sorted.length / 2);
        return sorted.length % 2 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
      }

      function extent(values) {
        return [Math.min.apply(null, values), Math.max.apply(null, values)];
      }

      function capacitySavings(fraction) {
        const ratio = (fraction * dramPrice + (1 - fraction) * lowerPrice) / dramPrice;
        return (1 - ratio) * 100;
      }

      function enrichRun(run, index) {
        const base = run.values.find((item) => item.fraction === 1.0);
        return {
          name: run.name,
          color: colors[index % colors.length],
          values: run.values.map((item) => ({
            fraction: item.fraction,
            throughput: item.throughput,
            p50: item.p50,
            p99: item.p99,
            cost: item.cost,
            pointOps: item.pointOps,
            pointThroughput: item.pointThroughput,
            pointP50: item.pointP50,
            pointP99: item.pointP99,
            scanOps: item.scanOps,
            scanThroughput: item.scanThroughput,
            scanP50: item.scanP50,
            scanP99: item.scanP99,
            dramHitRate: item.dramHitRate,
            lowerTierHitRate: item.lowerTierHitRate,
            pointDramHitRate: item.pointDramHitRate,
            scanDramHitRate: item.scanDramHitRate,
            retention: item.throughput / base.throughput * 100,
            p99Multiplier: item.p99 / base.p99,
            unitCostMultiplier: item.cost / base.cost
          })).sort((a, b) => b.fraction - a.fraction)
        };
      }

      const enrichedRuns = runs.map(enrichRun);
      const fractions = Array.from(new Set(enrichedRuns.flatMap((run) => run.values.map((item) => item.fraction)))).sort((a, b) => b - a);
      const byFraction = fractions.map((fraction) => {
        const points = enrichedRuns.map((run) => run.values.find((item) => item.fraction === fraction)).filter(Boolean);
        return {
          fraction,
          retentionValues: points.map((item) => item.retention),
          p99Values: points.map((item) => item.p99Multiplier),
          unitCostValues: points.map((item) => item.unitCostMultiplier),
          savings: capacitySavings(fraction)
        };
      });

      function makeSvg(tag, attrs) {
        const el = document.createElementNS("http://www.w3.org/2000/svg", tag);
        Object.entries(attrs || {}).forEach(([key, value]) => el.setAttribute(key, value));
        return el;
      }

      function scaleLinear(domain, range) {
        const d0 = domain[0];
        const d1 = domain[1];
        const r0 = range[0];
        const r1 = range[1];
        return (value) => r0 + (value - d0) * (r1 - r0) / (d1 - d0);
      }

      function pathFor(points, x, y) {
        return points.map((point, index) => (index === 0 ? "M" : "L") + x(point.fraction).toFixed(2) + "," + y(point.value).toFixed(2)).join(" ");
      }

      function addAxes(svg, cfg, yTicks, yFormat) {
        yTicks.forEach((tick) => {
          const yPos = cfg.y(tick);
          svg.appendChild(makeSvg("line", { class: "grid-line", x1: cfg.left, y1: yPos, x2: cfg.width - cfg.right, y2: yPos }));
          const label = makeSvg("text", { class: "tick", x: cfg.left - 10, y: yPos + 4, "text-anchor": "end" });
          label.textContent = yFormat(tick);
          svg.appendChild(label);
        });

        svg.appendChild(makeSvg("line", { class: "axis", x1: cfg.left, y1: cfg.height - cfg.bottom, x2: cfg.width - cfg.right, y2: cfg.height - cfg.bottom }));
        svg.appendChild(makeSvg("line", { class: "axis", x1: cfg.left, y1: cfg.top, x2: cfg.left, y2: cfg.height - cfg.bottom }));

        fractions.forEach((fraction) => {
          const xPos = cfg.x(fraction);
          const tick = makeSvg("text", { class: "tick", x: xPos, y: cfg.height - cfg.bottom + 24, "text-anchor": "middle" });
          tick.textContent = Math.round(fraction * 100) + "%";
          svg.appendChild(tick);
        });

        const xLabel = makeSvg("text", { class: "label", x: (cfg.left + cfg.width - cfg.right) / 2, y: cfg.height - 12, "text-anchor": "middle" });
        xLabel.textContent = "DRAM resident fraction";
        svg.appendChild(xLabel);
      }

      function showTooltip(evt, lines) {
        tooltip.innerHTML = lines.map((line) => "<div>" + line + "</div>").join("");
        const rootRect = root.getBoundingClientRect();
        const tipRect = tooltip.getBoundingClientRect();
        const left = Math.min(Math.max(evt.clientX - rootRect.left + 12, 0), Math.max(rootRect.width - tipRect.width, 0));
        const top = Math.max(evt.clientY - rootRect.top - tipRect.height - 12, 0);
        tooltip.style.left = left + "px";
        tooltip.style.top = top + "px";
        tooltip.style.opacity = "1";
      }

      function hideTooltip() {
        tooltip.style.opacity = "0";
      }

      function drawLineChart(svgId, options) {
        const svg = document.getElementById(svgId);
        const width = 680;
        const height = 330;
        const left = 62;
        const right = 22;
        const top = 24;
        const bottom = 58;
        const x = scaleLinear([1.0, Math.min.apply(null, fractions)], [left, width - right]);
        const y = scaleLinear(options.domain, [height - bottom, top]);
        const cfg = { left, right, top, bottom, width, height, x, y };

        addAxes(svg, cfg, options.ticks, options.format);

        if (options.target !== undefined) {
          const targetY = y(options.target);
          svg.appendChild(makeSvg("line", { class: "target", x1: left, y1: targetY, x2: width - right, y2: targetY }));
          const targetLabel = makeSvg("text", { class: "label", x: width - right, y: targetY - 10, "text-anchor": "end" });
          targetLabel.textContent = options.targetLabel;
          svg.appendChild(targetLabel);
        }

        if (options.band) {
          const upper = byFraction.map((item) => ({ fraction: item.fraction, value: Math.max.apply(null, item[options.band]) }));
          const lower = byFraction.slice().reverse().map((item) => ({ fraction: item.fraction, value: Math.min.apply(null, item[options.band]) }));
          const bandPath = pathFor(upper, x, y) + " " + pathFor(lower, x, y).replace(/^M/, "L") + " Z";
          svg.appendChild(makeSvg("path", { class: "band", d: bandPath }));
        }

        enrichedRuns.forEach((run) => {
          const points = run.values.map((item) => ({ fraction: item.fraction, value: options.value(item), raw: item }));
          svg.appendChild(makeSvg("path", { class: "line", d: pathFor(points, x, y), stroke: run.color }));
          points.forEach((point) => {
            const circle = makeSvg("circle", { class: "point", cx: x(point.fraction), cy: y(point.value), r: 4.5, fill: run.color });
            const lines = [
              run.name + " · DRAM " + Math.round(point.fraction * 100) + "%",
              options.tooltipLabel + ": " + options.tooltipFormat(point.value),
              "throughput: " + Math.round(point.raw.throughput).toLocaleString() + " ops/s",
              "p99: " + point.raw.p99.toFixed(3) + " us"
            ];
            circle.addEventListener("mouseenter", (evt) => showTooltip(evt, lines));
            circle.addEventListener("mousemove", (evt) => showTooltip(evt, lines));
            circle.addEventListener("mouseleave", hideTooltip);
            svg.appendChild(circle);
          });
        });

        byFraction.forEach((item) => {
          const med = options.median(item);
          const label = makeSvg("text", { class: "value", x: x(item.fraction), y: y(med) - 14, "text-anchor": "middle" });
          label.textContent = options.labelFormat(med);
          svg.appendChild(label);
        });
      }

      function drawSavingsChart() {
        const svg = document.getElementById("savings-chart");
        const width = 680;
        const height = 300;
        const left = 62;
        const right = 22;
        const top = 24;
        const bottom = 58;
        const x = scaleLinear([1.0, Math.min.apply(null, fractions)], [left, width - right]);
        const y = scaleLinear([0, 95], [height - bottom, top]);
        const cfg = { left, right, top, bottom, width, height, x, y };
        addAxes(svg, cfg, [0, 20, 40, 60, 80], (tick) => pct(tick));

        const points = byFraction.map((item) => ({ fraction: item.fraction, value: item.savings }));
        svg.appendChild(makeSvg("path", { class: "line", d: pathFor(points, x, y), stroke: "var(--foreground)" }));
        points.forEach((point) => {
          const circle = makeSvg("circle", { class: "point", cx: x(point.fraction), cy: y(point.value), r: 5, fill: "var(--foreground)" });
          circle.addEventListener("mouseenter", (evt) => showTooltip(evt, [
            "DRAM " + Math.round(point.fraction * 100) + "%",
            "capacity cost saving: " + pct(point.value, 1)
          ]));
          circle.addEventListener("mouseleave", hideTooltip);
          svg.appendChild(circle);
          const label = makeSvg("text", { class: "value", x: x(point.fraction), y: y(point.value) - 14, "text-anchor": "middle" });
          label.textContent = pct(point.value, 0);
          svg.appendChild(label);
        });
      }

      function drawPairedMetricChart(svgId, options) {
        const svg = document.getElementById(svgId);
        if (!svg || !hasExtendedMetrics) {
          return;
        }
        const width = 680;
        const height = 330;
        const left = 72;
        const right = 22;
        const top = 24;
        const bottom = 58;
        const x = scaleLinear([1.0, Math.min.apply(null, fractions)], [left, width - right]);
        const y = scaleLinear(options.domain, [height - bottom, top]);
        const cfg = { left, right, top, bottom, width, height, x, y };
        addAxes(svg, cfg, options.ticks, options.format);

        enrichedRuns.forEach((run) => {
          options.metrics.forEach((metric, metricIndex) => {
            const points = run.values.map((item) => ({
              fraction: item.fraction,
              value: metric.value(item),
              raw: item
            }));
            const path = makeSvg("path", {
              class: "line",
              d: pathFor(points, x, y),
              stroke: run.color
            });
            if (metricIndex === 1) {
              path.setAttribute("stroke-dasharray", "9 7");
            }
            svg.appendChild(path);
            points.forEach((point) => {
              const circle = makeSvg("circle", {
                class: "point",
                cx: x(point.fraction),
                cy: y(point.value),
                r: metricIndex === 0 ? 4.5 : 3.5,
                fill: metricIndex === 0 ? run.color : "var(--card)",
                stroke: run.color,
                "stroke-width": 2
              });
              const lines = [
                run.name + " · DRAM " + Math.round(point.fraction * 100) + "%",
                metric.label + ": " + options.tooltipFormat(point.value),
                "point p99: " + point.raw.pointP99.toFixed(3) + " us",
                "scan p99: " + point.raw.scanP99.toFixed(3) + " us"
              ];
              circle.addEventListener("mouseenter", (evt) => showTooltip(evt, lines));
              circle.addEventListener("mousemove", (evt) => showTooltip(evt, lines));
              circle.addEventListener("mouseleave", hideTooltip);
              svg.appendChild(circle);
            });
          });
        });
      }

      function drawHotpathChart() {
        const svg = document.getElementById("hotpath-chart");
        if (!svg || hotPath.length === 0) {
          return;
        }
        const width = 680;
        const height = 300;
        const left = 190;
        const right = 36;
        const top = 48;
        const rowGap = 72;
        const max = Math.max.apply(null, hotPath.map((item) => item.meanNs)) * 1.08;
        const x = scaleLinear([0, max], [left, width - right]);
        const tickStep = max > 1500 ? 500 : 50;
        const ticks = [];
        for (let tick = 0; tick <= max; tick += tickStep) {
          ticks.push(tick);
        }

        ticks.forEach((tick) => {
          const xPos = x(tick);
          svg.appendChild(makeSvg("line", { class: "grid-line", x1: xPos, y1: top - 26, x2: xPos, y2: top + rowGap * hotPath.length - 18 }));
          const label = makeSvg("text", { class: "tick", x: xPos, y: top + rowGap * hotPath.length + 10, "text-anchor": "middle" });
          label.textContent = tick === 0 ? "0ns" : tick + "ns";
          svg.appendChild(label);
        });

        hotPath.forEach((item, index) => {
          const y = top + index * rowGap;
          const label = makeSvg("text", { class: "label", x: left - 16, y: y + 11, "text-anchor": "end" });
          label.textContent = item.label;
          svg.appendChild(label);
          svg.appendChild(makeSvg("rect", { x: left, y, width: Math.max(x(item.meanNs) - left, 1), height: 22, fill: colors[index % colors.length], opacity: "0.85" }));
          const value = makeSvg("text", { class: "value", x: x(item.meanNs) + 8, y: y + 12 });
          value.textContent = item.meanNs >= 1000 ? (item.meanNs / 1000).toFixed(2) + "us" : item.meanNs.toFixed(1) + "ns";
          svg.appendChild(value);
        });

        if (hotPath.length >= 2) {
          const ratio = hotPath[1].meanNs / hotPath[0].meanNs;
          const note = makeSvg("text", { class: "value", x: left, y: height - 26 });
          note.textContent = "snapshot mean is " + ratio.toFixed(1) + "x slower because it copies a full 64KiB page safely";
          svg.appendChild(note);
        }
      }

      function fillLegend() {
        const legend = document.getElementById("legend");
        enrichedRuns.forEach((run) => {
          const item = document.createElement("span");
          const swatch = document.createElement("i");
          swatch.className = "swatch";
          swatch.style.color = run.color;
          item.appendChild(swatch);
          item.appendChild(document.createTextNode(run.name));
          legend.appendChild(item);
        });
        const target = document.createElement("span");
        const swatch = document.createElement("i");
        swatch.className = "swatch";
        swatch.style.color = "var(--foreground)";
        target.appendChild(swatch);
        target.appendChild(document.createTextNode("Median / target"));
        legend.appendChild(target);
        if (hasExtendedMetrics) {
          const metricStyles = document.createElement("span");
          metricStyles.appendChild(
            document.createTextNode(
              "Solid: point / DRAM · Dashed: scan / lower tier"
            )
          );
          legend.appendChild(metricStyles);
        }
      }

      function fillSummaryTable() {
        const body = document.getElementById("summary-body");
        fractions.filter((fraction) => fraction !== 1.0).forEach((fraction) => {
          const item = byFraction.find((entry) => entry.fraction === fraction);
          const retentionRange = extent(item.retentionValues);
          const p99Range = extent(item.p99Values);
          const unitCostRange = extent(item.unitCostValues);
          const row = document.createElement("tr");
          row.innerHTML = [
            "<td>" + Math.round(fraction * 100) + "%</td>",
            "<td>" + pct(retentionRange[0], 1) + " - " + pct(retentionRange[1], 1) + "</td>",
            "<td>" + mult(p99Range[0]) + " - " + mult(p99Range[1]) + "</td>",
            "<td>" + pct(item.savings, 1) + "</td>",
            "<td>" + mult(unitCostRange[0]) + " - " + mult(unitCostRange[1]) + "</td>"
          ].join("");
          body.appendChild(row);
        });
      }

      fillLegend();
      drawLineChart("throughput-chart", {
        domain: [0, 110],
        ticks: [0, 25, 50, 75, 100],
        format: (tick) => pct(tick),
        value: (item) => item.retention,
        tooltipLabel: "retention",
        tooltipFormat: (value) => pct(value, 1),
        labelFormat: (value) => pct(value, 0),
        median: (item) => median(item.retentionValues),
        target: 100 / throughputLimit,
        targetLabel: throughputLimit.toFixed(0) + "x limit",
        band: "retentionValues"
      });
      drawLineChart("p99-chart", {
        domain: [0, Math.max(4.3, p99Limit * 1.08)],
        ticks: [0, 1, 2, 3, 4],
        format: (tick) => tick + "x",
        value: (item) => item.p99Multiplier,
        tooltipLabel: "p99 multiplier",
        tooltipFormat: mult,
        labelFormat: mult,
        median: (item) => median(item.p99Values),
        target: p99Limit,
        targetLabel: p99Limit.toFixed(0) + "x limit",
        band: "p99Values"
      });
      drawSavingsChart();
      if (hasExtendedMetrics) {
        const maxOperationThroughput = Math.max.apply(
          null,
          enrichedRuns.flatMap((run) =>
            run.values.flatMap((item) => [
              item.pointThroughput,
              item.scanThroughput
            ])
          )
        );
        const operationDomainMax = Math.max(1, maxOperationThroughput * 1.08);
        drawPairedMetricChart("operation-chart", {
          domain: [0, operationDomainMax],
          ticks: [0, operationDomainMax * 0.25, operationDomainMax * 0.5, operationDomainMax * 0.75, operationDomainMax],
          format: (tick) => Math.round(tick).toLocaleString(),
          tooltipFormat: (value) => Math.round(value).toLocaleString() + " ops/s",
          metrics: [
            { label: "point throughput", value: (item) => item.pointThroughput },
            { label: "scan throughput", value: (item) => item.scanThroughput }
          ]
        });
        drawPairedMetricChart("hit-rate-chart", {
          domain: [0, 100],
          ticks: [0, 25, 50, 75, 100],
          format: (tick) => pct(tick),
          tooltipFormat: (value) => pct(value, 1),
          metrics: [
            { label: "DRAM hit rate", value: (item) => item.dramHitRate * 100 },
            { label: "lower-tier hit rate", value: (item) => item.lowerTierHitRate * 100 }
          ]
        });
        drawPairedMetricChart("operation-hit-rate-chart", {
          domain: [0, 100],
          ticks: [0, 25, 50, 75, 100],
          format: (tick) => pct(tick),
          tooltipFormat: (value) => pct(value, 1),
          metrics: [
            { label: "point DRAM hit rate", value: (item) => item.pointDramHitRate * 100 },
            { label: "scan DRAM hit rate", value: (item) => item.scanDramHitRate * 100 }
          ]
        });
      }
      drawHotpathChart();
      fillSummaryTable();
    })();
  </script>
</body>
</html>
"""
)


def build_parser() -> argparse.ArgumentParser:
    """Build the command-line parser."""

    parser = argparse.ArgumentParser(
        description="render tierbuf benchmark CSV files as an English HTML dashboard"
    )
    parser.add_argument("csv_paths", nargs="+", type=Path, help="benchmark CSV file(s)")
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help=f"HTML output path (default: {DEFAULT_OUTPUT})",
    )
    parser.add_argument(
        "--labels",
        nargs="+",
        help="optional display labels, one per CSV file",
    )
    parser.add_argument(
        "--dram-price",
        type=float,
        default=DRAM_PRICE_GIB_MONTH,
        help=f"DRAM price in USD/GiB-month (default: {DRAM_PRICE_GIB_MONTH:g})",
    )
    parser.add_argument(
        "--lower-price",
        type=float,
        default=LOWER_PRICE_GIB_MONTH,
        help=f"lower-tier price in USD/GiB-month (default: {LOWER_PRICE_GIB_MONTH:g})",
    )
    parser.add_argument(
        "--shared-estimates",
        type=Path,
        default=DEFAULT_SHARED_ESTIMATES,
        help="Criterion estimates.json for resident shared fix",
    )
    parser.add_argument(
        "--snapshot-estimates",
        type=Path,
        default=DEFAULT_SNAPSHOT_ESTIMATES,
        help="Criterion estimates.json for safe optimistic snapshot",
    )
    parser.add_argument(
        "--no-hotpath",
        action="store_true",
        help="omit the optional Criterion hot-path panel",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run the CLI and return the process exit status."""

    arguments = build_parser().parse_args(argv)
    try:
        runs = load_runs(arguments.csv_paths, arguments.labels)
        if arguments.dram_price <= 0.0:
            raise degradation.CurveDataError("--dram-price must be positive")
        if arguments.lower_price <= 0.0:
            raise degradation.CurveDataError("--lower-price must be positive")
        hotpath = (
            []
            if arguments.no_hotpath
            else load_hotpath_estimates(arguments.shared_estimates, arguments.snapshot_estimates)
        )
        report = render_report(
            runs,
            dram_price=arguments.dram_price,
            lower_price=arguments.lower_price,
            hotpath=hotpath,
        )
        arguments.output.parent.mkdir(parents=True, exist_ok=True)
        arguments.output.write_text(report, encoding="utf-8")
    except (OSError, degradation.CurveDataError) as error:
        print(f"could not render benchmark dashboard: {error}", file=sys.stderr)
        return 2

    print(f"wrote {arguments.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
