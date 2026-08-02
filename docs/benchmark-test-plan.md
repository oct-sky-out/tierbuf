# Benchmark reliability and load-test plan

This plan defines the work required to turn `tierbuf-bench` from a repeatable
read-path degradation benchmark into a trustworthy, progressively broader load
test. It deliberately separates measurement correctness, S3 prefetch behavior,
S3 usage sensitivity, FileTier compression, and mixed read/write coverage.

The current harness remains useful for one narrow question: how a fixed,
closed-loop, 70% Zipf point-read and 30% interleaved scan-read workload degrades
as DRAM shrinks. It must not be presented as a comprehensive performance score
until T20 lands.

## Non-negotiable principles

1. T16 is a gate for every new benchmark result. No paid S3 run or replacement
   compression result starts before its applicable T16 acceptance checks pass.
2. Benchmark runners pass every payload and page-consumption setting
   explicitly. Historical defaults, especially the 100% compressible payload,
   are not silently inherited.
3. Latency results identify the load model, worker count, sample count, and page
   consumption mode. Closed-loop latency is valid for synchronous embedded
   callers but is not an open-loop service SLO.
4. One experiment changes one primary axis. The plan does not form the Cartesian
   product of workload, concurrency, DRAM fraction, payload, and backend.
5. Estimated budget, measured usage, and provider billing are separate concepts.
   Reports do not label a configured-rate calculation as an AWS invoice.
6. Raw artifacts are retained. Aggregated medians never replace per-repetition
   CSV, JSON, command line, seed, commit, host, kernel, and toolchain metadata.

## Known limitations and historical-result policy

The following findings define how existing artifacts may be used:

- The fixed 1-in-64 latency stride aliases with the 8-page prefetch trigger in
  the default scan-only S3 layout. Prefetch-run `p50_us`, `p99_us`,
  `scan_p50_us`, and `scan_p99_us` are not representative. Throughput and
  request/hit counters are not invalidated by this sampling defect.
- Warmup and measurement create fresh worker state and restart every scan at the
  same worker-specific cursor. Existing scan runs describe that restart
  behavior, not a continuous steady-state scan.
- Read-only measurement workers do not create sustained dirty traffic, although
  dirty pages created during initialization can still write back during warmup
  or measurement. A run with nonzero measurement writes is not a clean
  read-only measurement.
- Existing FileTier compression runs did not enable prefetch and therefore do
  not have the stride/prefetch alias. They remain historical end-to-end results,
  but they are not an apples-to-apples baseline for the corrected harness.
- The reported 1.1% compression-off range is neither repeated-run variance nor a
  confidence interval, and it is not even cross-payload path stability. Tier
  reads in all twelve compression-off runs are identical to the digit
  (160,535 reads in 30.0007 s = 5,351 reads/s = 94% of the 5,684 IOPS `fio`
  ceiling), so that arm is clamped by the device. Any saturated configuration
  reproduces the same number, and the agreement says nothing about run-to-run
  variance in the unsaturated compression-on arm, where every point is n=1.
- Residual initialization write-back in the measurement window is asymmetric
  between arms: compression-off averaged 12,570 tier writes and compression-on
  averaged 17,233, roughly 37% more. Compression-on writes are LZ4-encoded, so
  the confound consumes CPU only in the treatment arm and is of the same order
  as the effect being measured (4–16% for `chunked`). The existing
  `chunked` result is therefore biased against compression by an unquantified
  amount, and **the sign of that result is not established** — this is stronger
  than "not an apples-to-apples baseline."
- Cumulative writes in the same runs (129,063) never reached the dataset page
  count (131,072), so initial dirty drain was still in progress when
  measurement ended. No published run reached steady state.
- Existing S3 payloads inherit the historical nearly-all-zero default. Their
  request counts remain informative, but their compression, storage, transfer,
  and CPU results are not representative of ordinary records.

Historical artifacts remain in `results/` and are labeled with the harness
version. They are not silently regenerated or deleted.

## T16 — Measurement reliability gate

T16.1, T16.2, and T16.5 address direct causes of biased or non-steady
measurements. The remaining items make later S3 and compression claims
auditable.

### T16.1 — Independent latency sampling

Replace the fixed `operation % 64 == 0` gates with a per-worker sampling RNG
that is independent of the workload RNG.

Requirements:

- Each operation has an approximately 1-in-64 chance of being timed.
- One decision feeds the overall sample and the matching point or scan sample,
  so operation classes are not timed under different gates.
- Sampling seeds and overall/point/scan sample counts are recorded in JSON.
- CSV/dashboard output warns when a percentile has fewer than the documented
  minimum number of samples. It does not print a high-precision p99 without
  surfacing the sample count.
- The existing bounded Algorithm R reservoir remains in use after the unbiased
  timing gate.

Acceptance:

- A deterministic-seed regression runs at least 100,000 scan-only accesses with
  an 8-page prefetch trigger. Both trigger and non-trigger accesses appear in
  the timed sample, and the trigger share falls inside the **absolute band
  [10.0%, 15.0%]** around the expected 12.5%. At 100,000 accesses and a 1-in-64
  gate this is about 1,563 samples, where the binomial standard error at
  p = 0.125 is 0.84 percentage points, so the band is approximately ±3σ. Do not
  implement this as a relative ±10–15% tolerance: that yields [11.25%, 14.4%],
  roughly ±1.7σ, which flakes.
- Overall, point, and scan sample-count tests pass at zero, low, and reservoir-
  capacity-exceeding operation counts.
- Sampling does not consume or otherwise perturb the workload RNG sequence.

### T16.2 — Continuous warmup-to-measurement workers

Warmup and measurement run in the same worker threads. Workers pause at a phase
barrier while the main thread captures the warmup snapshot, resets worker-local
counters and latency samplers, and publishes the measurement deadline.

The scan cursor is preserved across the boundary. The random access stream may
be reseeded with the existing measurement salt, but reseeding must not reset the
scan cursor.

Acceptance:

- A unit test proves that the last warmup cursor is the first measurement
  cursor, including wraparound.
- A barrier test proves that no operation is counted in both phases and no
  operation crosses the statistics snapshot.
- Warmup and measurement retain deterministic replay for a fixed seed and
  worker count.

### T16.3 — Explicit scan-consumption modes

Add `--scan-work marker|cacheline|digest`:

- `marker` reads the verification marker only and is the lowest-overhead
  buffer-manager path.
- `cacheline` consumes a fixed value from every cache line in the 64 KiB page.
  This is the primary storage and prefetch mode.
- `digest` retains the current two-lane full-page digest and represents an
  application that performs substantial page processing.

Every orchestration script passes this option explicitly. The standalone CLI
may retain `digest` as a compatibility default, but the selected mode is stored
in CSV and JSON. Adding the field increments the stats schema version and
updates all parsers, fixtures, dashboards, and documentation.

Acceptance:

- CLI parsing, invalid-value, CSV round-trip, JSON schema, and dashboard tests
  cover all three modes.
- Each mode touches the intended bytes and preserves marker verification.
- Benchmark documentation states whether reported latency includes page
  consumption.

### T16.4 — Separate fix and operation latency

For each timed operation, record:

- `fix_latency`: request start through successful shared/exclusive guard
  acquisition, including a lower-tier fault when one occurs.
- `operation_latency`: request start through page consumption and guard release.

Keep the existing operation-latency columns for compatibility and add overall,
point, and scan fix p50/p99 columns. JSON records sample counts for every
distribution. Read and write latency distributions are added when T20.1 lands.

Acceptance:

- Unit tests prove `fix_latency <= operation_latency` for marker, cacheline, and
  digest paths.
- CSV/JSON/dashboard consumers accept the new schema and display unambiguous
  labels.
- Unsupported or empty operation classes report null/empty values rather than
  fabricated zero-latency percentiles.

### T16.5 — Explicit clean-state settling

Do not infer cleanliness from a quiet time window. A dirty resident page can
remain untouched while tier writes stay at zero.

Before timed warmup, a bounded settle stage must establish the read-only
precondition for every fraction below 1.0:

- every logical page has a lower-tier backing location;
- no resident frame is dirty;
- settle progress and timeout are reported explicitly.

**This item requires new public library surface and is not a bench-local
change.** Neither capability exists today:

```text
crates/tierbuf/src/pool.rs:1550   fn flush_resident_pages(&self)   // private
crates/tierbuf/src/pool.rs:1016   └─ sole caller is shutdown()
crates/tierbuf/src/pool.rs:968    pub fn stats(&self) -> TierStats // counters only;
                                     no dirty-frame or unbacked-page observability
```

`tierbuf` is a published v0.2 crate, so exposing either capability is an API
design and semver decision. **Resolve this before starting T16.5**, and record
the choice:

- **Option A — observability only.** Expose `dirty_frame_count()` and
  `unbacked_page_count()`; the benchmark drives a bounded preconditioning pass
  until both reach zero. Smaller public surface, no new durability semantics,
  preferred unless a checkpoint is wanted for its own sake.
- **Option B — non-destructive `checkpoint()`.** Larger surface and a new
  durability contract to specify and test, but usable by applications.

Whichever is chosen, it must not call the shutdown-only drain and accidentally
redefine warmup as a cold start without recording that choice. Fraction 1.0 may
skip lower-tier backing, but its read-only measurement must still perform zero
tier writes.

Because T16.5 carries library-API risk that T16.1 and T16.2 do not, it is
tracked and scheduled separately from them even though all three gate the same
results.

After settling, warmup runs for a minimum duration and may extend in bounded
windows until the final two windows meet documented throughput and hit-rate
stability tolerances. Reaching the maximum without convergence is a recorded
failure, not silent success.

Acceptance:

- Small deterministic integration tests cover fractions 1.0 and below 1.0,
  settle timeout, and an injected write failure.
- A non-CI 8 GiB/fraction-0.25 validation records zero measurement tier writes.
- Every read-only runner fails or prominently marks a result when measurement
  tier writes are nonzero.

### T16.6 — Explicit S3 payload configuration

`s3_cliff_demo.py` requires and forwards payload shape, compressibility, and
spread. The runner never inherits the benchmark CLI's historical 100% default.
It also records requested settings, sampled LZ4 ratio, and measured S3 stored-
byte ratio.

Acceptance:

- Command-construction tests fail when any payload field is absent.
- Dry-run output shows all payload and compression flags.
- Stats validation rejects artifacts whose recorded payload differs from the
  requested configuration.

### T16.7 — Phase-aware S3 usage and cost reporting

Retain `estimate_s3_cost` as a conservative pre-run budget guard. Add a separate
post-run report based on measured usage.

Instrumentation must distinguish logical API calls from actual HTTP attempts
and internal retries. Capture S3 snapshots after initialization, after warmup,
and after measurement so reports can show setup, warmup, measurement, and total
deltas.

Report four independent categories:

1. Request usage: logical GET/PUT/DELETE calls, HTTP attempts, retries, and
   configured-rate cost.
2. Storage usage: ending `stored_bytes`, normalized storage cost for an explicit
   retention period, and pending-delete caveats.
3. Transfer usage: uploaded and downloaded bytes by phase. Monetary transfer
   cost is shown only when region/direction pricing is explicitly configured.
4. Logical capacity: logical page bytes, never substituted for stored bytes.

Acceptance:

- Retry tests demonstrate that logical calls and HTTP attempts diverge as
  expected.
- Phase-delta tests reconcile to total counters without underflow.
- Reports say “measured usage × configured rates,” not “actual AWS bill.”
- Zero-usage, failed-request, pending-delete, and compression-off cases are
  covered.

### T16.8 — Prefetch outcome and saturation metrics

Split aggregate prefetch skips into at least:

- in-flight/queue capacity reached;
- already pending;
- already resident;
- stale, non-canonical, or otherwise ineligible handle.

Record current and high-watermark in-flight requests. T17 may claim queue
saturation only from the capacity counter and high watermark, not aggregate
`prefetch_skipped`.

Acceptance:

- Focused tests trigger every outcome independently.
- Outcome totals reconcile with submitted, completed, and skipped requests.

### T16.9 — Physical FileTier I/O accounting

FileTier compression analysis needs actual transferred bytes, not the logical
64 KiB recorded for every tier fault. Add physical read/write-byte counters
that include 4 KiB direct-I/O rounding and distinguish raw slots from envelopes.

Acceptance:

- Raw slots record 64 KiB transfers.
- Compressed direct-I/O slots record the rounded physical length.
- Buffered slots record the exact envelope length.
- T19 reports logical and physical bytes separately.

### T16 validation gate

Before T17 or T19:

```text
cargo fmt --all -- --check
cargo test --workspace --all-targets
cargo test --workspace --all-targets --no-default-features
cargo clippy --workspace --all-targets -- -D warnings
python -m unittest discover -s scripts -p 'test_*.py'
```

Run a small local benchmark smoke test for all scan-work modes, one prefetch-on
scan, one read-only settle path, and one retry-injected in-memory S3 path. The
8 GiB acceptance run is a separately retained benchmark artifact, not a normal
CI test.

## T17 — S3 run A: prefetch and degradation

### Purpose

Measure how bounded prefetch overlaps page-sized S3 GET latency as DRAM shrinks.
This is a closed-loop, read-only, sequential-scan storage benchmark, not a
comprehensive application workload or storage-cost headline.

### Fixed configuration

- `--scan-only`
- `--scan-work cacheline`
- explicit incompressible payload settings
- `--s3-compression off`
- prefetch variants: off, 4 workers, and 64 workers
- all command lines, seeds, sample counts, and T16 settle/convergence outcomes
  retained

The primary low-CPU profile may use `i4i.large` only with an explicitly reduced
dataset, currently 8 GiB or less, so fraction 1.0 and process overhead fit in
16 GiB RAM. The current 32 GiB AWS-scale profile requires `i4i.2xlarge` or a
larger-memory instance. Infrastructure checks derive required memory from the
configured dataset instead of hard-coding a contradictory instance/profile
pair.

### Matrix and repetitions

Local MinIO validation runs all six DRAM fractions for all three prefetch
variants three times. It validates orchestration and counter behavior, not AWS
latency.

The paid S3 pilot identifies the cliff. The authoritative paid run repeats,
three times in crossed order:

- fraction 1.0;
- the lowest fraction;
- the observed cliff fraction and its adjacent sampled fractions.

Do not expand the paid run to every combination unless the pilot shows that the
selected points miss the transition.

### Primary outputs

- throughput and operation count;
- fix and operation p50/p99 plus sample counts;
- DRAM hit, fault, and eviction counts;
- logical GET calls, HTTP attempts, and retries;
- prefetch submitted/completed/hit and reason-specific skips;
- in-flight high watermark and capacity saturation;
- median and min/max across repetitions.

Storage cost is not a headline. Request usage may be reported with the explicit
T16.7 pricing assumptions.

### Exit gate

T17 is complete when local tests reconcile all counters, every authoritative
point has three valid repetitions, no read-only measurement writes occur, and
no percentile is published below the sample-count threshold.

## T18 — S3 run B: measured-usage sensitivity

### Purpose

Show how payload compressibility changes stored bytes, transferred bytes, CPU,
and end-to-end latency. Until a real page corpus exists, this is a sensitivity
analysis and does not produce one representative storage-cost number.

### Fixed configuration

Use the same EC2 session as T17 to avoid a second provisioning cycle, while
recording the additional EC2 runtime and S3 charges. Fix one T17-selected DRAM
fraction, one prefetch configuration, `--scan-work cacheline`, and
`--s3-compression on`.

Payload points are explicit and reported by achieved envelope ratio, not only
the requested percentage:

1. incompressible;
2. medium-compressibility `chunked` data;
3. high-compressibility `chunked` data;
4. a real 64 KiB page corpus when available.

The exact synthetic percentages and spread are frozen in the run manifest after
a local ratio pilot. They are not chosen after seeing S3 performance.

### Repetitions

One successful run per synthetic point is sufficient for deterministic stored-
byte sensitivity. Any throughput or latency comparison requires three crossed-
order repetitions at that point.

### Outputs and exit gate

Use the T16.7 four-part report and show setup versus measurement deltas. T18 is
complete when logical calls, HTTP attempts, stored bytes, and transfer bytes
reconcile; payload settings and achieved ratios are present; and the report is
labeled as sensitivity rather than representative cost.

## T19 — FileTier compression crossover remeasurement

### Purpose

Re-evaluate the current FileTier LZ4 design after removing dirty-start,
sampling, page-consumption, and physical-byte-accounting ambiguity.

### Hosts and fixed workload

- `i4i.large`: low-CPU, 2-vCPU/1-physical-core profile.
- `i4i.2xlarge`: CPU-richer comparison profile, 8 vCPU / 4 physical cores.
- Same 8 GiB dataset and NVMe setup on both hosts.
- Primary `scan-work cacheline`; one selected `digest` sensitivity point to show
  application-processing interaction.

Worker counts must separate core count from oversubscription ratio. Running
four workers on both hosts does not: that is 4 workers per physical core on
`large` against 1 on `2xlarge`, so "more CPU" and "less per-core contention"
move together and the caveat this task exists to settle cannot be attributed.
It also contradicts T20.4, which treats 4 workers on `large` as a
scheduler-contention experiment rather than a scaling point.

Run four host/worker points:

| Point | Workers per physical core |
| --- | --- |
| `large` @ 1 worker | 1:1 |
| `large` @ 4 workers | 4:1 |
| `2xlarge` @ 4 workers | 1:1 |
| `2xlarge` @ 16 workers | 4:1 |

This yields three interpretable contrasts:

- `large`@1 vs `2xlarge`@4 — core count at matched per-core load;
- `large`@1 vs `large`@4 — oversubscription on the starved host;
- `large`@4 vs `2xlarge`@4 — the original comparison, now with the confound
  bounded by the other two.

The `2xlarge`@16 point is optional if run time is tight; the first three carry
the attribution. Every published figure states workers per physical core.

Per principle 4, the full payload sweep does not run at all four points. The
authoritative sweep runs at one designated primary point (`large`@4, matching
the historical configuration so the supersession is legible). The other points
run only the compressibility values needed to locate the crossover, which the
primary sweep identifies first. This keeps the added cost at a few extra
configurations rather than four full sweeps.

Sweep compression off/on across the existing `uniform` and `chunked` payload
points. Repeat every point near a crossover three to five times and cross the
off/on execution order. A coarse pilot may leave clearly dominated endpoints at
fewer repetitions, but no interpolated crossover is published from single
runs.

### Outputs

- throughput and fix/operation latency by operation class;
- logical and physical FileTier bytes;
- CPU utilization and host metadata;
- compression ratio distribution;
- median, range, and crossover uncertainty rather than a five-digit point
  estimate.

The rewritten report describes the old 1.1% control range as device saturation
— identical-to-the-digit tier reads at 94% of the `fio` IOPS ceiling — and not
as repeatability or cross-payload path stability. It keeps the old result as a
historical appendix and states explicitly that the old `chunked` sign was
confounded by asymmetric residual write-back, so T19 is a first measurement of
that question rather than a confirmation.

## T20 — Comprehensive load expansion

T20 uses one-factor-at-a-time experiments around this baseline:

```text
70% Zipf point reads / 30% interleaved scan reads
4 closed-loop workers
DRAM fraction 0.25
scan-work cacheline
fixed hardware and backend
```

Only the baseline and a small number of final representative profiles receive a
full DRAM-fraction sweep.

### T20.1 — Sustained writes and mixed OLTP

Add `--write-percent` as the percentage of point operations that acquire an
exclusive guard and update a page version. Preserve deterministic correctness
under concurrent readers and writers.

Report logical read/write operations, separate read/write fix and operation
latencies, dirty resident pages, dirty evictions, physical tier writes, flush
latency, and write amplification. This task is the minimum requirement before
the harness is called a mixed read/write load test.

### T20.2 — Dynamic workloads

Add isolated scenarios for hotspot shift, bounded scan bursts, dirty bursts,
and allocate/free churn. Each scenario records transition time so recovery and
policy adaptation are visible instead of averaged into one steady-state value.

### T20.3 — Access-profile separation

Run point-only uniform, point-only Zipf, scan-only sequential, and the existing
70/30 mixed profile independently. Use scan-only—not the interleaved mixed
profile—for prefetch-window tuning.

### T20.4 — Concurrency scaling

Run 1, 2, 4, 8, and 16 workers on `i4i.2xlarge` or larger. Treat 16 workers as
an oversubscription point on an 8-vCPU host. On `i4i.large`, only 1 and 2 are
primary scaling points; higher counts are explicitly scheduler-contention
experiments.

### T20.5 — Open-loop saturation

Add a rate-controlled mode only for selected profiles that need service-style
SLO claims. Report offered rate, completed rate, queue depth, dropped/rejected
work, and p99 versus target rate. Closed-loop remains the canonical embedded-
caller throughput mode.

### T20.6 — Zipf rank permutation

Add a fixed recorded permutation from Zipf rank to logical page index for point
workloads. This is last priority because it does not affect the scan-only S3
cliff and the current S3 backend performs one object GET per page without range
GET coalescing.

## Reporting template

Every authoritative benchmark report contains:

- objective and the single varied axis;
- commit, harness schema, complete command, seeds, and payload descriptor;
- instance, physical/logical CPU count, RAM, device, kernel, and toolchain;
- initialization, settle, warmup, and measurement durations and convergence;
- closed/open-loop mode, worker count, scan-work mode, and sample counts;
- per-repetition raw results and median/range summary;
- logical versus physical bytes and phase-aware tier/S3 counters;
- known caveats and a clear statement of what the run does not establish.

## Execution order

```text
T16.5 public-API decision (Option A or B)   ← resolve first, blocks nothing else
        ↓
T16.1 + T16.2 (bench-local)  ∥  T16.5 (library surface + settle stage)
        ↓
remaining T16 instrumentation and schema work
        ↓
local T16 gate and 8 GiB validation
        ↓
T17 local MinIO → T17 paid S3
        ↓
T18 in the same provisioned session
        ↓
T19 on i4i.large and i4i.2xlarge
        ↓
T20.1 writes → T20.2/T20.3 → T20.4 → T20.5 → T20.6
```

No later task weakens an earlier acceptance gate. If a gate fails, the affected
run is retained as a diagnostic artifact but is not promoted to an
authoritative benchmark result.
