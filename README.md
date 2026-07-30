# tierbuf

**DRAM, NVMe, and future storage tiers managed as one embeddable page space.**

**DRAM 가격 위기에 대한 아키텍처적 응답.**

tierbuf is a Rust buffer-manager kernel built around fixed 64 KiB pages,
aligned DRAM frames, and explicit lower storage tiers. It gives storage engines
a small mechanism layer they can embed instead of coupling them to an async
runtime or a complete database.

DRAM capacity is increasingly an economic constraint, not just a hardware
sizing choice. Hot data keeps the direct-access path it deserves, while colder
data can move to less expensive media without splitting the application's
logical address space.

The project treats graceful degradation as a contract. Pointer swizzling,
optimistic latching, measured access heat, placement economics, background
prefetch, and degradation-curve CI make reducing the DRAM fraction a measured
slope rather than a silent performance cliff.

## Architecture

```text
 Application / storage engine
       │ owns canonical Swip clones
       ▼
┌──────────────────────── BufferManager ────────────────────────┐
│ allocate / fix_shared / fix_exclusive                         │
│                                                              │
│  resident fast path                 fault / write-back path   │
│  Swip ── ResidentAddr ─────┐       ┌── PageDirectory(pid)     │
│                            ▼       ▼                          │
│                    ┌───────────────┐                          │
│                    │  FrameTable   │                          │
│                    │ metadata Box  │                          │
│                    │ + free queue  │                          │
│                    └───────┬───────┘                          │
│                            │ same frame index                 │
│                    ┌───────▼───────┐                          │
│                    │ AlignedPool   │  anonymous mmap          │
│                    │ 64 KiB pages  │                          │
│                    └───────┬───────┘                          │
└────────────────────────────┼──────────────────────────────────┘
                             │ TierBackend
                   ┌─────────┴─────────┐
                   ▼                   ▼
              FileTier             MockTier
          O_DIRECT/buffered     deterministic tests
```

A resident fix follows the tagged `Swip` directly and does not consult the page
directory. Only an evicted page enters the coalesced fault path and reads its
recorded lower-tier location. See [Architecture and invariants](docs/architecture.md)
for the state machine, pin/eviction ordering, tier authority, and module map.

## Current v0.1 scope

The kernel includes:

- a single anonymous `mmap` DRAM pool with 64 KiB-aligned frames;
- stable, cloneable `Swip` handles with `Hot`, `Cooling`, and `Evicted` states;
- hybrid shared/exclusive/optimistic latching and closure-scoped byte access;
- allocation, resident fixes, coalesced lower-tier faults, and a
  page directory that stays off the resident hot path;
- `FileTier` and deterministic `MockTier` implementations of `TierBackend`,
  including write-budget and latency metadata;
- lock-free frame free-list and pin-versus-eviction reservation mechanics;
- lazy 8.24 fixed-point heat tracking, economic placement decisions, metrics,
  and cost accounting;
- autonomous cooling, generation-checked eviction, and write-budget fallback;
- bounded nonblocking prefetch with failure cleanup and generation-exact
  one-shot hit tracking;
- optional Linux `io_uring` reads (enabled by default) through a dedicated
  concurrent completion thread, with portable backend fallback;
- a deterministic degradation-curve harness, checker, and scheduled CI jobs.

## Embed in 5 min

The example uses the current public synchronous API. Keep the `Swip` returned
by the allocation guard; independently reconstructing a handle for the same
page ID is deliberately rejected.

```rust
use tierbuf::PAGE_SIZE;
use tierbuf::pool::{BufConfig, BufferManager, Economics, EvictionMode};
use tierbuf::tier::mock::MockTier;
use tierbuf::tier::TierBackend;

fn main() -> tierbuf::Result<()> {
    let cold = MockTier::new((PAGE_SIZE * 128) as u64)?;
    let manager = BufferManager::new(BufConfig {
        dram_pool_bytes: PAGE_SIZE * 16,
        cooling_ratio: 0.1,
        eviction_mode: EvictionMode::Demand,
        economics: Economics::default(),
        tiers: vec![Box::new(cold) as Box<dyn TierBackend>],
    })?;

    let mut allocated = manager.allocate()?;
    let swip = allocated.swip();
    allocated.write_with(|page| {
        page[..8].copy_from_slice(b"tierbuf!");
    });
    drop(allocated);

    let shared = manager.fix_shared(&swip)?;
    shared.read_with(|page| {
        assert_eq!(&page[..8], b"tierbuf!");
    });
    drop(shared);

    println!("{}", manager.cost_report());

    Ok(())
}
```

`write_with` and `read_with` intentionally keep page references inside the
guard's latch lifetime. The returned data cannot escape validation or outlive
its pin.

After at least one configured epoch, the cost report renders actual residency
beside the all-DRAM counterfactual:

```text
tierbuf cost: actual $0.000000001, all-DRAM $0.000000004 (0.250x)
  dram: 0.000977 GiB·s, $0.000000002
  mock: 0.003906 GiB·s, $0.000000001
```

## Degradation curve

```bash
cargo run -p tierbuf-bench --release -- --quick
python3 scripts/degradation.py results/curve.csv --plot results/curve.png
python3 scripts/visualize_bench.py results/curve.csv --output results/curve.html
```

The checker rejects any adjacent DRAM-fraction pair whose throughput ratio is
greater than 3.0 or whose p99 ratio is greater than 4.0. Scheduled CI publishes
the full 4 GiB CSV and graph; the curve below is the first reference hardware
run (`20260730-164346-i4i-large`, i4i.large, 8192 MiB dataset).

`scripts/visualize_bench.py` renders a dependency-free English HTML dashboard
from one or more benchmark CSVs. Multiple runs can be compared directly:

```bash
python3 scripts/visualize_bench.py run-a.csv run-b.csv \
  --labels "quick run A" "quick run B" \
  --output results/curve.html
```

For the standard 5s, 10s, 30s, and 60s measurement cases, use the duration
sweep wrapper. It writes one CSV per duration and then renders the same
dashboard:

```bash
python3 scripts/bench_duration_sweep.py
```

By default this uses a practical local profile: 256 MiB dataset, 1s warmup per
fraction, 4 workers, and release mode. Override sizing when a heavier reference
run is needed:

```bash
python3 scripts/bench_duration_sweep.py --dataset-mib 4096 --warmup-secs 10
```

Scheduled and manual CI runs publish the same sweep as the
`degradation-duration-sweep` artifact, including:

- `curve-5s.csv`
- `curve-10s.csv`
- `curve-30s.csv`
- `curve-60s.csv`
- `dashboard.html`

Point operations validate their record marker. Scan operations consume the
complete 64 KiB page through a two-lane digest, so the 70/30 workload measures
real page processing instead of comparing lower-tier I/O with a one-byte
synthetic hot path. `--file-tier PATH` selects local file storage;
`--scan-only --prefetch-scan --mock-latency-us 200` provides the prefetch
on/off comparison profile.

Each benchmark CSV also separates point and scan traffic into operation count,
throughput, p50, p99, and DRAM hit-rate columns. `dram_hit_rate` is the
fraction of completed demand fixes served by an already-resident frame;
`lower_tier_hit_rate` is the fraction restored from a configured lower tier.
The two rates sum to 1.0 and exclude speculative prefetch I/O.
`point_dram_hit_rate` and `scan_dram_hit_rate` attribute those hits to the
operation that completed the fix. Both the PNG plotter and standalone HTML
dashboard automatically add operation-type and tier-hit panels when these
extended columns are present, while continuing to accept older five-column
CSV files.

The background cooler defaults to `EvictionMode::Demand`, which maintains the
low watermark only during epochs containing demand fault-ins.
`EvictionMode::Watermark` preserves the original always-on low-watermark
behavior for A/B comparisons. The benchmark accepts
`--eviction-mode demand|watermark`, `--fraction`, and `--stats-output`; the
JSON stats artifact records cumulative and measurement-window fixes,
evictions, second chances, and per-tier I/O.

![tierbuf degradation curve](docs/degradation-curve.svg)

Throughput drops fastest on the very first fault-bearing step (1.0 → 0.8 costs
about half of full-DRAM throughput) and flattens out well before fraction 0.1;
p99 latency, by contrast, stays essentially flat from 0.8 down to 0.1 once any
lower-tier traffic exists at all, because the NVMe device rather than the
kernel bounds tail latency across that whole range. The third panel checks
that shape against the shape this repository assumed before any reference
hardware run existed: the pre-run expectation was a steady, near-linear
decline to about 18% of peak by fraction 0.1, while the measured curve
front-loads most of its loss into the 1.0 → 0.8 step (down to 51%, versus an
assumed 89%) and only converges back toward the pre-run assumption by 0.1.

The ignored release stress can be exercised at its full five-minute duration:

```bash
sh scripts/stress.sh 300
```

## Safety corrections

The implementation tightens several contracts from the original design plan:

- a frame retains an `Arc`-backed owner `Swip` clone instead of a movable raw
  backpointer;
- one atomic pin-control word makes pin acquisition mutually exclusive with
  the exact `0 → EVICTING` reservation;
- per-page fault generations coalesce one backing read and replay its same
  outcome to every registered waiter;
- safe page APIs use latch-scoped closures rather than exposing optimistic
  references to concurrently mutable bytes;
- direct and asynchronous I/O capabilities are treated as platform-specific;
  native requests own aligned buffers until a dedicated ring thread observes
  their completions, with explicit portable fallbacks.

The rationale and exact invariants are recorded in
[v0.1 safety corrections](docs/design-corrections.md).

## Non-goals

v0.1 is an ephemeral cache/buffer-manager layer, not a database. It does not
provide crash recovery, WAL, transactions, MVCC, or index structures. The
latest lower-tier location is authoritative after write-back, but the
directory itself is not persisted across process restarts.

Variable-size pages, compression, object storage, JVM bindings, and distributed
cooperative caching are intentionally outside v0.1.

## Roadmap

- **v0.1 (current):** fixed-page tiering kernel described above; scheduled CI
  publishes the first reference degradation curve.
- **v0.2:** S3/object-storage tiers, variable-size pages, and LZ4-compressed
  resident pages.
- **v0.3:** Project Panama bindings for direct JVM/Kotlin/Spark embedding.
- **v0.4:** distributed cooperative caching with consistent-hash placement.
