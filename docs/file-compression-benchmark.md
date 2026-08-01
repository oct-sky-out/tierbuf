# FileTier compression: measured crossover

Fixed 64 KiB slots mean LZ4 never adds capacity to a file tier, so compression
trades CPU time and the `io_uring` read path for fewer transferred bytes. A
compression-capable tier withholds its raw descriptor, because `io_uring` would
otherwise hand callers slot bytes that were never decoded. Whether that trade
pays is a measurement, not a derivation, so this run measures it.

## Run

| | |
| --- | --- |
| Run ID | `20260801-163306-i4i-large` |
| Instance | `i4i.large` — 2 vCPU / **1 physical core**, 16 GiB RAM, 468 GB NVMe |
| Zone | `ap-northeast-1d` |
| Kernel | `6.17.0-1019-aws` |
| Toolchain | `rustc 1.97.1` |
| Commit | `260664c` |
| Dataset | 8192 MiB, DRAM fraction 0.25, 4 worker threads |
| Sweep | 2 shapes x 6 compressibility targets x {off, on} = 24 measurements |
| Per-page spread | ±20% |

Reproduce with `BENCH_FILE_COMPRESSION_DEMO=1` in `infra/aws-bench/bench.env`,
or locally via `scripts/file_compression_demo.py`.

### Device ceiling

`fio` randread, 64 KiB blocks, iodepth 16, `io_uring`, `direct=1`:

```
IOPS=5684  BW=355MiB/s  clat p99=3032us
```

## Results

`ratio` is the mean LZ4 envelope size relative to a 64 KiB page, sampled over 64
generated pages; `range` is its min–max across those pages. `on MiB/s` is the
tier bandwidth actually consumed with compression enabled, after the 4 KiB
direct-I/O rounding.

### `uniform` — one zero-filled prefix per page

| pct | ratio | range | off ops/s | on ops/s | speedup | off p99 | on p99 | on MiB/s | % of fio |
|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|
| 0 | 0.912 | 0.805–1.000 | 13,902 | 11,895 | 0.856 | 849 | 2,545 | 252.8 | 71% |
| 25 | 0.761 | 0.555–0.955 | 13,895 | 12,416 | 0.894 | 848 | 2,491 | 224.0 | 63% |
| 50 | 0.511 | 0.305–0.705 | 13,921 | 12,751 | 0.916 | 853 | 2,175 | 165.2 | 47% |
| 75 | 0.261 | 0.055–0.455 | 13,938 | 14,585 | 1.046 | 850 | 1,990 | 107.8 | 30% |
| 90 | 0.144 | 0.005–0.305 | 13,917 | 16,471 | 1.184 | 850 | 1,805 | 70.4 | 20% |
| 100 | 0.112 | 0.005–0.205 | 13,892 | 16,931 | 1.219 | 850 | 1,820 | 48.6 | 14% |

**Crossover: compression breaks even at about 66.1% payload compressibility.**

### `chunked` — variable-length runs, per-run tokens

| pct | ratio | range | off ops/s | on ops/s | speedup | off p99 | on p99 | on MiB/s | % of fio |
|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|
| 0 | 0.882 | 0.733–1.000 | 13,892 | 11,687 | 0.841 | 937 | 2,499 | 242.3 | 68% |
| 25 | 0.747 | 0.526–0.956 | 13,897 | 12,068 | 0.868 | 1,061 | 2,356 | 203.4 | 57% |
| 50 | 0.521 | 0.311–0.725 | 13,789 | 12,600 | 0.914 | 851 | 2,398 | 155.5 | 44% |
| 75 | 0.293 | 0.065–0.507 | 13,912 | 12,530 | 0.901 | 851 | 2,340 | 96.0 | 27% |
| 90 | 0.174 | 0.022–0.346 | 13,817 | 13,013 | 0.942 | 852 | 2,271 | 59.8 | 17% |
| 100 | 0.151 | 0.010–0.264 | 13,848 | 13,308 | 0.961 | 848 | 2,200 | 60.9 | 17% |

**Crossover: none. Compression lost at every sampled compressibility.**

## Analysis

### The control held

Compression-off throughput stayed within **1.1%** across all twelve runs
(13,789–13,938 ops/s) while payload compressibility swept 0→100%. Fixed 64 KiB
slots make page content irrelevant to the raw I/O path, and the measurement
shows exactly that. The sweep isolated the intended variable.

### Compression-off saturates the device; compression-on does not

Compression off drives **334 MiB/s** of tier traffic — **94% of the fio
ceiling**. It is bandwidth-bound.

Compression on is not. At `uniform` ratio 0.112 it consumes only 48.6 MiB/s,
leaving 86% of the device idle, and returns just 1.22x throughput. Had bandwidth
remained the binding constraint, freeing that much of it should have multiplied
throughput several-fold. It did not.

Enabling compression relocates the bottleneck from the device to the synchronous
read path and LZ4 decode. **The saved bytes are largely never converted into
throughput.**

### Compression ratio does not predict the outcome

| shape | ratio | bytes saved | speedup |
| --- | ---: | ---: | ---: |
| `uniform` | 0.112 | 89% | **1.219** |
| `chunked` | 0.151 | 85% | **0.961** |

Cutting 85% of transferred bytes still loses. The *structure* that produces a
ratio matters as much as the ratio:

- `uniform` is one enormous match, so LZ4 decode is close to a `memcpy`.
- `chunked` is hundreds of short matches and literal spans, which costs
  materially more CPU to decode.
- After 4 KiB rounding the stored sizes diverge further — roughly 8 KiB versus
  12 KiB per page.

A design note that reads "compression pays when data compresses well" is
therefore wrong as stated. It pays when data compresses well **and** decodes
cheaply.

### Tail latency regresses everywhere, including the wins

```
compression off : p99   848–1,061 us   (all 12 runs)
compression on  : p99 1,805–2,545 us   (all 12 runs)
```

Replacing batched `io_uring` submission with blocking `pread` costs 2–3x at p99
in **every** configuration — including `uniform` at 100%, where throughput wins
1.22x while p99 still degrades 2.1x (850 → 1,820 us). Even the winning case is a
qualified win, and for latency-sensitive callers it is not a win at all.

## What this does and does not establish

Established on this hardware: for record-shaped data with mixed field content,
FileTier compression has no operating point worth enabling. Throughput loses
4–16% and p99 loses ~2.6x. Uniformly compressible payloads — logs, sparse
arrays, heavily padded records — need roughly 66% compressibility before
throughput turns positive, and pay 2x tail latency even then.

Two caveats bound the generalization:

1. **The instance is CPU-starved.** `i4i.large` has one physical core, and the
   sweep ran four worker threads on it. Compression spends CPU to save I/O, so
   this is close to the worst case for compression. A CPU-rich instance
   (`i4i.2xlarge`, 8 vCPU) would plausibly lower the crossover and could let
   `chunked` win. This run does not settle that.
2. **`--payload-spread 20` smeared both endpoints.** The 0% target measured
   ratio 0.91 rather than ~1.0, and the 100% target measured 0.112 rather than
   ~0.005. Genuinely incompressible data would be worse for compression, and a
   pure all-zero payload better. **The 66.1% figure is specific to spread 20.**

The result supports the v0.2 plan's decision to deprioritize compressed FileTier
slots (T15.1): on fixed-size slots the feature cannot add capacity, and the
throughput case for it is narrow and hardware-dependent.

## Related

- [Architecture and invariants](architecture.md) — slot representation, why a
  compressing tier withholds its raw descriptor
- `scripts/file_compression_demo.py` — the sweep runner
- `infra/aws-bench/README.md` — running it on EC2
