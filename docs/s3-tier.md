# S3 tier

`S3Tier` keeps tierbuf's synchronous, runtime-independent `TierBackend`
contract while using blocking HTTP underneath. It is intended as the
authoritative cold tier in a `[FileTier, S3Tier]` configuration. Each page has
exactly one authoritative lower-tier location; the NVMe tier is not a second
copy of an S3 object.

Enable it with the opt-in Cargo feature:

```toml
tierbuf = { version = "0.2", features = ["s3"] }
```

The implementation uses `ureq` and a small SigV4 signer rather than an async
runtime or the AWS SDK. One request blocks one caller thread. Parallelism
comes from buffer-manager prefetch workers and benchmark worker threads.

## Page envelope

DRAM frames remain uncompressed 64 KiB pages. Before an S3 PUT, the tier wraps
the page in a self-describing envelope. All integers use little-endian byte
order and the header is exactly 32 bytes.

| Offset | Size | Field | Meaning |
| ---: | ---: | --- | --- |
| 0 | 4 | `magic` | `0x5442_5031` |
| 4 | 1 | `version` | `1` |
| 5 | 1 | `codec` | `0` = none, `1` = LZ4 block |
| 6 | 2 | `flags` | must be zero |
| 8 | 4 | `uncompressed_len` | must be 65,536 |
| 12 | 4 | `payload_len` | bytes following the header |
| 16 | 4 | `payload_crc32` | IEEE CRC32 of the stored payload |
| 20 | 12 | `reserved` | must be zero |

CRC validation happens before decompression. If LZ4 output is not smaller than
the original page, the encoder stores the page verbatim and records codec
`none`; the header always describes what was actually stored. The `lz4`
feature is enabled by default. With
`--no-default-features --features s3`, set
`S3TierConfig.codec = PageCodec::None`; the default configuration intentionally
selects LZ4 and is rejected when that feature is disabled.

## Immutable object identifiers

Keys have this form:

```text
{key_prefix}p{object_id:016x}
```

`object_id` comes from one process-wide monotonically increasing `u64`, so
replacement `S3Tier` instances in the same process cannot reuse an identifier
even when they share a prefix. It is never reused after a failed PUT or
successful free. The corresponding virtual `TierOffset` is
`object_id * PAGE_SIZE`.

This is a correctness rule, not merely a naming preference. `free()` removes
the page from in-memory metadata immediately and normally sends DELETE to a
background worker. If an old identifier could be reused, this sequence would
lose data:

1. queue DELETE for the old page;
2. PUT a new page at the reused key;
3. receive the delayed DELETE and remove the new page.

A fresh key for every write makes that race impossible. S3's strong
read-after-write behavior then permits an immediate GET after a successful
PUT.

The counter is not persisted across process restarts because v0.2 has no
persistent directory. Applications must therefore give each process or
dataset generation a unique prefix. Benchmark and integration-test tooling
does this automatically; a bucket lifecycle rule should expire abandoned
prefixes.

Capacity has two meanings:

- admission uses logical pages: `live_pages * 64 KiB <= capacity_bytes`;
- `used_bytes()` and cost metrics use current envelope bytes.

Compression therefore reduces reported stored bytes without allowing more
logical pages than the configured capacity.

## Credentials and endpoints

The default credential source checks `AWS_ACCESS_KEY_ID`,
`AWS_SECRET_ACCESS_KEY`, and optional `AWS_SESSION_TOKEN`, then falls back to
EC2 IMDSv2. IMDS credentials refresh within five minutes of expiration. A
still-valid cached value remains usable if a refresh attempt fails.

With no custom endpoint, the client uses virtual-hosted AWS URLs:

```text
https://{bucket}.s3.{region}.amazonaws.com/{key}
```

A custom `http://` or `https://` endpoint forces path-style URLs for MinIO and
LocalStack:

```text
{endpoint}/{bucket}/{key}
```

GET, PUT, and DELETE retry connection failures, timeouts, HTTP 429, and
HTTP 500/502/503/504 with capped exponential full-jitter backoff. Other 4xx
responses fail immediately so permission and signature errors remain visible.

## Placement economics

Every tier exposes storage price plus read and write request charges.
`EconomicPolicy` models one lower-tier read as:

```text
read_latency_us * cpu_cost_usd_per_us
+ global_read_opportunity_cost_usd
+ tier_read_request_cost_usd
```

Dividing that value by the cost of retaining one page in DRAM for one second
produces the break-even reaccess interval. The default S3 Standard inputs are
`$0.023/GiB-month`, `$0.0000004/GET`, and `$0.000005/PUT`. As a result, warm
pages normally choose a low-latency NVMe tier, while truly cold pages can
choose S3.

The write request cost is carried through policy metadata and validated, but
v0.2 does not yet put it into the break-even equation. Doing so correctly
requires an amortization model for a one-time write versus an uncertain count
of future reads.

## Prefetch concurrency

One 64 KiB GET at 30 ms provides only about 2.1 MiB/s. Use multiple prefetch
workers to overlap independent requests:

| Workers | Approximate latency-limited ceiling | Suggested in-flight cap |
| ---: | ---: | ---: |
| 4 | 8 MiB/s | 128 |
| 32 | 67 MiB/s | 128–256 |
| 64 | 133 MiB/s | 256 |
| 128 | 267 MiB/s | 512 |
| 256 | 533 MiB/s | 1,024 |

These are planning estimates, not guaranteed throughput. Endpoint limits,
network bandwidth, CPU decompression, object-store throttling, and access
shape can all lower the result. `BufConfig` permits 1–256 workers and up to
4,096 queued-plus-running prefetch requests. The shared HTTP agent retains up
to 256 idle connections for one host, matching the maximum worker count so
completed request waves reuse sockets instead of churning through ephemeral
ports.

## Cleanup and known limits

- An eviction PUT is synchronous and can occupy an evictor thread for tens or
  hundreds of milliseconds.
- `Drop` gives the DELETE worker two seconds to drain. A process crash,
  exhausted retries, or a longer request can leave orphaned objects.
- There is no LIST-based recovery or persistent page directory.
- Page-sized objects mean high request counts; range-GET coalescing is deferred
  to a later segmented-object design.
- The HTTP pool lazily retains up to 256 idle sockets to support the maximum
  prefetch concurrency. With the currently locked ureq 3.3 release, those
  sockets may remain until the endpoint closes them or the client is dropped;
  long-lived processes should size file-descriptor limits accordingly.
- `S3TierStatsHandle` counts logical object-API calls. Retries inside
  `S3Client` are not counted again, so these are not exact billable-request
  counters when a request is retried.

Use a unique prefix per benchmark run and an S3 lifecycle rule that expires
`tierbuf-bench/` objects after one day. This bounds orphan cost without making
LIST or crash recovery part of the embedded kernel.
