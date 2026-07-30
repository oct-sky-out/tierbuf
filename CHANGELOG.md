# Changelog

All notable changes to tierbuf are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0] - 2026-07-31

### Added

- Opt-in `s3` backend with blocking S3-compatible HTTP, SigV4 signing,
  environment/static/IMDSv2 credentials, retries, immutable object IDs, and
  background deletion.
- Self-describing page envelopes with CRC32 and default-on optional LZ4
  compression.
- Optional LZ4 envelope storage inside fixed-size `FileTier` slots, including
  4 KiB-rounded direct I/O and raw fallback for envelopes that do not fit.
- Per-tier read/write request prices and read-request-aware economic
  placement.
- Configurable prefetch worker and in-flight limits for high-latency object
  storage.
- S3 benchmark assembly, request/compression statistics, MinIO integration
  tests, cliff-demo orchestration, dashboard panels, and AWS benchmark
  lifecycle support.

### Changed

- **Breaking:** `BufConfig` gained `prefetch_workers` and
  `max_prefetch_in_flight`; use `..BufConfig::default()` when constructing it.
- **Breaking:** `FileTierConfig` gained `request_costs`, defaulting to zero in
  `FileTierConfig::new`.
- **Breaking:** `FileTierConfig` gained `codec`, defaulting to
  `PageCodec::None`; compressed file tiers use synchronous reads so envelope
  decoding cannot be bypassed by `io_uring`.
- **Breaking:** `TierInfo::new` now accepts read and write request costs.
- Variable-size pages moved to the v0.3 roadmap; v0.2 keeps fixed 64 KiB DRAM
  frames and compresses only lower-tier envelopes.

### Fixed

- S3 object IDs are never reused, eliminating delayed-DELETE versus reused-key
  data loss.

## [0.1.0] - 2026-07-31

### Added

- Initial Rust workspace and 64 KiB aligned DRAM frame pool.
- Stable tagged page handles, hybrid latches, and race-free pin/eviction
  reservation.
- Runtime-independent tier backend SPI with deterministic mock and file-backed
  implementations.
- Autonomous cooling and generation-checked eviction with economic placement
  and write-budget fallback.
- Bounded background prefetch, a dedicated concurrent Linux `io_uring`
  completion engine, exact operational metrics, and cumulative residency-cost
  reporting.
- Degradation-curve benchmark/checker, release CI matrix, and a 512-case
  model-based invariant suite.

### Changed

- Tightened the original design around canonical page-handle lifetime,
  same-outcome fault generations, backing-location retention, free-frame
  checkout reservation, exact prefetch markers, and Rust-safe optimistic byte
  access. See
  `docs/design-corrections.md`.

[Unreleased]: https://github.com/oct-sky-out/tierbuf/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/oct-sky-out/tierbuf/releases/tag/v0.2.0
[0.1.0]: https://github.com/oct-sky-out/tierbuf/releases/tag/v0.1.0
