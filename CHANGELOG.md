# Changelog

All notable changes to tierbuf are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

[Unreleased]: https://github.com/oct-sky-out/tierbuf/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/oct-sky-out/tierbuf/releases/tag/v0.1.0
