# Repository Guidelines

## Project Structure & Module Organization

This is a Rust 2024 workspace (minimum Rust 1.88) with two crates:

- `crates/tierbuf/`: the embeddable buffer-manager library. Core modules live in `src/`; integration and property tests are in `tests/`; Criterion benchmarks are in `benches/`.
- `crates/tierbuf-bench/`: the degradation-curve command-line harness.
- `scripts/`: Python benchmark analysis/visualization tools, their `test_*.py` unit tests, CSV fixtures, and the long-running stress wrapper.
- `docs/`: architecture, invariants, and design-correction notes.

Keep mechanism changes aligned with the module responsibilities documented in `docs/architecture.md`. Generated benchmark outputs belong under `results/` and should not be treated as source.

## Build, Test, and Development Commands

- `cargo build --workspace`: compile both workspace crates.
- `cargo test --workspace --all-targets`: run Rust unit, integration, property, and benchmark-target tests.
- `cargo test --workspace --all-targets --no-default-features`: verify the portable path without Linux `io_uring`.
- `cargo fmt --all -- --check`: check Rust formatting.
- `cargo clippy --workspace --all-targets -- -D warnings`: enforce warning-free Rust.
- `python -m unittest scripts/test_degradation.py scripts/test_visualize_bench.py scripts/test_bench_duration_sweep.py`: run Python tooling tests.
- `cargo run -p tierbuf-bench --release -- --quick`: produce a quick local degradation curve.
- `sh scripts/stress.sh 300`: run the ignored randomized pressure test across DRAM sizes.

## Coding Style & Naming Conventions

Use standard `rustfmt` output and four-space indentation in Python. Follow Rust conventions: `snake_case` for modules/functions, `CamelCase` for types and traits, and `SCREAMING_SNAKE_CASE` for constants. Keep unsafe/platform-specific operations isolated under `sys` or `uring`, document safety invariants, and preserve closure-scoped page access. Python tools should remain standard-library-only unless the feature explicitly requires an optional dependency.

## Testing Guidelines

Place focused unit tests beside Rust implementation code and cross-module behavior in `crates/tierbuf/tests/`. Name regression tests after the contract they protect. Use `proptest` for state-machine invariants and `unittest` plus `scripts/fixtures/` for Python edge cases. No numeric coverage threshold is defined; new behavior must include success, failure, and relevant feature-disabled cases.

## Commit & Pull Request Guidelines

The current history uses concise, imperative, sentence-case commit subjects (for example, `Implement tierbuf L0 kernel and benchmark dashboard`). Keep commits scoped and explain invariant or performance implications in the body. Pull requests should summarize behavior, list validation commands, link issues, and include CSV/HTML results for benchmark changes. Call out safety, concurrency, platform, or public-API effects explicitly; add screenshots only for rendered dashboard changes.
