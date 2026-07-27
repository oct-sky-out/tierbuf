#!/bin/sh
set -eu

total_seconds="${1:-300}"
case "$total_seconds" in
  ''|*[!0-9]*) echo "usage: $0 [positive-seconds]" >&2; exit 2 ;;
esac
if [ "$total_seconds" -le 0 ]; then
  echo "duration must be greater than zero" >&2
  exit 2
fi

seconds_per_run=$((total_seconds / 6))
if [ "$seconds_per_run" -lt 1 ]; then
  seconds_per_run=1
fi

for frames in 64 52 39 26 13 7; do
  echo "stress: 128 logical pages, $frames DRAM frames, ${seconds_per_run}s"
  TIERBUF_STRESS_SECONDS="$seconds_per_run" \
  TIERBUF_STRESS_FRAMES="$frames" \
    cargo test -p tierbuf --test stress \
      -- --ignored --exact five_minute_randomized_pressure --nocapture
done
