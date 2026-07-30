#!/usr/bin/env bash
# Synchronize benchmark artifacts from S3 into results/cloud.
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=bench.env
source "${SCRIPT_DIR}/bench.env"

RESULTS_DIR="${SCRIPT_DIR}/../../results/cloud"
S3_DEMO_RESULTS_DIR="${SCRIPT_DIR}/../../results/s3-demo/cloud"
mkdir -p "${RESULTS_DIR}"
aws s3 sync "s3://${BUCKET}/results/" "${RESULTS_DIR}/"

echo "Synchronized results to ${RESULTS_DIR}"
find "${RESULTS_DIR}" -mindepth 1 -maxdepth 1 -type d -print | sort -r | head -5

# Preserve run IDs while also surfacing S3 demo dashboards under the local
# results/s3-demo convention used by scripts/s3_cliff_demo.py.
DEMO_COUNT=0
for run_dir in "${RESULTS_DIR}"/*; do
  if [ ! -d "${run_dir}/s3-demo" ]; then
    continue
  fi
  run_id=$(basename "${run_dir}")
  demo_run_dir="${S3_DEMO_RESULTS_DIR}/${run_id}"
  mkdir -p "${demo_run_dir}"
  cp -R "${run_dir}/s3-demo/." "${demo_run_dir}/"
  DEMO_COUNT=$((DEMO_COUNT + 1))
done
if [ "${DEMO_COUNT}" -gt 0 ]; then
  echo "Copied ${DEMO_COUNT} S3 demo run(s) to ${S3_DEMO_RESULTS_DIR}"
fi
