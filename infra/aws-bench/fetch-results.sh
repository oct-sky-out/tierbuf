#!/usr/bin/env bash
# Synchronize benchmark artifacts from S3 into results/cloud.
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=bench.env
source "${SCRIPT_DIR}/bench.env"

RESULTS_DIR="${SCRIPT_DIR}/../../results/cloud"
mkdir -p "${RESULTS_DIR}"
aws s3 sync "s3://${BUCKET}/results/" "${RESULTS_DIR}/"

echo "Synchronized results to ${RESULTS_DIR}"
find "${RESULTS_DIR}" -mindepth 1 -maxdepth 1 -type d -print | sort -r | head -5

