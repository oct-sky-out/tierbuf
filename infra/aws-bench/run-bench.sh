#!/usr/bin/env bash
# Launch a self-contained Spot benchmark and optionally poll for completion.
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=bench.env
source "${SCRIPT_DIR}/bench.env"

require_value() {
  local name=$1
  if [ -z "${!name:-}" ]; then
    echo "bench.env: ${name} must be set" >&2
    exit 2
  fi
}

for name in REGION INSTANCE_TYPE BUCKET REPO_URL REPO_BRANCH BENCH_ARGS \
  MAX_MINUTES PROFILE_NAME SG_NAME; do
  require_value "${name}"
done
if [[ "${BUCKET}" == *replace-with* ]] || [[ "${REPO_URL}" == *YOUR_GITHUB_ID* ]]; then
  echo "bench.env still contains placeholder values" >&2
  exit 2
fi
if ! [[ "${MAX_MINUTES}" =~ ^[1-9][0-9]*$ ]]; then
  echo "bench.env: MAX_MINUTES must be a positive integer" >&2
  exit 2
fi

RUN_ID="$(date -u +%Y%m%d-%H%M%S)-${INSTANCE_TYPE//./-}"
echo "RUN_ID=${RUN_ID}"

AMI=$(aws ssm get-parameter --region "${REGION}" \
  --name /aws/service/canonical/ubuntu/server/24.04/stable/current/amd64/hvm/ebs-gp3/ami-id \
  --query 'Parameter.Value' --output text)
echo "AMI=${AMI}"

SG_ID=$(aws ec2 describe-security-groups --region "${REGION}" \
  --filters "Name=group-name,Values=${SG_NAME}" \
  --query 'SecurityGroups[0].GroupId' --output text)
if [ -z "${SG_ID}" ] || [ "${SG_ID}" = "None" ]; then
  echo "Security group ${SG_NAME} was not found; run ./setup-once.sh first" >&2
  exit 2
fi

encode() {
  printf '%s' "$1" | base64 | tr -d '\n'
}

UD_FILE=$(mktemp "${TMPDIR:-/tmp}/tierbuf-user-data.XXXXXX")
cleanup() {
  rm -f "${UD_FILE}"
}
trap cleanup EXIT

sed -e "s|__RUN_ID_B64__|$(encode "${RUN_ID}")|g" \
  -e "s|__BUCKET_B64__|$(encode "${BUCKET}")|g" \
  -e "s|__REPO_URL_B64__|$(encode "${REPO_URL}")|g" \
  -e "s|__REPO_BRANCH_B64__|$(encode "${REPO_BRANCH}")|g" \
  -e "s|__BENCH_ARGS_B64__|$(encode "${BENCH_ARGS}")|g" \
  -e "s|__MAX_MINUTES__|${MAX_MINUTES}|g" \
  "${SCRIPT_DIR}/user-data.sh.tpl" > "${UD_FILE}"

KEY_OPT=()
if [ -n "${KEY_NAME:-}" ]; then
  KEY_OPT=(--key-name "${KEY_NAME}")
fi

INSTANCE_ID=$(aws ec2 run-instances --region "${REGION}" \
  --image-id "${AMI}" --instance-type "${INSTANCE_TYPE}" \
  --iam-instance-profile "Name=${PROFILE_NAME}" \
  --security-group-ids "${SG_ID}" \
  --metadata-options "HttpTokens=required,HttpEndpoint=enabled" \
  --instance-market-options 'MarketType=spot,SpotOptions={SpotInstanceType=one-time,InstanceInterruptionBehavior=terminate}' \
  --instance-initiated-shutdown-behavior terminate \
  --user-data "file://${UD_FILE}" \
  --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=tierbuf-bench-${RUN_ID}},{Key=project,Value=tierbuf}]" \
  "${KEY_OPT[@]}" \
  --query 'Instances[0].InstanceId' --output text)

echo "Launched ${INSTANCE_ID} (Spot, terminate on shutdown)."
S3_PREFIX="s3://${BUCKET}/results/${RUN_ID}"
echo "Polling ${S3_PREFIX} every 30 seconds for up to ${MAX_MINUTES} minutes."

for ((i = 1; i <= MAX_MINUTES * 2; i++)); do
  sleep 30
  if aws s3 ls "${S3_PREFIX}/_DONE" >/dev/null 2>&1; then
    echo "Benchmark complete: ${S3_PREFIX}/"
    exit 0
  fi
  if aws s3 ls "${S3_PREFIX}/_FAILED" >/dev/null 2>&1; then
    echo "Benchmark failed. Inspect: aws s3 cp ${S3_PREFIX}/bench.log -" >&2
    exit 1
  fi
  echo "Waiting... $((i / 2)) minute(s) elapsed"
done

echo "Polling timed out. Inspect ${S3_PREFIX}/bench.log" >&2
exit 1

