#!/usr/bin/env bash
# EC2 boot workflow: prepare NVMe, benchmark, upload artifacts, and terminate.
set -uo pipefail
exec > /var/log/tierbuf-bench.log 2>&1

decode() {
  printf '%s' "$1" | base64 --decode
}

RUN_ID=$(decode "__RUN_ID_B64__")
BUCKET=$(decode "__BUCKET_B64__")
BENCH_S3_BUCKET=$(decode "__BENCH_S3_BUCKET_B64__")
BENCH_S3_REGION=$(decode "__BENCH_S3_REGION_B64__")
REPO_URL=$(decode "__REPO_URL_B64__")
REPO_BRANCH=$(decode "__REPO_BRANCH_B64__")
BENCH_ARGS=$(decode "__BENCH_ARGS_B64__")
MAX_MINUTES="__MAX_MINUTES__"
BENCH_SUCCEEDED=0

# EC2 shutdown is configured to terminate the instance.
shutdown -h "+${MAX_MINUTES}"

upload_and_terminate() {
  local status=$?
  trap - EXIT

  if [ -f /tmp/fio-baseline.txt ]; then
    aws s3 cp /tmp/fio-baseline.txt \
      "s3://${BUCKET}/results/${RUN_ID}/fio-baseline.txt" || true
  fi
  if [ -d /tmp/results ]; then
    aws s3 cp --recursive /tmp/results \
      "s3://${BUCKET}/results/${RUN_ID}/" || true
  fi
  aws s3 cp /var/log/tierbuf-bench.log \
    "s3://${BUCKET}/results/${RUN_ID}/bench.log" || true

  if [ "${status}" -eq 0 ] && [ "${BENCH_SUCCEEDED}" -eq 1 ]; then
    printf 'ok\n' >/tmp/marker
    aws s3 cp /tmp/marker "s3://${BUCKET}/results/${RUN_ID}/_DONE" || true
  else
    printf 'exit_status=%s\n' "${status}" >/tmp/marker
    aws s3 cp /tmp/marker "s3://${BUCKET}/results/${RUN_ID}/_FAILED" || true
  fi
  shutdown -h now
}
trap upload_and_terminate EXIT
set -e

export DEBIAN_FRONTEND=noninteractive
apt-get update -y
apt-get install -y build-essential pkg-config curl unzip git xfsprogs fio python3

curl -fsSL https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip \
  -o /tmp/awscliv2.zip
unzip -q /tmp/awscliv2.zip -d /tmp
/tmp/aws/install

DEV=$(lsblk -dno NAME,MODEL | awk '/Instance Storage/{print "/dev/" $1; exit}')
if [ -z "${DEV}" ]; then
  echo "No instance-store NVMe device found; check INSTANCE_TYPE" >&2
  exit 1
fi
mkfs.xfs -f "${DEV}"
mkdir -p /mnt/nvme
mount "${DEV}" /mnt/nvme

fio --name=baseline --filename=/mnt/nvme/fio.test --rw=randread --bs=64k \
  --size=8G --iodepth=16 --ioengine=io_uring --direct=1 --runtime=30 \
  --time_based --lat_percentiles=1 --output=/tmp/fio-baseline.txt
rm -f /mnt/nvme/fio.test

curl -fsSL https://sh.rustup.rs | sh -s -- -y --profile minimal \
  --default-toolchain 1.97.1
# shellcheck source=/dev/null
source /root/.cargo/env

git clone --depth 1 --branch "${REPO_BRANCH}" "${REPO_URL}" /mnt/nvme/tierbuf
cd /mnt/nvme/tierbuf
cargo build --release -p tierbuf-bench

mkdir -p /tmp/results /mnt/nvme/tier
# BENCH_ARGS is intentionally split into CLI arguments from the trusted local
# configuration file.
# shellcheck disable=SC2086
./target/release/tierbuf-bench ${BENCH_ARGS} \
  --file-tier /mnt/nvme/tier/tierbuf.bin \
  --output /tmp/results/curve.csv

if [ -n "${BENCH_S3_BUCKET}" ]; then
  python3 scripts/s3_cliff_demo.py --yes \
    --bucket "${BENCH_S3_BUCKET}" \
    --region "${BENCH_S3_REGION}" \
    --output-dir /tmp/results/s3-demo \
    --dashboard /tmp/results/s3-demo/dashboard.html
fi

TOKEN=$(curl -fsS -X PUT \
  -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' \
  http://169.254.169.254/latest/api/token)
metadata() {
  curl -fsS -H "X-aws-ec2-metadata-token: ${TOKEN}" \
    "http://169.254.169.254/latest/meta-data/$1"
}
{
  echo "run_id=${RUN_ID}"
  echo "instance_type=$(metadata instance-type)"
  echo "az=$(metadata placement/availability-zone)"
  echo "commit=$(git rev-parse HEAD)"
  echo "kernel=$(uname -r)"
  echo "rustc=$(rustc --version)"
  echo "bench_args=${BENCH_ARGS}"
  echo "bench_s3_bucket=${BENCH_S3_BUCKET}"
  echo "bench_s3_region=${BENCH_S3_REGION}"
} > /tmp/results/meta.txt

BENCH_SUCCEEDED=1
exit 0
