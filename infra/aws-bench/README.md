# AWS fire-and-forget benchmark

This directory launches a Tokyo-region Spot instance that prepares local NVMe,
runs an `fio` baseline and the tierbuf degradation benchmark, uploads artifacts
to S3, and terminates itself. It can also run the 32 GiB S3 cliff demo against a
separate benchmark-data bucket. A scheduled shutdown bounds the maximum runtime
even if provisioning or the benchmark hangs.

## Layout

- `bench.env.example`: versioned configuration template.
- `bench.env`: local configuration, ignored by Git.
- `setup-once.sh`: creates the result bucket, optional benchmark-data bucket,
  least-privilege EC2 role and instance profile, and an egress-only security
  group.
- `run-bench.sh`: launches one Spot instance and polls S3 completion markers.
- `user-data.sh.tpl`: unattended EC2 boot workflow.
- `fetch-results.sh`: downloads artifacts into `results/cloud/<run-id>/` and
  copies S3 demo artifacts to `results/s3-demo/cloud/<run-id>/`.

## Prerequisites

1. Install AWS CLI v2 and configure credentials that can manage S3, IAM, and
   EC2 resources in `ap-northeast-1`.
2. Copy and edit the configuration:

   ```bash
   cd infra/aws-bench
   cp bench.env.example bench.env
   chmod +x setup-once.sh run-bench.sh fetch-results.sh
   ```

3. Replace `BUCKET` with a globally unique result bucket name and `REPO_URL`
   with a public HTTPS clone URL. Review `BENCH_ARGS` against the selected
   instance memory before launching.
4. To enable the S3 cliff demo, set `BENCH_S3_BUCKET` to a second, globally
   unique bucket name and set `BENCH_S3_REGION`. `BUCKET` remains dedicated to
   uploaded results; `BENCH_S3_BUCKET` holds page objects and must be different.
   Set `INSTANCE_TYPE=i4i.2xlarge` (64 GiB) or a larger-memory instance and
   `MAX_MINUTES` to at least 180. The launcher checks both before spending
   money. Leaving `BENCH_S3_BUCKET` empty runs only the existing NVMe benchmark.

The harness currently has fixed DRAM fractions of 1.0, 0.8, 0.6, 0.4, 0.2,
and 0.1. Its supported sizing flags use MiB, so the supplied default is
`--dataset-mib 8192`; the older `--dataset-gib`, `--durations`, and
`--fractions` flags are not supported.

## Run

```bash
cd infra/aws-bench
./setup-once.sh
./run-bench.sh
./fetch-results.sh
```

Interrupting `run-bench.sh` only stops local polling. The instance continues,
uploads `curve.csv`, `fio-baseline.txt`, `meta.txt`, and `bench.log`, writes
either `_DONE` or `_FAILED`, and then terminates. Resume result retrieval with
`fetch-results.sh`. When enabled, the S3 demo runs after the NVMe curve with
`scripts/s3_cliff_demo.py --yes`; its CSV, stats JSON, and dashboard are
uploaded below `results/<run-id>/s3-demo/`.

## Security and cost

- There are no inbound security-group rules by default. To enable temporary
  SSH debugging, set both `KEY_NAME` and a narrowly scoped `SSH_CIDR`; never
  use `0.0.0.0/0`.
- The instance uses IMDSv2. It can only list and upload below `results/` in the
  result bucket. When the S3 demo is enabled, it can list `tierbuf-bench/` and
  get, put, or delete objects only below that prefix in the benchmark-data
  bucket.
- S3 public access is blocked and default server-side encryption is enabled.
- `setup-once.sh` expires objects below `tierbuf-bench/` in the benchmark-data
  bucket after one day. This limits orphan cost but is not a substitute for
  deleting the bucket at teardown.
- The full six-fraction 32 GiB sweep creates isolated datasets. Its conservative
  preflight estimate is roughly 3.15 million PUTs ($15.73), 2.1–10.6 million
  GETs ($0.85–$4.23), and up to 192 GiB of one-day uncompressed storage, plus
  the selected EC2 instance and retries. The runner prints the current estimate
  before execution.
- `MAX_MINUTES` schedules shutdown, and EC2 is configured to terminate on
  instance-initiated shutdown.
- `setup-once.sh` creates billable/persistent AWS resources.

## Teardown

Download any artifacts you want to keep before teardown. Then empty and delete
the result bucket and, when configured, the separate benchmark-data bucket:

```bash
cd infra/aws-bench
source ./bench.env

aws s3 rm "s3://${BUCKET}" --recursive
aws s3api delete-bucket --bucket "${BUCKET}" --region "${REGION}"

if [ -n "${BENCH_S3_BUCKET}" ]; then
  aws s3 rm "s3://${BENCH_S3_BUCKET}" --recursive
  aws s3api delete-bucket \
    --bucket "${BENCH_S3_BUCKET}" --region "${BENCH_S3_REGION}"
fi
```

The buckets must be empty before `delete-bucket` succeeds. Remove the two
inline role policies (`s3-results` and, when enabled, `s3-benchmark-data`), then
remove the role from the instance profile before deleting the profile, role,
and security group.
