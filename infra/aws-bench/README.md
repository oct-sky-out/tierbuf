# AWS fire-and-forget benchmark

This directory launches a Tokyo-region Spot instance that prepares local NVMe,
runs an `fio` baseline and the tierbuf degradation benchmark, uploads artifacts
to S3, and terminates itself. A scheduled shutdown bounds the maximum runtime
even if provisioning or the benchmark hangs.

## Layout

- `bench.env.example`: versioned configuration template.
- `bench.env`: local configuration, ignored by Git.
- `setup-once.sh`: creates the result bucket, least-privilege EC2 role and
  instance profile, and an egress-only security group.
- `run-bench.sh`: launches one Spot instance and polls S3 completion markers.
- `user-data.sh.tpl`: unattended EC2 boot workflow.
- `fetch-results.sh`: downloads artifacts into `results/cloud/<run-id>/`.

## Prerequisites

1. Install AWS CLI v2 and configure credentials that can manage S3, IAM, and
   EC2 resources in `ap-northeast-1`.
2. Copy and edit the configuration:

   ```bash
   cd infra/aws-bench
   cp bench.env.example bench.env
   chmod +x setup-once.sh run-bench.sh fetch-results.sh
   ```

3. Replace `BUCKET` with a globally unique bucket name and `REPO_URL` with a
   public HTTPS clone URL. Review `BENCH_ARGS` against the selected instance
   memory before launching.

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
`fetch-results.sh`.

## Security and cost

- There are no inbound security-group rules by default. To enable temporary
  SSH debugging, set both `KEY_NAME` and a narrowly scoped `SSH_CIDR`; never
  use `0.0.0.0/0`.
- The instance uses IMDSv2 and can only list the configured bucket prefix and
  upload under `results/`.
- S3 public access is blocked and default server-side encryption is enabled.
- `MAX_MINUTES` schedules shutdown, and EC2 is configured to terminate on
  instance-initiated shutdown.
- `setup-once.sh` creates billable/persistent AWS resources. Delete the bucket,
  inline role policy, role/profile, and security group when they are no longer
  needed.

