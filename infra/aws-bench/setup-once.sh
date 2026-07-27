#!/usr/bin/env bash
# Create the S3, IAM, and network resources used by AWS benchmark runs.
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=bench.env
source "${SCRIPT_DIR}/bench.env"

for name in REGION BUCKET ROLE_NAME PROFILE_NAME SG_NAME; do
  if [ -z "${!name:-}" ]; then
    echo "bench.env: ${name} must be set" >&2
    exit 2
  fi
done

echo "[1/4] S3 bucket: ${BUCKET}"
if ! aws s3api head-bucket --bucket "${BUCKET}" 2>/dev/null; then
  if [ "${REGION}" = "us-east-1" ]; then
    aws s3api create-bucket --bucket "${BUCKET}" --region "${REGION}"
  else
    aws s3api create-bucket --bucket "${BUCKET}" --region "${REGION}" \
      --create-bucket-configuration "LocationConstraint=${REGION}"
  fi
fi
aws s3api put-public-access-block --bucket "${BUCKET}" \
  --public-access-block-configuration \
  "BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true"
aws s3api put-bucket-encryption --bucket "${BUCKET}" \
  --server-side-encryption-configuration \
  '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"}}]}'

echo "[2/4] IAM role: ${ROLE_NAME}"
TRUST='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}'
aws iam get-role --role-name "${ROLE_NAME}" >/dev/null 2>&1 ||
  aws iam create-role --role-name "${ROLE_NAME}" \
    --assume-role-policy-document "${TRUST}" >/dev/null
POLICY=$(printf '%s' \
  '{"Version":"2012-10-17","Statement":[' \
  "{\"Effect\":\"Allow\",\"Action\":\"s3:PutObject\",\"Resource\":\"arn:aws:s3:::${BUCKET}/results/*\"}," \
  "{\"Effect\":\"Allow\",\"Action\":\"s3:ListBucket\",\"Resource\":\"arn:aws:s3:::${BUCKET}\",\"Condition\":{\"StringLike\":{\"s3:prefix\":\"results/*\"}}}" \
  ']}')
aws iam put-role-policy --role-name "${ROLE_NAME}" \
  --policy-name s3-results --policy-document "${POLICY}"

echo "[3/4] Instance profile: ${PROFILE_NAME}"
if ! aws iam get-instance-profile --instance-profile-name "${PROFILE_NAME}" >/dev/null 2>&1; then
  aws iam create-instance-profile --instance-profile-name "${PROFILE_NAME}" >/dev/null
fi
if ! aws iam get-instance-profile --instance-profile-name "${PROFILE_NAME}" \
  --query "InstanceProfile.Roles[?RoleName=='${ROLE_NAME}'].RoleName" \
  --output text | grep -q "${ROLE_NAME}"; then
  aws iam add-role-to-instance-profile \
    --instance-profile-name "${PROFILE_NAME}" --role-name "${ROLE_NAME}"
  echo "Waiting 10 seconds for IAM propagation"
  sleep 10
fi

echo "[4/4] Security group: ${SG_NAME}"
VPC_ID=$(aws ec2 describe-vpcs --region "${REGION}" \
  --filters "Name=isDefault,Values=true" --query 'Vpcs[0].VpcId' --output text)
if [ -z "${VPC_ID}" ] || [ "${VPC_ID}" = "None" ]; then
  echo "No default VPC exists in ${REGION}" >&2
  exit 1
fi
SG_ID=$(aws ec2 describe-security-groups --region "${REGION}" \
  --filters "Name=group-name,Values=${SG_NAME}" "Name=vpc-id,Values=${VPC_ID}" \
  --query 'SecurityGroups[0].GroupId' --output text)
if [ -z "${SG_ID}" ] || [ "${SG_ID}" = "None" ]; then
  SG_ID=$(aws ec2 create-security-group --region "${REGION}" --vpc-id "${VPC_ID}" \
    --group-name "${SG_NAME}" --description "tierbuf benchmark egress only" \
    --query 'GroupId' --output text)
fi

if [ -n "${KEY_NAME:-}" ]; then
  if [ -z "${SSH_CIDR:-}" ]; then
    echo "bench.env: SSH_CIDR is required when KEY_NAME is set" >&2
    exit 2
  fi
  aws ec2 authorize-security-group-ingress --region "${REGION}" \
    --group-id "${SG_ID}" --protocol tcp --port 22 --cidr "${SSH_CIDR}" \
    2>/dev/null || true
  echo "SSH enabled from ${SSH_CIDR}"
fi

echo "Setup complete (security group ${SG_ID}). Run ./run-bench.sh."
