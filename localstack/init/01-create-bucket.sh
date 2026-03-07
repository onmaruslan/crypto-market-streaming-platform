#!/usr/bin/env bash
set -e

echo "Creating S3 bucket: ${S3_BUCKET}"

awslocal s3 mb "s3://${S3_BUCKET}" 2>/dev/null || true

echo "Bucket ready: ${S3_BUCKET}"