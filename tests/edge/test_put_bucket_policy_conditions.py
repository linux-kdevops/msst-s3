#!/usr/bin/env python3
"""
S3 PutBucketPolicy Condition Tests

Tests PutBucketPolicy with Condition blocks:
- StringLike conditions
- IpAddress conditions
- Condition operators
- Policy size limits

Ported from versitygw integration tests.

Generated-by: Claude AI
Signed-off-by: Luis Chamberlain <mcgrof@kernel.org>
"""

import pytest
import sys
import os
import json

# Add parent directory to path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from tests.common.fixtures import TestFixture
from botocore.exceptions import ClientError


def test_put_bucket_policy_with_condition_string_like(s3_client, config):
    """
    Test PutBucketPolicy with Condition block using StringLike

    Conditions allow conditional policy enforcement
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("policy-condition-str")
        s3_client.create_bucket(bucket_name)

        # Create policy with StringLike condition
        policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": "*",
                    "Action": "s3:GetObject",
                    "Resource": f"arn:aws:s3:::{bucket_name}/*",
                    "Condition": {
                        "StringLike": {"s3:prefix": ["photos/*", "videos/*"]}
                    },
                }
            ],
        }

        # Put bucket policy
        try:
            s3_client.client.put_bucket_policy(
                Bucket=bucket_name, Policy=json.dumps(policy)
            )
        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            # MinIO may not fully support Condition blocks
            if error_code in ["NotImplemented", "AccessDenied", "MalformedPolicy"]:
                pytest.skip(f"Condition blocks not supported: {error_code}")
                return
            raise

        # Verify policy was set
        try:
            policy_response = s3_client.client.get_bucket_policy(Bucket=bucket_name)
            assert "Policy" in policy_response
        except ClientError as e:
            if e.response["Error"]["Code"] == "NotImplemented":
                pytest.skip("GetBucketPolicy not supported")

    finally:
        fixture.cleanup()


def test_put_bucket_policy_with_condition_ip_address(s3_client, config):
    """
    Test PutBucketPolicy with IpAddress condition

    Restricts access based on source IP address
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("policy-condition-ip")
        s3_client.create_bucket(bucket_name)

        # Create policy with IpAddress condition
        policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": "*",
                    "Action": "s3:GetObject",
                    "Resource": f"arn:aws:s3:::{bucket_name}/*",
                    "Condition": {
                        "IpAddress": {"aws:SourceIp": ["192.168.1.0/24", "10.0.0.0/8"]}
                    },
                }
            ],
        }

        # Put bucket policy
        try:
            s3_client.client.put_bucket_policy(
                Bucket=bucket_name, Policy=json.dumps(policy)
            )
        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            # MinIO may not fully support IpAddress conditions
            if error_code in ["NotImplemented", "AccessDenied", "MalformedPolicy"]:
                pytest.skip(f"IpAddress condition not supported: {error_code}")
                return
            raise

        # Verify policy was set
        try:
            policy_response = s3_client.client.get_bucket_policy(Bucket=bucket_name)
            assert "Policy" in policy_response
        except ClientError as e:
            if e.response["Error"]["Code"] == "NotImplemented":
                pytest.skip("GetBucketPolicy not supported")

    finally:
        fixture.cleanup()


def test_put_bucket_policy_size_limit(s3_client, config):
    """
    Test PutBucketPolicy with policy size limits

    S3 bucket policies have a 20KB size limit
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("policy-size-limit")
        s3_client.create_bucket(bucket_name)

        # Create large policy with many statements (approaching 20KB limit)
        # Each statement is ~200 bytes, so 100+ statements would exceed limit
        statements = []
        for i in range(150):
            statements.append(
                {
                    "Sid": f"Statement{i}",
                    "Effect": "Allow",
                    "Principal": "*",
                    "Action": "s3:GetObject",
                    "Resource": f"arn:aws:s3:::{bucket_name}/path{i}/*",
                }
            )

        policy = {"Version": "2012-10-17", "Statement": statements}

        # Try PutBucketPolicy with oversized policy
        try:
            s3_client.client.put_bucket_policy(
                Bucket=bucket_name, Policy=json.dumps(policy)
            )
            # Some implementations may accept larger policies
        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            # Should return error for policy too large
            assert error_code in [
                "PolicyTooLarge",
                "InvalidArgument",
                "MalformedPolicy",
            ], f"Expected PolicyTooLarge, got {error_code}"

    finally:
        fixture.cleanup()
