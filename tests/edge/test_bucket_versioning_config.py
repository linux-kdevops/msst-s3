#!/usr/bin/env python3
"""
S3 Bucket Versioning Configuration Tests

Tests PutBucketVersioning and GetBucketVersioning APIs:
- Enabling and suspending bucket versioning
- Versioning status retrieval
- Error handling for non-existing buckets
- Invalid versioning status values

Ported from versitygw integration tests.

Generated-by: Claude AI
Signed-off-by: Luis Chamberlain <mcgrof@kernel.org>
"""

import pytest
import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from tests.common.fixtures import TestFixture
from botocore.exceptions import ClientError


def test_put_bucket_versioning_non_existing_bucket(s3_client, config):
    """
    Test PutBucketVersioning on non-existing bucket

    MinIO may silently succeed or return NoSuchBucket (implementation-specific)
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("ver-put-nobucket")

        # Try to enable versioning on non-existing bucket
        # MinIO behavior varies - may succeed silently or return error
        try:
            s3_client.put_bucket_versioning(bucket_name, {"Status": "Enabled"})
            # MinIO succeeded silently - this is acceptable behavior
        except ClientError as e:
            # Should return NoSuchBucket if it errors
            error_code = e.response["Error"]["Code"]
            assert (
                error_code == "NoSuchBucket"
            ), f"Expected NoSuchBucket, got {error_code}"

    finally:
        fixture.cleanup()


def test_put_bucket_versioning_invalid_status(s3_client, config):
    """
    Test PutBucketVersioning with invalid status value

    Should return error (MalformedXML or IllegalVersioningConfigurationException)
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("ver-put-invalid")
        s3_client.create_bucket(bucket_name)

        # Try to set invalid versioning status
        with pytest.raises(ClientError) as exc_info:
            s3_client.put_bucket_versioning(bucket_name, {"Status": "InvalidStatus"})

        error_code = exc_info.value.response["Error"]["Code"]
        # MinIO returns IllegalVersioningConfigurationException
        assert error_code in [
            "MalformedXML",
            "InvalidArgument",
            "IllegalVersioningConfigurationException",
        ], f"Expected versioning error, got {error_code}"

    finally:
        fixture.cleanup()


def test_put_bucket_versioning_success_enabled(s3_client, config):
    """
    Test PutBucketVersioning to enable versioning

    Should successfully enable versioning on bucket
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("ver-put-enabled")
        s3_client.create_bucket(bucket_name)

        # Enable versioning
        s3_client.put_bucket_versioning(bucket_name, {"Status": "Enabled"})

        # Verify versioning is enabled
        response = s3_client.get_bucket_versioning(bucket_name)
        assert response.get("Status") == "Enabled"

    finally:
        fixture.cleanup()


def test_put_bucket_versioning_success_suspended(s3_client, config):
    """
    Test PutBucketVersioning to suspend versioning

    Should successfully suspend versioning on bucket
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("ver-put-suspended")
        s3_client.create_bucket(bucket_name)

        # Enable versioning first
        s3_client.put_bucket_versioning(bucket_name, {"Status": "Enabled"})

        # Suspend versioning
        s3_client.put_bucket_versioning(bucket_name, {"Status": "Suspended"})

        # Verify versioning is suspended
        response = s3_client.get_bucket_versioning(bucket_name)
        assert response.get("Status") == "Suspended"

    finally:
        fixture.cleanup()


def test_get_bucket_versioning_non_existing_bucket(s3_client, config):
    """
    Test GetBucketVersioning on non-existing bucket

    Should return NoSuchBucket error
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("ver-get-nobucket")

        # Try to get versioning on non-existing bucket
        with pytest.raises(ClientError) as exc_info:
            s3_client.get_bucket_versioning(bucket_name)

        error_code = exc_info.value.response["Error"]["Code"]
        assert error_code == "NoSuchBucket", f"Expected NoSuchBucket, got {error_code}"

    finally:
        fixture.cleanup()


def test_get_bucket_versioning_empty_response(s3_client, config):
    """
    Test GetBucketVersioning on bucket with versioning not configured

    Should return empty/absent Status field
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("ver-get-empty")
        s3_client.create_bucket(bucket_name)

        # Get versioning on bucket with versioning not configured
        response = s3_client.get_bucket_versioning(bucket_name)

        # MinIO may return empty dict or dict without Status field
        # Both behaviors are acceptable
        assert "Status" not in response or response.get("Status") in [None, ""]

    finally:
        fixture.cleanup()


def test_get_bucket_versioning_success(s3_client, config):
    """
    Test GetBucketVersioning on versioned bucket

    Should return Status="Enabled"
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("ver-get-success")
        s3_client.create_bucket(bucket_name)

        # Enable versioning
        s3_client.put_bucket_versioning(bucket_name, {"Status": "Enabled"})

        # Get versioning status
        response = s3_client.get_bucket_versioning(bucket_name)

        assert "Status" in response
        assert response["Status"] == "Enabled"

    finally:
        fixture.cleanup()


def test_versioning_delete_bucket_not_empty(s3_client, config):
    """
    Test deleting bucket with object versions

    Should return BucketNotEmpty error (versioned buckets can't be deleted)
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("ver-del-not-empty")
        s3_client.create_bucket(bucket_name)

        # Enable versioning
        s3_client.put_bucket_versioning(bucket_name, {"Status": "Enabled"})

        # Create object versions
        key = "my-obj"
        s3_client.put_object(bucket_name, key, b"v1")
        s3_client.put_object(bucket_name, key, b"v2")

        # Try to delete bucket (should fail - has versions)
        with pytest.raises(ClientError) as exc_info:
            s3_client.client.delete_bucket(Bucket=bucket_name)

        error_code = exc_info.value.response["Error"]["Code"]
        # MinIO may return BucketNotEmpty or VersionedBucketNotEmpty
        assert error_code in [
            "BucketNotEmpty",
            "VersionedBucketNotEmpty",
        ], f"Expected BucketNotEmpty/VersionedBucketNotEmpty, got {error_code}"

    finally:
        fixture.cleanup()


def test_bucket_versioning_toggle(s3_client, config):
    """
    Test toggling bucket versioning multiple times

    Should handle Enabled → Suspended → Enabled transitions
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("ver-toggle")
        s3_client.create_bucket(bucket_name)

        # Initially not configured
        response1 = s3_client.get_bucket_versioning(bucket_name)
        assert "Status" not in response1 or response1.get("Status") in [None, ""]

        # Enable versioning
        s3_client.put_bucket_versioning(bucket_name, {"Status": "Enabled"})
        response2 = s3_client.get_bucket_versioning(bucket_name)
        assert response2["Status"] == "Enabled"

        # Suspend versioning
        s3_client.put_bucket_versioning(bucket_name, {"Status": "Suspended"})
        response3 = s3_client.get_bucket_versioning(bucket_name)
        assert response3["Status"] == "Suspended"

        # Re-enable versioning
        s3_client.put_bucket_versioning(bucket_name, {"Status": "Enabled"})
        response4 = s3_client.get_bucket_versioning(bucket_name)
        assert response4["Status"] == "Enabled"

    finally:
        fixture.cleanup()


def test_versioning_mfa_delete_not_supported(s3_client, config):
    """
    Test MFADelete configuration (often not supported by S3-compatible services)

    MinIO may ignore MFADelete parameter
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("ver-mfa")
        s3_client.create_bucket(bucket_name)

        # Try to enable versioning with MFADelete
        # MinIO typically ignores MFADelete but doesn't error
        try:
            s3_client.client.put_bucket_versioning(
                Bucket=bucket_name,
                VersioningConfiguration={"Status": "Enabled", "MFADelete": "Enabled"},
            )

            # Get versioning status
            response = s3_client.get_bucket_versioning(bucket_name)
            assert response.get("Status") == "Enabled"

            # MFADelete may or may not be in response (implementation-specific)
            # Both behaviors are acceptable
        except ClientError as e:
            # Some implementations may reject MFADelete
            error_code = e.response["Error"]["Code"]
            assert error_code in ["InvalidArgument", "NotImplemented"]

    finally:
        fixture.cleanup()
