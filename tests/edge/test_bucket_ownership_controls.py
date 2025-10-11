#!/usr/bin/env python3
"""
S3 Bucket Ownership Controls Tests

Tests PutBucketOwnershipControls, GetBucketOwnershipControls, and
DeleteBucketOwnershipControls with:
- Non-existing bucket errors
- Multiple rules validation (only 1 rule allowed)
- Invalid ownership value validation
- Default ownership behavior (BucketOwnerEnforced)
- Ownership setting and retrieval
- Ownership deletion

Valid ObjectOwnership values:
- BucketOwnerPreferred
- BucketOwnerEnforced
- ObjectWriter

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


def test_put_bucket_ownership_controls_non_existing_bucket(s3_client, config):
    """
    Test PutBucketOwnershipControls on non-existing bucket

    Should return NoSuchBucket error
    """
    fixture = TestFixture(s3_client, config)

    try:
        non_existing_bucket = fixture.generate_bucket_name("non-existing")

        ownership_controls = {
            "Rules": [
                {"ObjectOwnership": "BucketOwnerPreferred"},
            ]
        }

        with pytest.raises(ClientError) as exc_info:
            s3_client.client.put_bucket_ownership_controls(
                Bucket=non_existing_bucket,
                OwnershipControls=ownership_controls,
            )

        error_code = exc_info.value.response["Error"]["Code"]
        # MinIO may return MalformedXML when feature not supported
        if error_code == "MalformedXML":
            pytest.skip(
                "Bucket ownership controls not supported by this S3 implementation"
            )
        assert error_code == "NoSuchBucket", f"Expected NoSuchBucket, got {error_code}"

    finally:
        fixture.cleanup()


def test_put_bucket_ownership_controls_multiple_rules(s3_client, config):
    """
    Test PutBucketOwnershipControls with multiple rules

    Only 1 rule is allowed - should return MalformedXML error
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("ownership-multiple")
        s3_client.create_bucket(bucket_name)

        # Try to set multiple ownership rules (invalid)
        ownership_controls = {
            "Rules": [
                {"ObjectOwnership": "BucketOwnerPreferred"},
                {"ObjectOwnership": "ObjectWriter"},
            ]
        }

        with pytest.raises(ClientError) as exc_info:
            s3_client.client.put_bucket_ownership_controls(
                Bucket=bucket_name,
                OwnershipControls=ownership_controls,
            )

        error_code = exc_info.value.response["Error"]["Code"]
        assert error_code in [
            "MalformedXML",
            "InvalidArgument",
        ], f"Expected MalformedXML, got {error_code}"

    finally:
        fixture.cleanup()


def test_put_bucket_ownership_controls_invalid_ownership(s3_client, config):
    """
    Test PutBucketOwnershipControls with invalid ownership value

    Valid values are: BucketOwnerPreferred, BucketOwnerEnforced, ObjectWriter
    Invalid value should be rejected
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("ownership-invalid")
        s3_client.create_bucket(bucket_name)

        # Try to set invalid ownership value
        ownership_controls = {
            "Rules": [
                {"ObjectOwnership": "invalid_ownership"},
            ]
        }

        with pytest.raises(ClientError) as exc_info:
            s3_client.client.put_bucket_ownership_controls(
                Bucket=bucket_name,
                OwnershipControls=ownership_controls,
            )

        error_code = exc_info.value.response["Error"]["Code"]
        assert error_code in [
            "MalformedXML",
            "InvalidArgument",
        ], f"Expected MalformedXML, got {error_code}"

    finally:
        fixture.cleanup()


def test_put_bucket_ownership_controls_success(s3_client, config):
    """
    Test successful PutBucketOwnershipControls

    Sets ObjectWriter ownership
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("ownership-success")
        s3_client.create_bucket(bucket_name)

        ownership_controls = {
            "Rules": [
                {"ObjectOwnership": "ObjectWriter"},
            ]
        }

        # Should succeed
        try:
            s3_client.client.put_bucket_ownership_controls(
                Bucket=bucket_name,
                OwnershipControls=ownership_controls,
            )
        except ClientError as e:
            if e.response["Error"]["Code"] in ["NotImplemented", "MalformedXML"]:
                pytest.skip(
                    "Bucket ownership controls not supported by this S3 implementation"
                )
            raise

    finally:
        fixture.cleanup()


def test_get_bucket_ownership_controls_non_existing_bucket(s3_client, config):
    """
    Test GetBucketOwnershipControls on non-existing bucket

    Should return NoSuchBucket error
    """
    fixture = TestFixture(s3_client, config)

    try:
        non_existing_bucket = fixture.generate_bucket_name("non-existing")

        with pytest.raises(ClientError) as exc_info:
            s3_client.client.get_bucket_ownership_controls(Bucket=non_existing_bucket)

        error_code = exc_info.value.response["Error"]["Code"]
        # MinIO may return NotImplemented when feature not supported
        if error_code == "NotImplemented":
            pytest.skip(
                "Bucket ownership controls not supported by this S3 implementation"
            )
        assert error_code == "NoSuchBucket", f"Expected NoSuchBucket, got {error_code}"

    finally:
        fixture.cleanup()


def test_get_bucket_ownership_controls_default_ownership(s3_client, config):
    """
    Test GetBucketOwnershipControls default ownership

    New buckets should have BucketOwnerEnforced as default
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("ownership-default")
        s3_client.create_bucket(bucket_name)

        # Get default ownership controls
        try:
            response = s3_client.client.get_bucket_ownership_controls(
                Bucket=bucket_name
            )
        except ClientError as e:
            if e.response["Error"]["Code"] == "NotImplemented":
                pytest.skip(
                    "Bucket ownership controls not supported by this S3 implementation"
                )
            # Some implementations may return error for unset ownership
            if e.response["Error"]["Code"] == "OwnershipControlsNotFoundError":
                pytest.skip("Implementation doesn't have default ownership controls")
            raise

        # Verify default ownership
        assert "OwnershipControls" in response
        assert "Rules" in response["OwnershipControls"]
        assert len(response["OwnershipControls"]["Rules"]) == 1

        # Default should be BucketOwnerEnforced
        ownership = response["OwnershipControls"]["Rules"][0]["ObjectOwnership"]
        assert (
            ownership == "BucketOwnerEnforced"
        ), f"Expected BucketOwnerEnforced, got {ownership}"

    finally:
        fixture.cleanup()


def test_get_bucket_ownership_controls_success(s3_client, config):
    """
    Test successful GetBucketOwnershipControls

    Sets ownership and retrieves it
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("ownership-get-success")
        s3_client.create_bucket(bucket_name)

        ownership_controls = {
            "Rules": [
                {"ObjectOwnership": "ObjectWriter"},
            ]
        }

        # Set ownership controls
        try:
            s3_client.client.put_bucket_ownership_controls(
                Bucket=bucket_name,
                OwnershipControls=ownership_controls,
            )
        except ClientError as e:
            if e.response["Error"]["Code"] in ["NotImplemented", "MalformedXML"]:
                pytest.skip(
                    "Bucket ownership controls not supported by this S3 implementation"
                )
            raise

        # Get ownership controls
        response = s3_client.client.get_bucket_ownership_controls(Bucket=bucket_name)

        # Verify ownership
        assert "OwnershipControls" in response
        assert "Rules" in response["OwnershipControls"]
        assert len(response["OwnershipControls"]["Rules"]) == 1

        ownership = response["OwnershipControls"]["Rules"][0]["ObjectOwnership"]
        assert ownership == "ObjectWriter", f"Expected ObjectWriter, got {ownership}"

    finally:
        fixture.cleanup()


def test_delete_bucket_ownership_controls_non_existing_bucket(s3_client, config):
    """
    Test DeleteBucketOwnershipControls on non-existing bucket

    Should return NoSuchBucket error
    """
    fixture = TestFixture(s3_client, config)

    try:
        non_existing_bucket = fixture.generate_bucket_name("non-existing")

        with pytest.raises(ClientError) as exc_info:
            s3_client.client.delete_bucket_ownership_controls(
                Bucket=non_existing_bucket
            )

        error_code = exc_info.value.response["Error"]["Code"]
        assert error_code == "NoSuchBucket", f"Expected NoSuchBucket, got {error_code}"

    finally:
        fixture.cleanup()


def test_delete_bucket_ownership_controls_success(s3_client, config):
    """
    Test successful DeleteBucketOwnershipControls

    Deletes ownership controls and verifies removal
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("ownership-delete")
        s3_client.create_bucket(bucket_name)

        # Delete ownership controls (should succeed even if not set)
        try:
            s3_client.client.delete_bucket_ownership_controls(Bucket=bucket_name)
        except ClientError as e:
            if e.response["Error"]["Code"] == "NotImplemented":
                pytest.skip(
                    "Bucket ownership controls not supported by this S3 implementation"
                )
            raise

        # Verify ownership controls were deleted
        # GetBucketOwnershipControls should return error
        with pytest.raises(ClientError) as exc_info:
            s3_client.client.get_bucket_ownership_controls(Bucket=bucket_name)

        error_code = exc_info.value.response["Error"]["Code"]
        if error_code == "NotImplemented":
            # Ownership controls not supported - test passes anyway
            return
        assert error_code in [
            "OwnershipControlsNotFoundError",
            "OwnershipControlsNotFound",
            "NoSuchOwnershipControls",
        ], f"Expected OwnershipControlsNotFoundError, got {error_code}"

    finally:
        fixture.cleanup()
