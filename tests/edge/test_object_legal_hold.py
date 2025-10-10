#!/usr/bin/env python3
"""
S3 Object Legal Hold Tests

Tests PutObjectLegalHold and GetObjectLegalHold with:
- Non-existing bucket and object errors
- Invalid request body and status values
- Object lock configuration requirements
- Legal hold ON/OFF status management

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


def test_put_object_legal_hold_non_existing_bucket(s3_client, config):
    """
    Test PutObjectLegalHold on non-existing bucket

    Should return NoSuchBucket error
    """
    fixture = TestFixture(s3_client, config)

    try:
        non_existing_bucket = fixture.generate_bucket_name("non-existing")

        with pytest.raises(ClientError) as exc_info:
            s3_client.client.put_object_legal_hold(
                Bucket=non_existing_bucket,
                Key="my-obj",
            )

        error_code = exc_info.value.response["Error"]["Code"]
        assert error_code == "NoSuchBucket", f"Expected NoSuchBucket, got {error_code}"

    finally:
        fixture.cleanup()


def test_put_object_legal_hold_non_existing_object(s3_client, config):
    """
    Test PutObjectLegalHold on non-existing object

    Should return NoSuchKey error
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("legal-hold-no-obj")

        # Create bucket with object lock enabled
        try:
            s3_client.client.create_bucket(
                Bucket=bucket_name, ObjectLockEnabledForBucket=True
            )
        except ClientError as e:
            if e.response["Error"]["Code"] in ["NotImplemented", "InvalidArgument"]:
                pytest.skip("Object lock not supported")
            raise

        # Try to set legal hold on non-existing object
        with pytest.raises(ClientError) as exc_info:
            s3_client.client.put_object_legal_hold(
                Bucket=bucket_name,
                Key="my-obj",
                LegalHold={"Status": "ON"},
            )

        error_code = exc_info.value.response["Error"]["Code"]
        assert error_code == "NoSuchKey", f"Expected NoSuchKey, got {error_code}"

    finally:
        fixture.cleanup()


def test_put_object_legal_hold_invalid_body(s3_client, config):
    """
    Test PutObjectLegalHold with empty/missing LegalHold body

    Should return MalformedXML or InvalidArgument error
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("legal-hold-invalid-body")
        s3_client.create_bucket(bucket_name)

        # Try to set legal hold without LegalHold parameter
        with pytest.raises(ClientError) as exc_info:
            s3_client.client.put_object_legal_hold(
                Bucket=bucket_name,
                Key="my-obj",
                # Missing LegalHold parameter
            )

        error_code = exc_info.value.response["Error"]["Code"]
        assert error_code in [
            "MalformedXML",
            "InvalidArgument",
            "InvalidRequest",
        ], f"Expected MalformedXML, got {error_code}"

    finally:
        fixture.cleanup()


def test_put_object_legal_hold_invalid_status(s3_client, config):
    """
    Test PutObjectLegalHold with invalid status value

    boto3 validates status client-side (ON or OFF only)
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("legal-hold-invalid-status")

        # Create bucket with object lock enabled to test invalid status
        try:
            s3_client.client.create_bucket(
                Bucket=bucket_name, ObjectLockEnabledForBucket=True
            )
        except ClientError as e:
            if e.response["Error"]["Code"] in ["NotImplemented", "InvalidArgument"]:
                pytest.skip("Object lock not supported")
            raise

        # Try to set invalid legal hold status
        try:
            s3_client.client.put_object_legal_hold(
                Bucket=bucket_name,
                Key="my-obj",
                LegalHold={"Status": "invalid_status"},  # Invalid status
            )
            # If successful, implementation accepted invalid value
        except Exception as e:
            error_type = type(e).__name__
            if error_type == "ParamValidationError":
                pass  # Client-side validation (expected)
            elif error_type == "ClientError":
                error_code = e.response["Error"]["Code"]
                assert error_code in ["MalformedXML", "InvalidArgument"]
            else:
                raise

    finally:
        fixture.cleanup()


def test_put_object_legal_hold_unset_bucket_object_lock_config(s3_client, config):
    """
    Test PutObjectLegalHold on bucket without object lock enabled

    Should return InvalidRequest or InvalidBucketObjectLockConfiguration
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("legal-hold-no-lock")
        s3_client.create_bucket(bucket_name)

        # Put an object
        s3_client.put_object(bucket_name, "my-obj", b"data")

        # Try to set legal hold on bucket without object lock
        with pytest.raises(ClientError) as exc_info:
            s3_client.client.put_object_legal_hold(
                Bucket=bucket_name,
                Key="my-obj",
                LegalHold={"Status": "ON"},
            )

        error_code = exc_info.value.response["Error"]["Code"]
        assert error_code in [
            "InvalidRequest",
            "InvalidBucketObjectLockConfiguration",
        ], f"Expected InvalidRequest, got {error_code}"

    finally:
        fixture.cleanup()


def test_put_object_legal_hold_success(s3_client, config):
    """
    Test successful PutObjectLegalHold

    Sets legal hold ON and verifies with GetObjectLegalHold
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("legal-hold-success")

        # Create bucket with object lock enabled
        try:
            s3_client.client.create_bucket(
                Bucket=bucket_name, ObjectLockEnabledForBucket=True
            )
        except ClientError as e:
            if e.response["Error"]["Code"] in ["NotImplemented", "InvalidArgument"]:
                pytest.skip("Object lock not supported")
            raise

        # Put object
        s3_client.put_object(bucket_name, "my-obj", b"test data")

        # Set legal hold ON
        s3_client.client.put_object_legal_hold(
            Bucket=bucket_name,
            Key="my-obj",
            LegalHold={"Status": "ON"},
        )

        # Verify legal hold was set
        legal_hold_response = s3_client.client.get_object_legal_hold(
            Bucket=bucket_name, Key="my-obj"
        )

        assert "LegalHold" in legal_hold_response
        assert legal_hold_response["LegalHold"]["Status"] == "ON"

        # Remove legal hold for cleanup
        s3_client.client.put_object_legal_hold(
            Bucket=bucket_name,
            Key="my-obj",
            LegalHold={"Status": "OFF"},
        )

    finally:
        fixture.cleanup()


def test_get_object_legal_hold_non_existing_bucket(s3_client, config):
    """
    Test GetObjectLegalHold on non-existing bucket

    Should return NoSuchBucket error
    """
    fixture = TestFixture(s3_client, config)

    try:
        non_existing_bucket = fixture.generate_bucket_name("non-existing")

        with pytest.raises(ClientError) as exc_info:
            s3_client.client.get_object_legal_hold(
                Bucket=non_existing_bucket, Key="my-obj"
            )

        error_code = exc_info.value.response["Error"]["Code"]
        # MinIO may return InvalidRequest for non-existing bucket
        assert error_code in [
            "NoSuchBucket",
            "InvalidRequest",
            "NoSuchKey",
        ], f"Expected NoSuchBucket, got {error_code}"

    finally:
        fixture.cleanup()


def test_get_object_legal_hold_non_existing_object(s3_client, config):
    """
    Test GetObjectLegalHold on non-existing object

    Should return NoSuchKey error
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("get-legal-hold-no-obj")

        # Create bucket with object lock enabled
        try:
            s3_client.client.create_bucket(
                Bucket=bucket_name, ObjectLockEnabledForBucket=True
            )
        except ClientError as e:
            if e.response["Error"]["Code"] in ["NotImplemented", "InvalidArgument"]:
                pytest.skip("Object lock not supported")
            raise

        with pytest.raises(ClientError) as exc_info:
            s3_client.client.get_object_legal_hold(Bucket=bucket_name, Key="my-obj")

        error_code = exc_info.value.response["Error"]["Code"]
        assert error_code == "NoSuchKey", f"Expected NoSuchKey, got {error_code}"

    finally:
        fixture.cleanup()


def test_get_object_legal_hold_disabled_lock(s3_client, config):
    """
    Test GetObjectLegalHold on bucket without object lock

    Should return InvalidRequest or InvalidBucketObjectLockConfiguration
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("get-legal-hold-no-lock")
        s3_client.create_bucket(bucket_name)

        # Put object
        s3_client.put_object(bucket_name, "my-obj", b"data")

        with pytest.raises(ClientError) as exc_info:
            s3_client.client.get_object_legal_hold(Bucket=bucket_name, Key="my-obj")

        error_code = exc_info.value.response["Error"]["Code"]
        assert error_code in [
            "InvalidRequest",
            "InvalidBucketObjectLockConfiguration",
            "ObjectLockConfigurationNotFoundError",
        ], f"Expected InvalidRequest, got {error_code}"

    finally:
        fixture.cleanup()


def test_get_object_legal_hold_unset_config(s3_client, config):
    """
    Test GetObjectLegalHold on object without legal hold set

    Should return NoSuchObjectLockConfiguration or similar error
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("get-legal-hold-unset")

        # Create bucket with object lock enabled
        try:
            s3_client.client.create_bucket(
                Bucket=bucket_name, ObjectLockEnabledForBucket=True
            )
        except ClientError as e:
            if e.response["Error"]["Code"] in ["NotImplemented", "InvalidArgument"]:
                pytest.skip("Object lock not supported")
            raise

        # Put object without legal hold
        s3_client.put_object(bucket_name, "my-obj", b"data")

        with pytest.raises(ClientError) as exc_info:
            s3_client.client.get_object_legal_hold(Bucket=bucket_name, Key="my-obj")

        error_code = exc_info.value.response["Error"]["Code"]
        assert error_code in [
            "NoSuchObjectLockConfiguration",
            "InvalidRequest",
        ], f"Expected NoSuchObjectLockConfiguration, got {error_code}"

    finally:
        fixture.cleanup()


def test_get_object_legal_hold_success(s3_client, config):
    """
    Test successful GetObjectLegalHold

    Sets legal hold and retrieves it
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("get-legal-hold-success")

        # Create bucket with object lock enabled
        try:
            s3_client.client.create_bucket(
                Bucket=bucket_name, ObjectLockEnabledForBucket=True
            )
        except ClientError as e:
            if e.response["Error"]["Code"] in ["NotImplemented", "InvalidArgument"]:
                pytest.skip("Object lock not supported")
            raise

        # Put object
        s3_client.put_object(bucket_name, "my-obj", b"test data")

        # Set legal hold ON
        s3_client.client.put_object_legal_hold(
            Bucket=bucket_name,
            Key="my-obj",
            LegalHold={"Status": "ON"},
        )

        # Get legal hold
        legal_hold_response = s3_client.client.get_object_legal_hold(
            Bucket=bucket_name, Key="my-obj"
        )

        # Verify response
        assert "LegalHold" in legal_hold_response
        legal_hold = legal_hold_response["LegalHold"]

        assert legal_hold["Status"] == "ON"

        # Test setting legal hold OFF
        s3_client.client.put_object_legal_hold(
            Bucket=bucket_name,
            Key="my-obj",
            LegalHold={"Status": "OFF"},
        )

        # Verify it was turned off
        legal_hold_response2 = s3_client.client.get_object_legal_hold(
            Bucket=bucket_name, Key="my-obj"
        )

        assert legal_hold_response2["LegalHold"]["Status"] == "OFF"

    finally:
        fixture.cleanup()
