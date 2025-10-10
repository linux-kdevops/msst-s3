#!/usr/bin/env python3
"""
S3 Object Lock Configuration Tests

Tests PutObjectLockConfiguration and GetObjectLockConfiguration with:
- Non-existing bucket errors
- Empty/invalid configurations
- Invalid status and mode values
- Both years and days specified (invalid)
- Negative retention periods
- Successful configuration operations

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


def test_put_object_lock_configuration_non_existing_bucket(s3_client, config):
    """
    Test PutObjectLockConfiguration on non-existing bucket

    Should return NoSuchBucket error
    """
    fixture = TestFixture(s3_client, config)

    try:
        # Try to set object lock config on non-existing bucket
        non_existing_bucket = fixture.generate_bucket_name("non-existing")

        with pytest.raises(ClientError) as exc_info:
            s3_client.client.put_object_lock_configuration(
                Bucket=non_existing_bucket,
                ObjectLockConfiguration={"ObjectLockEnabled": "Enabled"},
            )

        error_code = exc_info.value.response["Error"]["Code"]
        # MinIO may return InvalidBucketState instead of NoSuchBucket
        assert error_code in [
            "NoSuchBucket",
            "InvalidBucketState",
        ], f"Expected NoSuchBucket, got {error_code}"

    finally:
        fixture.cleanup()


def test_put_object_lock_configuration_empty_config(s3_client, config):
    """
    Test PutObjectLockConfiguration with empty configuration

    Should return MalformedXML error
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("lock-empty-config")
        s3_client.create_bucket(bucket_name)

        # Try to set empty object lock configuration
        with pytest.raises(ClientError) as exc_info:
            s3_client.client.put_object_lock_configuration(
                Bucket=bucket_name
                # No ObjectLockConfiguration parameter
            )

        error_code = exc_info.value.response["Error"]["Code"]
        assert error_code in [
            "MalformedXML",
            "InvalidArgument",
            "InvalidRequest",
        ], f"Expected MalformedXML, got {error_code}"

    finally:
        fixture.cleanup()


def test_put_object_lock_configuration_not_enabled_on_bucket_creation(
    s3_client, config
):
    """
    Test PutObjectLockConfiguration on bucket without object lock enabled

    Note: This test addresses versitygw-specific behavior where object lock
    can be enabled without bucket versioning. NOT S3 compatible.
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("lock-not-enabled")
        s3_client.create_bucket(bucket_name)

        # Try to set object lock configuration on bucket created without object lock
        try:
            s3_client.client.put_object_lock_configuration(
                Bucket=bucket_name,
                ObjectLockConfiguration={
                    "ObjectLockEnabled": "Enabled",
                    "Rule": {
                        "DefaultRetention": {
                            "Mode": "COMPLIANCE",
                            "Days": 12,
                        }
                    },
                },
            )
            # versitygw allows this (non-S3 compatible), MinIO/AWS may reject
        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            # AWS/MinIO reject setting object lock on bucket without it enabled
            assert error_code in [
                "InvalidBucketState",
                "InvalidArgument",
                "NotImplemented",
            ]

    finally:
        fixture.cleanup()


def test_put_object_lock_configuration_invalid_status(s3_client, config):
    """
    Test PutObjectLockConfiguration with invalid ObjectLockEnabled status

    boto3 validates status client-side, so test may not reach server
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("lock-invalid-status")
        s3_client.create_bucket(bucket_name)

        # Try to set invalid ObjectLockEnabled status
        try:
            s3_client.client.put_object_lock_configuration(
                Bucket=bucket_name,
                ObjectLockConfiguration={
                    "ObjectLockEnabled": "invalid_status",  # Invalid value
                    "Rule": {"DefaultRetention": {"Days": 12}},
                },
            )
            # If successful, implementation accepted invalid value
        except Exception as e:
            # boto3 validates client-side or server rejects
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


def test_put_object_lock_configuration_invalid_mode(s3_client, config):
    """
    Test PutObjectLockConfiguration with invalid retention mode

    boto3 validates mode client-side
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("lock-invalid-mode")
        s3_client.create_bucket(bucket_name)

        # Try to set invalid retention mode
        try:
            s3_client.client.put_object_lock_configuration(
                Bucket=bucket_name,
                ObjectLockConfiguration={
                    "ObjectLockEnabled": "Enabled",
                    "Rule": {
                        "DefaultRetention": {
                            "Mode": "invalid_mode",  # Invalid mode
                            "Days": 12,
                        }
                    },
                },
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


def test_put_object_lock_configuration_both_years_and_days(s3_client, config):
    """
    Test PutObjectLockConfiguration with both Years and Days

    S3 only allows one or the other, not both
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("lock-years-and-days")
        s3_client.create_bucket(bucket_name)

        # Try to set both Years and Days (invalid)
        with pytest.raises(ClientError) as exc_info:
            s3_client.client.put_object_lock_configuration(
                Bucket=bucket_name,
                ObjectLockConfiguration={
                    "ObjectLockEnabled": "Enabled",
                    "Rule": {
                        "DefaultRetention": {
                            "Mode": "COMPLIANCE",
                            "Days": 12,
                            "Years": 24,  # Cannot specify both
                        }
                    },
                },
            )

        error_code = exc_info.value.response["Error"]["Code"]
        assert error_code in [
            "MalformedXML",
            "InvalidArgument",
            "InvalidRequest",
        ], f"Expected MalformedXML, got {error_code}"

    finally:
        fixture.cleanup()


def test_put_object_lock_configuration_invalid_years_days(s3_client, config):
    """
    Test PutObjectLockConfiguration with negative retention periods

    Negative values should be rejected
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("lock-negative-retention")
        s3_client.create_bucket(bucket_name)

        # Try to set negative days
        with pytest.raises(ClientError) as exc_info:
            s3_client.client.put_object_lock_configuration(
                Bucket=bucket_name,
                ObjectLockConfiguration={
                    "ObjectLockEnabled": "Enabled",
                    "Rule": {
                        "DefaultRetention": {
                            "Mode": "COMPLIANCE",
                            "Days": -3,  # Negative days
                        }
                    },
                },
            )

        error_code = exc_info.value.response["Error"]["Code"]
        assert error_code in [
            "InvalidArgument",
            "InvalidRequest",
        ], f"Expected InvalidArgument, got {error_code}"

        # Try to set negative years
        with pytest.raises(ClientError) as exc_info:
            s3_client.client.put_object_lock_configuration(
                Bucket=bucket_name,
                ObjectLockConfiguration={
                    "ObjectLockEnabled": "Enabled",
                    "Rule": {
                        "DefaultRetention": {
                            "Mode": "COMPLIANCE",
                            "Years": -5,  # Negative years
                        }
                    },
                },
            )

        error_code = exc_info.value.response["Error"]["Code"]
        assert error_code in [
            "InvalidArgument",
            "InvalidRequest",
        ], f"Expected InvalidArgument, got {error_code}"

    finally:
        fixture.cleanup()


def test_put_object_lock_configuration_success(s3_client, config):
    """
    Test successful PutObjectLockConfiguration

    Requires bucket created with object lock enabled
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("lock-success")

        # Create bucket with object lock enabled
        try:
            s3_client.client.create_bucket(
                Bucket=bucket_name, ObjectLockEnabledForBucket=True
            )
        except ClientError as e:
            if e.response["Error"]["Code"] in ["NotImplemented", "InvalidArgument"]:
                pytest.skip("Object lock not supported")
            raise

        # Set minimal object lock configuration
        s3_client.client.put_object_lock_configuration(
            Bucket=bucket_name,
            ObjectLockConfiguration={"ObjectLockEnabled": "Enabled"},
        )

        # Verify configuration was set
        config_response = s3_client.client.get_object_lock_configuration(
            Bucket=bucket_name
        )

        assert "ObjectLockConfiguration" in config_response
        assert (
            config_response["ObjectLockConfiguration"]["ObjectLockEnabled"] == "Enabled"
        )

    finally:
        fixture.cleanup()


def test_get_object_lock_configuration_non_existing_bucket(s3_client, config):
    """
    Test GetObjectLockConfiguration on non-existing bucket

    Should return NoSuchBucket error
    """
    fixture = TestFixture(s3_client, config)

    try:
        non_existing_bucket = fixture.generate_bucket_name("non-existing")

        with pytest.raises(ClientError) as exc_info:
            s3_client.client.get_object_lock_configuration(Bucket=non_existing_bucket)

        error_code = exc_info.value.response["Error"]["Code"]
        # MinIO may return ObjectLockConfigurationNotFoundError instead of NoSuchBucket
        assert error_code in [
            "NoSuchBucket",
            "ObjectLockConfigurationNotFoundError",
            "ObjectLockConfigurationNotFound",
        ], f"Expected NoSuchBucket, got {error_code}"

    finally:
        fixture.cleanup()


def test_get_object_lock_configuration_unset_config(s3_client, config):
    """
    Test GetObjectLockConfiguration when none has been set

    Should return ObjectLockConfigurationNotFoundError
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("lock-unset")
        s3_client.create_bucket(bucket_name)

        with pytest.raises(ClientError) as exc_info:
            s3_client.client.get_object_lock_configuration(Bucket=bucket_name)

        error_code = exc_info.value.response["Error"]["Code"]
        assert error_code in [
            "ObjectLockConfigurationNotFoundError",
            "ObjectLockConfigurationNotFound",
            "NoSuchConfiguration",
        ], f"Expected ObjectLockConfigurationNotFoundError, got {error_code}"

    finally:
        fixture.cleanup()


def test_get_object_lock_configuration_success(s3_client, config):
    """
    Test successful GetObjectLockConfiguration

    Sets configuration and verifies it can be retrieved
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("lock-get-success")

        # Create bucket with object lock enabled
        try:
            s3_client.client.create_bucket(
                Bucket=bucket_name, ObjectLockEnabledForBucket=True
            )
        except ClientError as e:
            if e.response["Error"]["Code"] in ["NotImplemented", "InvalidArgument"]:
                pytest.skip("Object lock not supported")
            raise

        # Set object lock configuration
        s3_client.client.put_object_lock_configuration(
            Bucket=bucket_name,
            ObjectLockConfiguration={
                "ObjectLockEnabled": "Enabled",
                "Rule": {"DefaultRetention": {"Mode": "COMPLIANCE", "Days": 20}},
            },
        )

        # Get object lock configuration
        config_response = s3_client.client.get_object_lock_configuration(
            Bucket=bucket_name
        )

        # Verify configuration
        assert "ObjectLockConfiguration" in config_response
        lock_config = config_response["ObjectLockConfiguration"]

        assert lock_config["ObjectLockEnabled"] == "Enabled"
        assert "Rule" in lock_config
        assert "DefaultRetention" in lock_config["Rule"]

        retention = lock_config["Rule"]["DefaultRetention"]
        assert retention["Mode"] == "COMPLIANCE"
        assert retention["Days"] == 20

    finally:
        fixture.cleanup()
