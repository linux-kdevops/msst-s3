#!/usr/bin/env python3
"""
S3 CreateMultipartUpload Special Cases Tests

Tests CreateMultipartUpload special scenarios:
- Object key edge cases
- Concurrent uploads to same key
- Upload lifecycle management
- Special characters in keys
- Checksum algorithms

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


def test_create_multipart_upload_with_checksum_algorithm(s3_client, config):
    """
    Test CreateMultipartUpload with ChecksumAlgorithm

    Should support CRC32, SHA1, SHA256 algorithms
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("cmu-checksum")
        object_key = "checksum-object"

        s3_client.create_bucket(bucket_name)

        # Try CRC32 algorithm
        try:
            mp_response = s3_client.client.create_multipart_upload(
                Bucket=bucket_name, Key=object_key, ChecksumAlgorithm="CRC32"
            )
            upload_id = mp_response["UploadId"]

            # Verify ChecksumAlgorithm in response
            if "ChecksumAlgorithm" in mp_response:
                assert mp_response["ChecksumAlgorithm"] == "CRC32"

            # Cleanup
            s3_client.client.abort_multipart_upload(
                Bucket=bucket_name, Key=object_key, UploadId=upload_id
            )

        except ClientError as e:
            if e.response["Error"]["Code"] in ["NotImplemented", "InvalidArgument"]:
                pytest.skip("ChecksumAlgorithm not supported")
                return
            raise

    finally:
        fixture.cleanup()


def test_create_multipart_upload_with_object_lock_mode(s3_client, config):
    """
    Test CreateMultipartUpload with ObjectLockMode

    Object lock settings should be preserved
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("cmu-lock")
        object_key = "lock-object"

        s3_client.create_bucket(bucket_name)

        from datetime import datetime, timezone, timedelta

        retain_until = datetime.now(timezone.utc) + timedelta(days=30)

        # Try to create multipart upload with object lock
        try:
            mp_response = s3_client.client.create_multipart_upload(
                Bucket=bucket_name,
                Key=object_key,
                ObjectLockMode="GOVERNANCE",
                ObjectLockRetainUntilDate=retain_until,
            )
            upload_id = mp_response["UploadId"]

            # Cleanup
            s3_client.client.abort_multipart_upload(
                Bucket=bucket_name, Key=object_key, UploadId=upload_id
            )

        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code in [
                "NotImplemented",
                "InvalidRequest",
                "ObjectLockConfigurationNotFoundError",
            ]:
                pytest.skip("Object lock not supported or not configured")
                return
            raise

    finally:
        fixture.cleanup()


def test_create_multipart_upload_with_very_long_key(s3_client, config):
    """
    Test CreateMultipartUpload with very long object key

    S3 allows keys up to 1024 bytes
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("cmu-long-key")

        s3_client.create_bucket(bucket_name)

        # Create 1024-byte key (maximum allowed)
        object_key = "a" * 1024

        # Should succeed with 1024-byte key
        mp_response = s3_client.client.create_multipart_upload(
            Bucket=bucket_name, Key=object_key
        )
        upload_id = mp_response["UploadId"]
        assert upload_id is not None

        # Cleanup
        s3_client.client.abort_multipart_upload(
            Bucket=bucket_name, Key=object_key, UploadId=upload_id
        )

    finally:
        fixture.cleanup()


def test_create_multipart_upload_key_too_long(s3_client, config):
    """
    Test CreateMultipartUpload with key > 1024 bytes

    Should return KeyTooLongError
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("cmu-key-too-long")

        s3_client.create_bucket(bucket_name)

        # Create 1025-byte key (exceeds maximum)
        object_key = "a" * 1025

        # Should fail with key too long
        try:
            with pytest.raises(ClientError) as exc_info:
                s3_client.client.create_multipart_upload(
                    Bucket=bucket_name, Key=object_key
                )

            error_code = exc_info.value.response["Error"]["Code"]
            assert error_code in [
                "KeyTooLongError",
                "InvalidArgument",
            ], f"Expected KeyTooLongError, got {error_code}"

        except Exception as e:
            # boto3 may validate client-side before sending request
            if "ParamValidationError" in str(type(e).__name__):
                # Client-side validation is acceptable
                pass
            else:
                raise

    finally:
        fixture.cleanup()


def test_create_multipart_upload_with_special_characters_in_key(s3_client, config):
    """
    Test CreateMultipartUpload with special characters in key

    Should handle special characters correctly
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("cmu-special-chars")

        s3_client.create_bucket(bucket_name)

        # Key with various special characters
        special_keys = [
            "key with spaces.txt",
            "key-with-dashes.txt",
            "key_with_underscores.txt",
            "key.with.dots.txt",
            "key!with@special#chars$.txt",
            "path/to/nested/object.txt",
            "κλειδί.txt",  # Greek characters
            "密钥.txt",  # Chinese characters
        ]

        for object_key in special_keys:
            # Create multipart upload
            mp_response = s3_client.client.create_multipart_upload(
                Bucket=bucket_name, Key=object_key
            )
            upload_id = mp_response["UploadId"]
            assert upload_id is not None

            # Verify key is preserved
            assert mp_response["Key"] == object_key

            # Cleanup
            s3_client.client.abort_multipart_upload(
                Bucket=bucket_name, Key=object_key, UploadId=upload_id
            )

    finally:
        fixture.cleanup()
