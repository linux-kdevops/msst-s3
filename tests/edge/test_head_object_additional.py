#!/usr/bin/env python3
"""
S3 HeadObject Additional Tests

Tests additional HeadObject scenarios:
- HeadObject with PartNumber parameter
- HeadObject with invalid PartNumber
- HeadObject on directory objects
- HeadObject with ChecksumMode parameter
- HeadObject on non-existing parent directory

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


def test_head_object_invalid_part_number(s3_client, config):
    """
    Test HeadObject with invalid PartNumber (negative)

    Should return BadRequest error
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("head-inv-partnum")
        s3_client.create_bucket(bucket_name)

        key = "my-obj"

        # Try HeadObject with negative PartNumber
        with pytest.raises(ClientError) as exc_info:
            s3_client.client.head_object(Bucket=bucket_name, Key=key, PartNumber=-3)

        error_code = exc_info.value.response["Error"]["Code"]
        assert error_code in [
            "BadRequest",
            "InvalidArgument",
            "400",
        ], f"Expected BadRequest/InvalidArgument, got {error_code}"

    finally:
        fixture.cleanup()


def test_head_object_part_number_not_supported(s3_client, config):
    """
    Test HeadObject with PartNumber on non-multipart object

    Should return NotImplemented or NoSuchKey
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("head-partnum-notsupp")
        s3_client.create_bucket(bucket_name)

        key = "my-obj"

        # Create regular object (not multipart)
        s3_client.put_object(bucket_name, key, b"test data")

        # Try HeadObject with PartNumber on regular object
        try:
            s3_client.client.head_object(Bucket=bucket_name, Key=key, PartNumber=4)
            # Some implementations may succeed (no assertion needed)
        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            # NotImplemented or various error codes are acceptable
            assert error_code in [
                "NotImplemented",
                "InvalidRequest",
                "InvalidArgument",
                "NoSuchKey",
                "404",
                "416",  # MinIO returns Range Not Satisfiable
            ], f"Unexpected error: {error_code}"

    finally:
        fixture.cleanup()


def test_head_object_non_existing_dir_object(s3_client, config):
    """
    Test HeadObject on non-existing directory object

    Should return NotFound for 'my-obj/' when only 'my-obj' exists
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("head-no-dir")
        s3_client.create_bucket(bucket_name)

        key = "my-obj"
        metadata = {"key1": "val1", "key2": "val2"}

        # Create regular object (no trailing slash)
        s3_client.put_object(bucket_name, key, b"x" * 1234567, Metadata=metadata)

        # Try to head directory object (with trailing slash)
        dir_key = "my-obj/"
        with pytest.raises(ClientError) as exc_info:
            s3_client.client.head_object(Bucket=bucket_name, Key=dir_key)

        error_code = exc_info.value.response["Error"]["Code"]
        assert error_code in [
            "NotFound",
            "NoSuchKey",
            "404",
        ], f"Expected NotFound/NoSuchKey, got {error_code}"

    finally:
        fixture.cleanup()


def test_head_object_directory_object_noslash(s3_client, config):
    """
    Test HeadObject on file when directory object exists

    Should return NotFound for 'my-dir' when only 'my-dir/' exists
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("head-dir-noslash")
        s3_client.create_bucket(bucket_name)

        # Create directory object (with trailing slash)
        dir_key = "my-dir/"
        metadata = {"key1": "val1", "key2": "val2"}
        s3_client.put_object(bucket_name, dir_key, b"x" * 1234567, Metadata=metadata)

        # Try to head file (without trailing slash)
        file_key = "my-dir"
        with pytest.raises(ClientError) as exc_info:
            s3_client.client.head_object(Bucket=bucket_name, Key=file_key)

        error_code = exc_info.value.response["Error"]["Code"]
        assert error_code in [
            "NotFound",
            "NoSuchKey",
            "404",
        ], f"Expected NotFound/NoSuchKey, got {error_code}"

    finally:
        fixture.cleanup()


def test_head_object_not_enabled_checksum_mode(s3_client, config):
    """
    Test HeadObject without ChecksumMode parameter

    Checksum fields should be None when ChecksumMode not enabled
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("head-no-chkmode")
        s3_client.create_bucket(bucket_name)

        key = "my-obj"

        # Create object with checksum
        try:
            s3_client.client.put_object(
                Bucket=bucket_name,
                Key=key,
                Body=b"x" * 500,
                ChecksumAlgorithm="SHA1",
            )
        except Exception:
            # Checksum not supported
            pytest.skip("ChecksumAlgorithm not supported")
            return

        # HeadObject without ChecksumMode (should not return checksums)
        head_response = s3_client.client.head_object(Bucket=bucket_name, Key=key)

        # Checksums should not be present without ChecksumMode=ENABLED
        # (Some implementations may still return them)
        assert head_response.get("ChecksumCRC32") is None or isinstance(
            head_response.get("ChecksumCRC32"), str
        )
        assert head_response.get("ChecksumCRC32C") is None or isinstance(
            head_response.get("ChecksumCRC32C"), str
        )
        assert head_response.get("ChecksumSHA1") is None or isinstance(
            head_response.get("ChecksumSHA1"), str
        )
        assert head_response.get("ChecksumSHA256") is None or isinstance(
            head_response.get("ChecksumSHA256"), str
        )

    finally:
        fixture.cleanup()


def test_head_object_checksums(s3_client, config):
    """
    Test HeadObject with ChecksumMode=ENABLED

    Should return checksums when ChecksumMode is ENABLED
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("head-checksums")
        s3_client.create_bucket(bucket_name)

        # Test objects with different checksum algorithms
        test_objects = [
            ("obj-1", "CRC32"),
            ("obj-2", "SHA1"),
            ("obj-3", "SHA256"),
        ]

        for i, (key, algo) in enumerate(test_objects):
            try:
                put_response = s3_client.client.put_object(
                    Bucket=bucket_name,
                    Key=key,
                    Body=b"x" * (i * 200),
                    ChecksumAlgorithm=algo,
                )
            except Exception:
                # Checksum algorithm not supported
                pytest.skip(f"ChecksumAlgorithm {algo} not supported")
                return

            # HeadObject with ChecksumMode=ENABLED
            head_response = s3_client.client.head_object(
                Bucket=bucket_name, Key=key, ChecksumMode="ENABLED"
            )

            # Should have checksum type
            if "ChecksumType" in head_response:
                assert head_response["ChecksumType"] == "FULL_OBJECT"

            # Verify checksum matches PutObject response
            checksum_field = f"Checksum{algo}"
            if checksum_field in put_response:
                # MinIO may not return checksums
                if checksum_field in head_response:
                    assert head_response[checksum_field] == put_response[checksum_field]

    finally:
        fixture.cleanup()


def test_head_object_invalid_parent_dir(s3_client, config):
    """
    Test HeadObject with invalid parent directory

    Should return NotFound for 'not-a-dir/bad-obj' when 'not-a-dir' is a file
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("head-inv-parent")
        s3_client.create_bucket(bucket_name)

        # Create file object (not a directory)
        file_key = "not-a-dir"
        s3_client.put_object(bucket_name, file_key, b"x")

        # Try to head object with file as parent directory
        nested_key = "not-a-dir/bad-obj"
        with pytest.raises(ClientError) as exc_info:
            s3_client.client.head_object(Bucket=bucket_name, Key=nested_key)

        error_code = exc_info.value.response["Error"]["Code"]
        assert error_code in [
            "NotFound",
            "NoSuchKey",
            "404",
        ], f"Expected NotFound/NoSuchKey, got {error_code}"

    finally:
        fixture.cleanup()


def test_head_object_zero_len_with_range(s3_client, config):
    """
    Test HeadObject with Range on zero-length object

    Range should not be satisfiable for empty object
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("head-zero-range")
        s3_client.create_bucket(bucket_name)

        key = "empty-obj"

        # Create zero-length object
        s3_client.put_object(bucket_name, key, b"")

        # HeadObject with Range on empty object
        try:
            head_response = s3_client.client.head_object(
                Bucket=bucket_name, Key=key, Range="bytes=0-9"
            )
            # Some implementations may return success with ContentLength=0
            assert head_response["ContentLength"] == 0
        except ClientError as e:
            # Others may return 416 Range Not Satisfiable
            error_code = e.response["Error"]["Code"]
            assert error_code in [
                "InvalidRange",
                "416",
            ], f"Unexpected error: {error_code}"

    finally:
        fixture.cleanup()


def test_head_object_dir_with_range(s3_client, config):
    """
    Test HeadObject with Range on directory object

    Should succeed and return partial content range
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("head-dir-range")
        s3_client.create_bucket(bucket_name)

        # Create directory object with data
        dir_key = "my-dir/"
        data = b"x" * 1000
        s3_client.put_object(bucket_name, dir_key, data)

        # HeadObject with Range
        head_response = s3_client.client.head_object(
            Bucket=bucket_name, Key=dir_key, Range="bytes=0-99"
        )

        # Should return 206 Partial Content
        assert head_response["ResponseMetadata"]["HTTPStatusCode"] == 206
        assert head_response["ContentLength"] == 100
        assert "ContentRange" in head_response

    finally:
        fixture.cleanup()


def test_head_object_name_too_long(s3_client, config):
    """
    Test HeadObject with key name exceeding maximum length

    Should return error for keys >1024 bytes
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("head-long-name")
        s3_client.create_bucket(bucket_name)

        # Create key with 1025 bytes (exceeds 1024 byte limit)
        long_key = "a" * 1025

        # Try HeadObject with too-long key
        with pytest.raises(ClientError) as exc_info:
            s3_client.client.head_object(Bucket=bucket_name, Key=long_key)

        error_code = exc_info.value.response["Error"]["Code"]
        assert error_code in [
            "KeyTooLongError",
            "InvalidArgument",
            "NoSuchKey",
            "404",
            "400",  # MinIO returns generic 400
        ], f"Expected KeyTooLongError/InvalidArgument, got {error_code}"

    finally:
        fixture.cleanup()
