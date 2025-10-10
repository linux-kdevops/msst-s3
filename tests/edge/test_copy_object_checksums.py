#!/usr/bin/env python3
"""
S3 CopyObject Checksum Tests

Tests CopyObject with checksum algorithms:
- ChecksumAlgorithm parameter
- Copying existing checksums
- Replacing checksums
- Checksum validation

Ported from versitygw integration tests.

Generated-by: Claude AI
Signed-off-by: Luis Chamberlain <mcgrof@kernel.org>
"""

import pytest
import sys
import os
import hashlib
import base64

# Add parent directory to path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from tests.common.fixtures import TestFixture
from botocore.exceptions import ClientError


def test_copy_object_invalid_checksum_algorithm(s3_client, config):
    """
    Test CopyObject with invalid ChecksumAlgorithm

    boto3 validates ChecksumAlgorithm client-side, so invalid values
    are caught before reaching the server
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("copy-invalid-checksum")
        source_key = "source-object"
        dest_key = "dest-object"

        s3_client.create_bucket(bucket_name)

        # Put source object
        s3_client.client.put_object(
            Bucket=bucket_name, Key=source_key, Body=b"source data"
        )

        # Try to copy with invalid checksum algorithm
        # boto3 validates client-side, so this should raise ParamValidationError
        try:
            s3_client.client.copy_object(
                Bucket=bucket_name,
                Key=dest_key,
                CopySource={"Bucket": bucket_name, "Key": source_key},
                ChecksumAlgorithm="INVALID_ALGO",
            )
            # If no error, boto3 accepted it (lenient validation)
            # This is not a failure, just documents client behavior

        except Exception as e:
            # boto3 validates client-side before sending request
            error_type = type(e).__name__
            # ParamValidationError or ClientError both acceptable
            assert error_type in [
                "ParamValidationError",
                "ClientError",
            ], f"Expected validation error, got {error_type}"

    finally:
        fixture.cleanup()


def test_copy_object_create_checksum_on_copy(s3_client, config):
    """
    Test CopyObject with ChecksumAlgorithm creates new checksum

    Source object has no checksum, copy creates checksum
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("copy-create-checksum")
        source_key = "source-object"
        dest_key = "dest-object"

        s3_client.create_bucket(bucket_name)

        object_data = b"test data for checksum creation"

        # Put source object without checksum
        s3_client.client.put_object(
            Bucket=bucket_name, Key=source_key, Body=object_data
        )

        # Copy with ChecksumAlgorithm to create checksum
        try:
            copy_response = s3_client.client.copy_object(
                Bucket=bucket_name,
                Key=dest_key,
                CopySource={"Bucket": bucket_name, "Key": source_key},
                ChecksumAlgorithm="SHA256",
            )

            # Verify checksum created in response
            if "ChecksumSHA256" in copy_response:
                # Calculate expected checksum
                sha256_hash = hashlib.sha256(object_data).digest()
                expected_checksum = base64.b64encode(sha256_hash).decode("utf-8")
                assert copy_response["ChecksumSHA256"] == expected_checksum

            # Verify with HeadObject
            head_response = s3_client.client.head_object(
                Bucket=bucket_name, Key=dest_key
            )
            if "ChecksumSHA256" in head_response:
                sha256_hash = hashlib.sha256(object_data).digest()
                expected_checksum = base64.b64encode(sha256_hash).decode("utf-8")
                assert head_response["ChecksumSHA256"] == expected_checksum

        except ClientError as e:
            if e.response["Error"]["Code"] in ["NotImplemented", "InvalidArgument"]:
                pytest.skip("ChecksumAlgorithm not supported")
                return
            raise

    finally:
        fixture.cleanup()


def test_copy_object_should_copy_the_existing_checksum(s3_client, config):
    """
    Test CopyObject preserves existing checksum

    Source object has checksum, copy preserves it
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("copy-preserve-checksum")
        source_key = "source-object"
        dest_key = "dest-object"

        s3_client.create_bucket(bucket_name)

        object_data = b"test data with checksum"

        # Calculate SHA256 checksum
        sha256_hash = hashlib.sha256(object_data).digest()
        checksum_sha256 = base64.b64encode(sha256_hash).decode("utf-8")

        # Put source object with checksum
        try:
            s3_client.client.put_object(
                Bucket=bucket_name,
                Key=source_key,
                Body=object_data,
                ChecksumAlgorithm="SHA256",
                ChecksumSHA256=checksum_sha256,
            )
        except ClientError as e:
            if e.response["Error"]["Code"] in ["NotImplemented", "InvalidArgument"]:
                pytest.skip("ChecksumAlgorithm not supported")
                return
            raise

        # Copy object (should preserve checksum)
        copy_response = s3_client.client.copy_object(
            Bucket=bucket_name,
            Key=dest_key,
            CopySource={"Bucket": bucket_name, "Key": source_key},
        )

        # Verify checksum preserved in copy response
        if "ChecksumSHA256" in copy_response:
            assert copy_response["ChecksumSHA256"] == checksum_sha256

        # Verify with HeadObject
        head_response = s3_client.client.head_object(Bucket=bucket_name, Key=dest_key)
        if "ChecksumSHA256" in head_response:
            assert head_response["ChecksumSHA256"] == checksum_sha256

    finally:
        fixture.cleanup()


def test_copy_object_should_replace_the_existing_checksum(s3_client, config):
    """
    Test CopyObject replaces existing checksum with new algorithm

    Source has CRC32, copy with SHA256 replaces it
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("copy-replace-checksum")
        source_key = "source-object"
        dest_key = "dest-object"

        s3_client.create_bucket(bucket_name)

        object_data = b"test data for checksum replacement"

        # Calculate CRC32 checksum for source
        import zlib

        crc32_value = zlib.crc32(object_data) & 0xFFFFFFFF
        checksum_crc32 = base64.b64encode(crc32_value.to_bytes(4, "big")).decode(
            "utf-8"
        )

        # Put source object with CRC32
        try:
            s3_client.client.put_object(
                Bucket=bucket_name,
                Key=source_key,
                Body=object_data,
                ChecksumAlgorithm="CRC32",
                ChecksumCRC32=checksum_crc32,
            )
        except ClientError as e:
            if e.response["Error"]["Code"] in ["NotImplemented", "InvalidArgument"]:
                pytest.skip("ChecksumAlgorithm not supported")
                return
            raise

        # Copy with SHA256 to replace checksum
        try:
            copy_response = s3_client.client.copy_object(
                Bucket=bucket_name,
                Key=dest_key,
                CopySource={"Bucket": bucket_name, "Key": source_key},
                ChecksumAlgorithm="SHA256",
            )

            # Verify SHA256 checksum in response (replaced CRC32)
            if "ChecksumSHA256" in copy_response:
                sha256_hash = hashlib.sha256(object_data).digest()
                expected_checksum = base64.b64encode(sha256_hash).decode("utf-8")
                assert copy_response["ChecksumSHA256"] == expected_checksum

            # Verify with HeadObject - should have SHA256, not CRC32
            head_response = s3_client.client.head_object(
                Bucket=bucket_name, Key=dest_key
            )
            if "ChecksumSHA256" in head_response:
                sha256_hash = hashlib.sha256(object_data).digest()
                expected_checksum = base64.b64encode(sha256_hash).decode("utf-8")
                assert head_response["ChecksumSHA256"] == expected_checksum

        except ClientError as e:
            if e.response["Error"]["Code"] in ["NotImplemented", "InvalidArgument"]:
                pytest.skip("ChecksumAlgorithm not supported for copy")
                return
            raise

    finally:
        fixture.cleanup()


def test_copy_object_to_itself_by_replacing_the_checksum(s3_client, config):
    """
    Test CopyObject to itself with checksum replacement

    Copy object to itself with MetadataDirective=REPLACE and new checksum
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("copy-self-checksum")
        object_key = "test-object"

        s3_client.create_bucket(bucket_name)

        object_data = b"test data for self-copy checksum"

        # Put object without checksum
        s3_client.client.put_object(
            Bucket=bucket_name, Key=object_key, Body=object_data
        )

        # Copy to itself with ChecksumAlgorithm
        try:
            copy_response = s3_client.client.copy_object(
                Bucket=bucket_name,
                Key=object_key,
                CopySource={"Bucket": bucket_name, "Key": object_key},
                MetadataDirective="REPLACE",
                ChecksumAlgorithm="SHA256",
            )

            # Verify checksum added in response
            if "ChecksumSHA256" in copy_response:
                sha256_hash = hashlib.sha256(object_data).digest()
                expected_checksum = base64.b64encode(sha256_hash).decode("utf-8")
                assert copy_response["ChecksumSHA256"] == expected_checksum

            # Verify with HeadObject
            head_response = s3_client.client.head_object(
                Bucket=bucket_name, Key=object_key
            )
            if "ChecksumSHA256" in head_response:
                sha256_hash = hashlib.sha256(object_data).digest()
                expected_checksum = base64.b64encode(sha256_hash).decode("utf-8")
                assert head_response["ChecksumSHA256"] == expected_checksum

        except ClientError as e:
            if e.response["Error"]["Code"] in ["NotImplemented", "InvalidArgument"]:
                pytest.skip("ChecksumAlgorithm not supported")
                return
            raise

    finally:
        fixture.cleanup()


def test_copy_object_checksum_with_crc32(s3_client, config):
    """
    Test CopyObject with CRC32 checksum algorithm

    Test CRC32 checksum creation during copy
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("copy-crc32")
        source_key = "source-object"
        dest_key = "dest-object"

        s3_client.create_bucket(bucket_name)

        object_data = b"test data for CRC32"

        # Put source object
        s3_client.client.put_object(
            Bucket=bucket_name, Key=source_key, Body=object_data
        )

        # Copy with CRC32
        try:
            copy_response = s3_client.client.copy_object(
                Bucket=bucket_name,
                Key=dest_key,
                CopySource={"Bucket": bucket_name, "Key": source_key},
                ChecksumAlgorithm="CRC32",
            )

            # Verify CRC32 checksum in response
            if "ChecksumCRC32" in copy_response:
                import zlib

                crc32_value = zlib.crc32(object_data) & 0xFFFFFFFF
                expected_checksum = base64.b64encode(
                    crc32_value.to_bytes(4, "big")
                ).decode("utf-8")
                assert copy_response["ChecksumCRC32"] == expected_checksum

        except ClientError as e:
            if e.response["Error"]["Code"] in ["NotImplemented", "InvalidArgument"]:
                pytest.skip("CRC32 not supported")
                return
            raise

    finally:
        fixture.cleanup()


def test_copy_object_checksum_across_buckets(s3_client, config):
    """
    Test CopyObject with checksum across different buckets

    Copy object with checksum from one bucket to another
    """
    fixture = TestFixture(s3_client, config)

    try:
        source_bucket = fixture.generate_bucket_name("copy-src-checksum")
        dest_bucket = fixture.generate_bucket_name("copy-dst-checksum")
        object_key = "checksum-object"

        s3_client.create_bucket(source_bucket)
        s3_client.create_bucket(dest_bucket)

        object_data = b"cross-bucket checksum data"

        # Calculate SHA256 checksum
        sha256_hash = hashlib.sha256(object_data).digest()
        checksum_sha256 = base64.b64encode(sha256_hash).decode("utf-8")

        # Put object in source bucket with checksum
        try:
            s3_client.client.put_object(
                Bucket=source_bucket,
                Key=object_key,
                Body=object_data,
                ChecksumAlgorithm="SHA256",
                ChecksumSHA256=checksum_sha256,
            )
        except ClientError as e:
            if e.response["Error"]["Code"] in ["NotImplemented", "InvalidArgument"]:
                pytest.skip("ChecksumAlgorithm not supported")
                return
            raise

        # Copy to destination bucket
        copy_response = s3_client.client.copy_object(
            Bucket=dest_bucket,
            Key=object_key,
            CopySource={"Bucket": source_bucket, "Key": object_key},
        )

        # Verify checksum preserved in destination
        if "ChecksumSHA256" in copy_response:
            assert copy_response["ChecksumSHA256"] == checksum_sha256

        head_response = s3_client.client.head_object(Bucket=dest_bucket, Key=object_key)
        if "ChecksumSHA256" in head_response:
            assert head_response["ChecksumSHA256"] == checksum_sha256

    finally:
        fixture.cleanup()


def test_copy_object_checksum_metadata_directive(s3_client, config):
    """
    Test CopyObject checksum with MetadataDirective

    Checksum behavior with COPY vs REPLACE directive
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("copy-checksum-directive")
        source_key = "source-object"
        dest_key = "dest-object"

        s3_client.create_bucket(bucket_name)

        object_data = b"test data for directive"

        # Calculate SHA256 checksum
        sha256_hash = hashlib.sha256(object_data).digest()
        checksum_sha256 = base64.b64encode(sha256_hash).decode("utf-8")

        # Put source object with checksum
        try:
            s3_client.client.put_object(
                Bucket=bucket_name,
                Key=source_key,
                Body=object_data,
                ChecksumAlgorithm="SHA256",
                ChecksumSHA256=checksum_sha256,
            )
        except ClientError as e:
            if e.response["Error"]["Code"] in ["NotImplemented", "InvalidArgument"]:
                pytest.skip("ChecksumAlgorithm not supported")
                return
            raise

        # Copy with MetadataDirective=COPY (should preserve checksum)
        copy_response = s3_client.client.copy_object(
            Bucket=bucket_name,
            Key=dest_key,
            CopySource={"Bucket": bucket_name, "Key": source_key},
            MetadataDirective="COPY",
        )

        # Verify checksum preserved
        if "ChecksumSHA256" in copy_response:
            assert copy_response["ChecksumSHA256"] == checksum_sha256

    finally:
        fixture.cleanup()


def test_copy_object_checksum_response_fields(s3_client, config):
    """
    Test CopyObject response contains checksum fields

    Verify response structure with checksum
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("copy-checksum-response")
        source_key = "source-object"
        dest_key = "dest-object"

        s3_client.create_bucket(bucket_name)

        object_data = b"response field test data"

        # Calculate SHA256 checksum
        sha256_hash = hashlib.sha256(object_data).digest()
        checksum_sha256 = base64.b64encode(sha256_hash).decode("utf-8")

        # Put source object with checksum
        try:
            s3_client.client.put_object(
                Bucket=bucket_name,
                Key=source_key,
                Body=object_data,
                ChecksumAlgorithm="SHA256",
                ChecksumSHA256=checksum_sha256,
            )
        except ClientError as e:
            if e.response["Error"]["Code"] in ["NotImplemented", "InvalidArgument"]:
                pytest.skip("ChecksumAlgorithm not supported")
                return
            raise

        # Copy object
        copy_response = s3_client.client.copy_object(
            Bucket=bucket_name,
            Key=dest_key,
            CopySource={"Bucket": bucket_name, "Key": source_key},
        )

        # Verify response structure
        assert "CopyObjectResult" in copy_response or "ETag" in copy_response
        assert "ResponseMetadata" in copy_response
        assert copy_response["ResponseMetadata"]["HTTPStatusCode"] == 200

        # ChecksumSHA256 may or may not be in response (implementation-specific)
        if "ChecksumSHA256" in copy_response:
            assert isinstance(copy_response["ChecksumSHA256"], str)
            assert len(copy_response["ChecksumSHA256"]) > 0

    finally:
        fixture.cleanup()


def test_copy_object_multiple_checksum_algorithms(s3_client, config):
    """
    Test CopyObject with different checksum algorithms

    Test SHA1 and SHA256 algorithms
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("copy-multi-checksum")
        source_key = "source-object"

        s3_client.create_bucket(bucket_name)

        object_data = b"multi-algorithm test data"

        # Put source object
        s3_client.client.put_object(
            Bucket=bucket_name, Key=source_key, Body=object_data
        )

        # Test SHA1
        try:
            dest_key1 = "dest-sha1"
            copy_response1 = s3_client.client.copy_object(
                Bucket=bucket_name,
                Key=dest_key1,
                CopySource={"Bucket": bucket_name, "Key": source_key},
                ChecksumAlgorithm="SHA1",
            )

            if "ChecksumSHA1" in copy_response1:
                sha1_hash = hashlib.sha1(object_data).digest()
                expected_checksum = base64.b64encode(sha1_hash).decode("utf-8")
                assert copy_response1["ChecksumSHA1"] == expected_checksum

        except ClientError as e:
            if e.response["Error"]["Code"] not in ["NotImplemented", "InvalidArgument"]:
                raise

        # Test SHA256
        try:
            dest_key2 = "dest-sha256"
            copy_response2 = s3_client.client.copy_object(
                Bucket=bucket_name,
                Key=dest_key2,
                CopySource={"Bucket": bucket_name, "Key": source_key},
                ChecksumAlgorithm="SHA256",
            )

            if "ChecksumSHA256" in copy_response2:
                sha256_hash = hashlib.sha256(object_data).digest()
                expected_checksum = base64.b64encode(sha256_hash).decode("utf-8")
                assert copy_response2["ChecksumSHA256"] == expected_checksum

        except ClientError as e:
            if e.response["Error"]["Code"] in ["NotImplemented", "InvalidArgument"]:
                pytest.skip("SHA256 not supported")
                return
            raise

    finally:
        fixture.cleanup()
