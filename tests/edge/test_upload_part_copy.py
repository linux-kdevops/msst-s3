#!/usr/bin/env python3
"""
S3 UploadPartCopy Tests

Tests UploadPartCopy API for copying data into multipart uploads:
- Source validation (bucket, key, range)
- Upload ID and part number validation
- Byte range copying with CopySourceRange
- Conditional copy operations (If-Match, If-None-Match, etc.)
- Checksum handling and preservation
- Error conditions

Ported from versitygw integration tests.

Generated-by: Claude AI
Signed-off-by: Luis Chamberlain <mcgrof@kernel.org>
"""

import pytest
import sys
import os
from datetime import datetime, timezone, timedelta

# Add parent directory to path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from tests.common.fixtures import TestFixture
from botocore.exceptions import ClientError


def test_upload_part_copy_non_existing_bucket(s3_client, config):
    """
    Test UploadPartCopy on non-existing bucket

    Should return NoSuchBucket error
    """
    fixture = TestFixture(s3_client, config)

    try:
        with pytest.raises(ClientError) as exc_info:
            s3_client.client.upload_part_copy(
                Bucket="non-existing-bucket-12345",
                CopySource="source-bucket/source-key",
                UploadId="fake-upload-id",
                Key="my-obj",
                PartNumber=1,
            )

        error_code = exc_info.value.response["Error"]["Code"]
        assert error_code == "NoSuchBucket", f"Expected NoSuchBucket, got {error_code}"

    finally:
        fixture.cleanup()


def test_upload_part_copy_incorrect_upload_id(s3_client, config):
    """
    Test UploadPartCopy with invalid upload ID

    Should return NoSuchUpload error
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("upc-bad-id")
        s3_client.create_bucket(bucket_name)

        # Create source object
        src_key = "source-obj"
        s3_client.put_object(bucket_name, src_key, b"test data")

        with pytest.raises(ClientError) as exc_info:
            s3_client.client.upload_part_copy(
                Bucket=bucket_name,
                CopySource=f"{bucket_name}/{src_key}",
                UploadId="invalid-upload-id",
                Key="my-obj",
                PartNumber=1,
            )

        error_code = exc_info.value.response["Error"]["Code"]
        assert error_code == "NoSuchUpload", f"Expected NoSuchUpload, got {error_code}"

    finally:
        fixture.cleanup()


def test_upload_part_copy_incorrect_object_key(s3_client, config):
    """
    Test UploadPartCopy with wrong object key

    Upload ID is tied to specific key
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("upc-bad-key")
        s3_client.create_bucket(bucket_name)

        # Create source object
        src_key = "source-obj"
        s3_client.put_object(bucket_name, src_key, b"test data")

        # Create multipart upload for one key
        key = "my-obj"
        upload_id = s3_client.create_multipart_upload(bucket_name, key)

        # Try to upload part copy with different key
        with pytest.raises(ClientError) as exc_info:
            s3_client.client.upload_part_copy(
                Bucket=bucket_name,
                CopySource=f"{bucket_name}/{src_key}",
                UploadId=upload_id,
                Key="different-key",
                PartNumber=1,
            )

        error_code = exc_info.value.response["Error"]["Code"]
        assert error_code == "NoSuchUpload", f"Expected NoSuchUpload, got {error_code}"

    finally:
        fixture.cleanup()


def test_upload_part_copy_invalid_part_number(s3_client, config):
    """
    Test UploadPartCopy with invalid part numbers

    Part numbers must be 1-10000
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("upc-bad-part")
        s3_client.create_bucket(bucket_name)

        # Create source object
        src_key = "source-obj"
        s3_client.put_object(bucket_name, src_key, b"test data" * 100)

        key = "my-obj"
        upload_id = s3_client.create_multipart_upload(bucket_name, key)

        # Invalid part numbers
        invalid_part_numbers = [0, -1, 10001]

        for part_num in invalid_part_numbers:
            with pytest.raises(ClientError) as exc_info:
                s3_client.client.upload_part_copy(
                    Bucket=bucket_name,
                    CopySource=f"{bucket_name}/{src_key}",
                    UploadId=upload_id,
                    Key=key,
                    PartNumber=part_num,
                )

            error_code = exc_info.value.response["Error"]["Code"]
            assert error_code in [
                "InvalidArgument",
                "InvalidPartNumber",
                "InvalidPart",
            ], f"Expected InvalidArgument/InvalidPartNumber/InvalidPart for {part_num}, got {error_code}"

    finally:
        fixture.cleanup()


def test_upload_part_copy_invalid_copy_source(s3_client, config):
    """
    Test UploadPartCopy with invalid CopySource format

    CopySource must be bucket/key format
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("upc-bad-source")
        s3_client.create_bucket(bucket_name)

        key = "my-obj"
        upload_id = s3_client.create_multipart_upload(bucket_name, key)

        # Invalid copy source formats
        invalid_sources = [
            "invalid-format",
            "/invalid",
            "bucket",
            "bucket/",
        ]

        for source in invalid_sources:
            with pytest.raises(ClientError) as exc_info:
                s3_client.client.upload_part_copy(
                    Bucket=bucket_name,
                    CopySource=source,
                    UploadId=upload_id,
                    Key=key,
                    PartNumber=1,
                )

            # Various error codes depending on what's wrong
            error_code = exc_info.value.response["Error"]["Code"]
            assert error_code in [
                "InvalidArgument",
                "NoSuchBucket",
                "NoSuchKey",
                "InvalidRequest",
            ], f"Expected copy source error for '{source}', got {error_code}"

    finally:
        fixture.cleanup()


def test_upload_part_copy_non_existing_source_bucket(s3_client, config):
    """
    Test UploadPartCopy with non-existing source bucket

    Should return NoSuchBucket error
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("upc-no-src-bkt")
        s3_client.create_bucket(bucket_name)

        key = "my-obj"
        upload_id = s3_client.create_multipart_upload(bucket_name, key)

        with pytest.raises(ClientError) as exc_info:
            s3_client.client.upload_part_copy(
                Bucket=bucket_name,
                CopySource="non-existing-source-bucket/source-key",
                UploadId=upload_id,
                Key=key,
                PartNumber=1,
            )

        error_code = exc_info.value.response["Error"]["Code"]
        assert error_code == "NoSuchBucket", f"Expected NoSuchBucket, got {error_code}"

    finally:
        fixture.cleanup()


def test_upload_part_copy_non_existing_source_object_key(s3_client, config):
    """
    Test UploadPartCopy with non-existing source object

    Should return NoSuchKey error
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("upc-no-src-key")
        s3_client.create_bucket(bucket_name)

        key = "my-obj"
        upload_id = s3_client.create_multipart_upload(bucket_name, key)

        with pytest.raises(ClientError) as exc_info:
            s3_client.client.upload_part_copy(
                Bucket=bucket_name,
                CopySource=f"{bucket_name}/non-existing-key",
                UploadId=upload_id,
                Key=key,
                PartNumber=1,
            )

        error_code = exc_info.value.response["Error"]["Code"]
        assert error_code == "NoSuchKey", f"Expected NoSuchKey, got {error_code}"

    finally:
        fixture.cleanup()


def test_upload_part_copy_success(s3_client, config):
    """
    Test successful UploadPartCopy

    Should copy source object data into multipart upload part
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("upc-success")
        src_bucket = fixture.generate_bucket_name("upc-src")
        s3_client.create_bucket(bucket_name)
        s3_client.create_bucket(src_bucket)

        # Create source object (5MB)
        src_key = "source-obj"
        obj_size = 5 * 1024 * 1024
        src_data = b"x" * obj_size
        put_response = s3_client.put_object(src_bucket, src_key, src_data)
        src_etag = put_response["ETag"]

        # Create multipart upload
        key = "my-obj"
        upload_id = s3_client.create_multipart_upload(bucket_name, key)

        # Upload part copy
        copy_response = s3_client.client.upload_part_copy(
            Bucket=bucket_name,
            CopySource=f"{src_bucket}/{src_key}",
            UploadId=upload_id,
            Key=key,
            PartNumber=1,
        )

        # Verify response
        assert "CopyPartResult" in copy_response
        assert "ETag" in copy_response["CopyPartResult"]
        copy_etag = copy_response["CopyPartResult"]["ETag"]

        # List parts to verify
        list_response = s3_client.client.list_parts(
            Bucket=bucket_name, Key=key, UploadId=upload_id
        )

        assert len(list_response["Parts"]) == 1
        part = list_response["Parts"][0]
        assert part["PartNumber"] == 1
        assert part["Size"] == obj_size
        assert part["ETag"] == copy_etag

    finally:
        fixture.cleanup()


def test_upload_part_copy_by_range_invalid_ranges(s3_client, config):
    """
    Test UploadPartCopy with invalid byte ranges

    Invalid ranges should return InvalidArgument error
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("upc-bad-range")
        s3_client.create_bucket(bucket_name)

        # Create source object
        src_key = "source-obj"
        obj_size = 5 * 1024 * 1024
        s3_client.put_object(bucket_name, src_key, b"x" * obj_size)

        key = "my-obj"
        upload_id = s3_client.create_multipart_upload(bucket_name, key)

        # Invalid range formats
        invalid_ranges = [
            "invalid",
            "bytes=invalid",
            "bytes=-",
            "bytes=100-50",  # Start > End
        ]

        for range_header in invalid_ranges:
            with pytest.raises(ClientError) as exc_info:
                s3_client.client.upload_part_copy(
                    Bucket=bucket_name,
                    CopySource=f"{bucket_name}/{src_key}",
                    CopySourceRange=range_header,
                    UploadId=upload_id,
                    Key=key,
                    PartNumber=1,
                )

            error_code = exc_info.value.response["Error"]["Code"]
            assert error_code in [
                "InvalidArgument",
                "InvalidRange",
            ], f"Expected InvalidArgument/InvalidRange for '{range_header}', got {error_code}"

    finally:
        fixture.cleanup()


def test_upload_part_copy_exceeding_copy_source_range(s3_client, config):
    """
    Test UploadPartCopy with range exceeding source object size

    Range beyond object size should be handled gracefully
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("upc-exceed-range")
        s3_client.create_bucket(bucket_name)

        # Create 1KB source object
        src_key = "source-obj"
        obj_size = 1024
        s3_client.put_object(bucket_name, src_key, b"a" * obj_size)

        key = "my-obj"
        upload_id = s3_client.create_multipart_upload(bucket_name, key)

        # Try to copy range beyond object size
        with pytest.raises(ClientError) as exc_info:
            s3_client.client.upload_part_copy(
                Bucket=bucket_name,
                CopySource=f"{bucket_name}/{src_key}",
                CopySourceRange="bytes=0-2000",  # Beyond 1024 bytes
                UploadId=upload_id,
                Key=key,
                PartNumber=1,
            )

        error_code = exc_info.value.response["Error"]["Code"]
        assert error_code in [
            "InvalidArgument",
            "InvalidRange",
        ], f"Expected InvalidArgument/InvalidRange, got {error_code}"

    finally:
        fixture.cleanup()


def test_upload_part_copy_greater_range_than_obj_size(s3_client, config):
    """
    Test UploadPartCopy with start position beyond object size

    Should return InvalidArgument error
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("upc-range-beyond")
        s3_client.create_bucket(bucket_name)

        # Create 1KB source object
        src_key = "source-obj"
        obj_size = 1024
        s3_client.put_object(bucket_name, src_key, b"b" * obj_size)

        key = "my-obj"
        upload_id = s3_client.create_multipart_upload(bucket_name, key)

        # Try to copy range starting beyond object
        with pytest.raises(ClientError) as exc_info:
            s3_client.client.upload_part_copy(
                Bucket=bucket_name,
                CopySource=f"{bucket_name}/{src_key}",
                CopySourceRange="bytes=2000-3000",  # Start beyond 1024 bytes
                UploadId=upload_id,
                Key=key,
                PartNumber=1,
            )

        error_code = exc_info.value.response["Error"]["Code"]
        assert error_code in [
            "InvalidArgument",
            "InvalidRange",
        ], f"Expected InvalidArgument/InvalidRange, got {error_code}"

    finally:
        fixture.cleanup()


def test_upload_part_copy_by_range_success(s3_client, config):
    """
    Test UploadPartCopy with byte range

    Should copy only specified byte range from source
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("upc-range-ok")
        src_bucket = fixture.generate_bucket_name("upc-range-src")
        s3_client.create_bucket(bucket_name)
        s3_client.create_bucket(src_bucket)

        # Create 5MB source object
        src_key = "source-obj"
        obj_size = 5 * 1024 * 1024
        s3_client.put_object(src_bucket, src_key, b"z" * obj_size)

        # Create multipart upload
        key = "my-obj"
        upload_id = s3_client.create_multipart_upload(bucket_name, key)

        # Copy bytes 100-200 (101 bytes total)
        copy_response = s3_client.client.upload_part_copy(
            Bucket=bucket_name,
            CopySource=f"{src_bucket}/{src_key}",
            CopySourceRange="bytes=100-200",
            UploadId=upload_id,
            Key=key,
            PartNumber=1,
        )

        # Verify copy response
        assert "CopyPartResult" in copy_response
        copy_etag = copy_response["CopyPartResult"]["ETag"]

        # List parts to verify size
        list_response = s3_client.client.list_parts(
            Bucket=bucket_name, Key=key, UploadId=upload_id
        )

        assert len(list_response["Parts"]) == 1
        part = list_response["Parts"][0]
        assert part["PartNumber"] == 1
        assert part["Size"] == 101  # bytes 100-200 inclusive
        assert part["ETag"] == copy_etag

    finally:
        fixture.cleanup()


def test_upload_part_copy_conditional_copy_if_match(s3_client, config):
    """
    Test UploadPartCopy with CopySourceIfMatch

    Should succeed when ETag matches, fail otherwise
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("upc-if-match")
        s3_client.create_bucket(bucket_name)

        # Create source object
        src_key = "source-obj"
        put_response = s3_client.put_object(bucket_name, src_key, b"test data")
        src_etag = put_response["ETag"]

        key = "my-obj"
        upload_id = s3_client.create_multipart_upload(bucket_name, key)

        # Success case - matching ETag
        copy_response = s3_client.client.upload_part_copy(
            Bucket=bucket_name,
            CopySource=f"{bucket_name}/{src_key}",
            CopySourceIfMatch=src_etag,
            UploadId=upload_id,
            Key=key,
            PartNumber=1,
        )
        assert "CopyPartResult" in copy_response

        # Failure case - non-matching ETag
        upload_id2 = s3_client.create_multipart_upload(bucket_name, "my-obj-2")
        with pytest.raises(ClientError) as exc_info:
            s3_client.client.upload_part_copy(
                Bucket=bucket_name,
                CopySource=f"{bucket_name}/{src_key}",
                CopySourceIfMatch='"invalid-etag"',
                UploadId=upload_id2,
                Key="my-obj-2",
                PartNumber=1,
            )

        error_code = exc_info.value.response["Error"]["Code"]
        assert (
            error_code == "PreconditionFailed"
        ), f"Expected PreconditionFailed, got {error_code}"

    finally:
        fixture.cleanup()


def test_upload_part_copy_conditional_copy_if_none_match(s3_client, config):
    """
    Test UploadPartCopy with CopySourceIfNoneMatch

    Should fail when ETag matches, succeed otherwise
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("upc-if-none-match")
        s3_client.create_bucket(bucket_name)

        # Create source object
        src_key = "source-obj"
        put_response = s3_client.put_object(bucket_name, src_key, b"test data")
        src_etag = put_response["ETag"]

        key = "my-obj"
        upload_id = s3_client.create_multipart_upload(bucket_name, key)

        # Success case - non-matching ETag
        copy_response = s3_client.client.upload_part_copy(
            Bucket=bucket_name,
            CopySource=f"{bucket_name}/{src_key}",
            CopySourceIfNoneMatch='"invalid-etag"',
            UploadId=upload_id,
            Key=key,
            PartNumber=1,
        )
        assert "CopyPartResult" in copy_response

        # Failure case - matching ETag
        upload_id2 = s3_client.create_multipart_upload(bucket_name, "my-obj-2")
        with pytest.raises(ClientError) as exc_info:
            s3_client.client.upload_part_copy(
                Bucket=bucket_name,
                CopySource=f"{bucket_name}/{src_key}",
                CopySourceIfNoneMatch=src_etag,
                UploadId=upload_id2,
                Key="my-obj-2",
                PartNumber=1,
            )

        error_code = exc_info.value.response["Error"]["Code"]
        # NotModified or PreconditionFailed depending on implementation
        assert error_code in [
            "NotModified",
            "PreconditionFailed",
        ], f"Expected NotModified/PreconditionFailed, got {error_code}"

    finally:
        fixture.cleanup()


def test_upload_part_copy_conditional_copy_if_modified_since(s3_client, config):
    """
    Test UploadPartCopy with CopySourceIfModifiedSince

    Should succeed if modified after specified time
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("upc-if-mod-since")
        s3_client.create_bucket(bucket_name)

        # Create source object
        src_key = "source-obj"
        s3_client.put_object(bucket_name, src_key, b"test data")

        key = "my-obj"
        upload_id = s3_client.create_multipart_upload(bucket_name, key)

        # Success case - date in the past
        past_date = datetime.now(timezone.utc) - timedelta(days=1)
        copy_response = s3_client.client.upload_part_copy(
            Bucket=bucket_name,
            CopySource=f"{bucket_name}/{src_key}",
            CopySourceIfModifiedSince=past_date,
            UploadId=upload_id,
            Key=key,
            PartNumber=1,
        )
        assert "CopyPartResult" in copy_response

        # Failure case - date in the future
        upload_id2 = s3_client.create_multipart_upload(bucket_name, "my-obj-2")
        future_date = datetime.now(timezone.utc) + timedelta(days=1)
        with pytest.raises(ClientError) as exc_info:
            s3_client.client.upload_part_copy(
                Bucket=bucket_name,
                CopySource=f"{bucket_name}/{src_key}",
                CopySourceIfModifiedSince=future_date,
                UploadId=upload_id2,
                Key="my-obj-2",
                PartNumber=1,
            )

        error_code = exc_info.value.response["Error"]["Code"]
        assert error_code in [
            "NotModified",
            "PreconditionFailed",
        ], f"Expected NotModified/PreconditionFailed, got {error_code}"

    finally:
        fixture.cleanup()


def test_upload_part_copy_conditional_copy_if_unmodified_since(s3_client, config):
    """
    Test UploadPartCopy with CopySourceIfUnmodifiedSince

    Should succeed if not modified after specified time
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("upc-if-unmod-since")
        s3_client.create_bucket(bucket_name)

        # Create source object
        src_key = "source-obj"
        s3_client.put_object(bucket_name, src_key, b"test data")

        key = "my-obj"
        upload_id = s3_client.create_multipart_upload(bucket_name, key)

        # Success case - date in the future
        future_date = datetime.now(timezone.utc) + timedelta(days=1)
        copy_response = s3_client.client.upload_part_copy(
            Bucket=bucket_name,
            CopySource=f"{bucket_name}/{src_key}",
            CopySourceIfUnmodifiedSince=future_date,
            UploadId=upload_id,
            Key=key,
            PartNumber=1,
        )
        assert "CopyPartResult" in copy_response

        # Failure case - date in the past
        upload_id2 = s3_client.create_multipart_upload(bucket_name, "my-obj-2")
        past_date = datetime.now(timezone.utc) - timedelta(days=1)
        with pytest.raises(ClientError) as exc_info:
            s3_client.client.upload_part_copy(
                Bucket=bucket_name,
                CopySource=f"{bucket_name}/{src_key}",
                CopySourceIfUnmodifiedSince=past_date,
                UploadId=upload_id2,
                Key="my-obj-2",
                PartNumber=1,
            )

        error_code = exc_info.value.response["Error"]["Code"]
        assert (
            error_code == "PreconditionFailed"
        ), f"Expected PreconditionFailed, got {error_code}"

    finally:
        fixture.cleanup()
