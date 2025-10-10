#!/usr/bin/env python3
"""
S3 CompleteMultipartUpload Tests

Tests CompleteMultipartUpload API edge cases and validations:
- Part number validation
- ETag validation
- Parts ordering
- Upload size validation
- Empty parts handling
- Success scenarios

Ported from versitygw integration tests.

Generated-by: Claude AI
Signed-off-by: Luis Chamberlain <mcgrof@kernel.org>
"""

import pytest
import sys
import os
import hashlib

# Add parent directory to path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from tests.common.fixtures import TestFixture
from botocore.exceptions import ClientError


def upload_parts_helper(s3_client, bucket, key, upload_id, part_size, num_parts):
    """Helper to upload multiple parts and return part info"""
    parts = []
    all_data = b""

    for part_num in range(1, num_parts + 1):
        part_data = bytes([part_num % 256]) * part_size
        all_data += part_data

        response = s3_client.client.upload_part(
            Bucket=bucket,
            Key=key,
            UploadId=upload_id,
            PartNumber=part_num,
            Body=part_data,
        )

        parts.append({"PartNumber": part_num, "ETag": response["ETag"]})

    # Calculate SHA256 of complete data
    checksum = hashlib.sha256(all_data).hexdigest()

    return parts, checksum


def test_complete_multipart_upload_incorrect_part_number(s3_client, config):
    """
    Test CompleteMultipartUpload with wrong part number

    Upload part 1 but try to complete with part 5
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("complete-wrong-part")
        s3_client.create_bucket(bucket_name)

        key = "my-obj"
        upload_id = s3_client.create_multipart_upload(bucket_name, key)

        # Upload part 1
        part_data = b"a" * (5 * 1024 * 1024)
        response = s3_client.client.upload_part(
            Bucket=bucket_name,
            Key=key,
            UploadId=upload_id,
            PartNumber=1,
            Body=part_data,
        )

        # Try to complete with part number 5 (but we uploaded part 1)
        with pytest.raises(ClientError) as exc_info:
            s3_client.client.complete_multipart_upload(
                Bucket=bucket_name,
                Key=key,
                UploadId=upload_id,
                MultipartUpload={
                    "Parts": [{"PartNumber": 5, "ETag": response["ETag"]}]
                },
            )

        error_code = exc_info.value.response["Error"]["Code"]
        assert error_code == "InvalidPart", f"Expected InvalidPart, got {error_code}"

    finally:
        fixture.cleanup()


def test_complete_multipart_upload_invalid_etag(s3_client, config):
    """
    Test CompleteMultipartUpload with invalid ETag

    Should return InvalidPart error
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("complete-bad-etag")
        s3_client.create_bucket(bucket_name)

        key = "my-obj"
        upload_id = s3_client.create_multipart_upload(bucket_name, key)

        # Upload part 1
        part_data = b"a" * (5 * 1024 * 1024)
        s3_client.client.upload_part(
            Bucket=bucket_name,
            Key=key,
            UploadId=upload_id,
            PartNumber=1,
            Body=part_data,
        )

        # Try to complete with invalid ETag
        with pytest.raises(ClientError) as exc_info:
            s3_client.client.complete_multipart_upload(
                Bucket=bucket_name,
                Key=key,
                UploadId=upload_id,
                MultipartUpload={"Parts": [{"PartNumber": 1, "ETag": "invalidETag"}]},
            )

        error_code = exc_info.value.response["Error"]["Code"]
        assert error_code == "InvalidPart", f"Expected InvalidPart, got {error_code}"

    finally:
        fixture.cleanup()


def test_complete_multipart_upload_small_upload_size(s3_client, config):
    """
    Test CompleteMultipartUpload with parts smaller than 5MB

    Should return EntityTooSmall error
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("complete-small")
        s3_client.create_bucket(bucket_name)

        key = "my-obj"
        upload_id = s3_client.create_multipart_upload(bucket_name, key)

        # Upload 4 parts of 1KB each (total 4KB < 5MB minimum)
        parts = []
        for part_num in range(1, 5):
            part_data = b"x" * 1024
            response = s3_client.client.upload_part(
                Bucket=bucket_name,
                Key=key,
                UploadId=upload_id,
                PartNumber=part_num,
                Body=part_data,
            )
            parts.append({"PartNumber": part_num, "ETag": response["ETag"]})

        # Try to complete with undersized parts
        with pytest.raises(ClientError) as exc_info:
            s3_client.client.complete_multipart_upload(
                Bucket=bucket_name,
                Key=key,
                UploadId=upload_id,
                MultipartUpload={"Parts": parts},
            )

        error_code = exc_info.value.response["Error"]["Code"]
        assert (
            error_code == "EntityTooSmall"
        ), f"Expected EntityTooSmall, got {error_code}"

    finally:
        fixture.cleanup()


def test_complete_multipart_upload_empty_parts(s3_client, config):
    """
    Test CompleteMultipartUpload with empty parts list

    Should return MalformedXML error
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("complete-empty")
        s3_client.create_bucket(bucket_name)

        key = "my-obj"
        upload_id = s3_client.create_multipart_upload(bucket_name, key)

        # Upload one part
        part_data = b"a" * (5 * 1024 * 1024)
        s3_client.client.upload_part(
            Bucket=bucket_name,
            Key=key,
            UploadId=upload_id,
            PartNumber=1,
            Body=part_data,
        )

        # Try to complete with empty parts list
        with pytest.raises(ClientError) as exc_info:
            s3_client.client.complete_multipart_upload(
                Bucket=bucket_name,
                Key=key,
                UploadId=upload_id,
                MultipartUpload={"Parts": []},
            )

        error_code = exc_info.value.response["Error"]["Code"]
        assert error_code in [
            "MalformedXML",
            "InvalidRequest",
        ], f"Expected MalformedXML/InvalidRequest, got {error_code}"

    finally:
        fixture.cleanup()


def test_complete_multipart_upload_incorrect_parts_order(s3_client, config):
    """
    Test CompleteMultipartUpload with parts in wrong order

    Parts must be in ascending order by part number
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("complete-wrong-order")
        s3_client.create_bucket(bucket_name)

        key = "my-obj"
        upload_id = s3_client.create_multipart_upload(bucket_name, key)

        # Upload 3 parts
        parts, _ = upload_parts_helper(
            s3_client, bucket_name, key, upload_id, 15 * 1024 * 1024, 3  # 15MB total
        )

        # Swap parts 0 and 1 (part numbers 1 and 2)
        parts[0], parts[1] = parts[1], parts[0]

        # Try to complete with parts in wrong order
        with pytest.raises(ClientError) as exc_info:
            s3_client.client.complete_multipart_upload(
                Bucket=bucket_name,
                Key=key,
                UploadId=upload_id,
                MultipartUpload={"Parts": parts},
            )

        error_code = exc_info.value.response["Error"]["Code"]
        assert (
            error_code == "InvalidPartOrder"
        ), f"Expected InvalidPartOrder, got {error_code}"

    finally:
        fixture.cleanup()


def test_complete_multipart_upload_invalid_part_number_negative(s3_client, config):
    """
    Test CompleteMultipartUpload with negative part number

    Part numbers must be positive
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("complete-neg-part")
        s3_client.create_bucket(bucket_name)

        key = "my-obj"
        upload_id = s3_client.create_multipart_upload(bucket_name, key)

        # Upload part 1
        part_data = b"a" * (5 * 1024 * 1024)
        response = s3_client.client.upload_part(
            Bucket=bucket_name,
            Key=key,
            UploadId=upload_id,
            PartNumber=1,
            Body=part_data,
        )

        # Try to complete with negative part number
        with pytest.raises(ClientError) as exc_info:
            s3_client.client.complete_multipart_upload(
                Bucket=bucket_name,
                Key=key,
                UploadId=upload_id,
                MultipartUpload={
                    "Parts": [{"PartNumber": -4, "ETag": response["ETag"]}]
                },
            )

        error_code = exc_info.value.response["Error"]["Code"]
        # Different implementations may return different error codes
        assert error_code in [
            "InvalidArgument",
            "InvalidPart",
        ], f"Expected InvalidArgument/InvalidPart, got {error_code}"

    finally:
        fixture.cleanup()


def test_complete_multipart_upload_success(s3_client, config):
    """
    Test successful CompleteMultipartUpload

    Should create object with correct size and content
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("complete-success")
        s3_client.create_bucket(bucket_name)

        key = "my-obj"
        upload_id = s3_client.create_multipart_upload(bucket_name, key)

        # Upload 5 parts of 5MB each (25MB total)
        obj_size = 5 * 1024 * 1024
        parts, expected_checksum = upload_parts_helper(
            s3_client, bucket_name, key, upload_id, obj_size, 5
        )

        # Complete the upload
        complete_response = s3_client.client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={"Parts": parts},
        )

        # Verify response
        assert complete_response["Key"] == key
        assert "ETag" in complete_response

        # Verify object via HeadObject
        head_response = s3_client.client.head_object(Bucket=bucket_name, Key=key)

        assert head_response["ETag"] == complete_response["ETag"]
        assert head_response["ContentLength"] == obj_size * 5

        # Verify object content
        get_response = s3_client.get_object(bucket_name, key)
        body = get_response["Body"].read()

        assert len(body) == obj_size * 5
        actual_checksum = hashlib.sha256(body).hexdigest()
        assert actual_checksum == expected_checksum

    finally:
        fixture.cleanup()


def test_complete_multipart_upload_non_existing_upload_id(s3_client, config):
    """
    Test CompleteMultipartUpload with non-existing upload ID

    Should return NoSuchUpload error
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("complete-no-upload")
        s3_client.create_bucket(bucket_name)

        with pytest.raises(ClientError) as exc_info:
            s3_client.client.complete_multipart_upload(
                Bucket=bucket_name,
                Key="my-obj",
                UploadId="non-existing-upload-id",
                MultipartUpload={"Parts": [{"PartNumber": 1, "ETag": '"fake-etag"'}]},
            )

        error_code = exc_info.value.response["Error"]["Code"]
        assert error_code == "NoSuchUpload", f"Expected NoSuchUpload, got {error_code}"

    finally:
        fixture.cleanup()


def test_complete_multipart_upload_after_abort(s3_client, config):
    """
    Test CompleteMultipartUpload after aborting upload

    Should return NoSuchUpload error
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("complete-after-abort")
        s3_client.create_bucket(bucket_name)

        key = "my-obj"
        upload_id = s3_client.create_multipart_upload(bucket_name, key)

        # Upload a part
        part_data = b"a" * (5 * 1024 * 1024)
        response = s3_client.client.upload_part(
            Bucket=bucket_name,
            Key=key,
            UploadId=upload_id,
            PartNumber=1,
            Body=part_data,
        )

        # Abort the upload
        s3_client.abort_multipart_upload(bucket_name, key, upload_id)

        # Try to complete after abort
        with pytest.raises(ClientError) as exc_info:
            s3_client.client.complete_multipart_upload(
                Bucket=bucket_name,
                Key=key,
                UploadId=upload_id,
                MultipartUpload={
                    "Parts": [{"PartNumber": 1, "ETag": response["ETag"]}]
                },
            )

        error_code = exc_info.value.response["Error"]["Code"]
        assert error_code == "NoSuchUpload", f"Expected NoSuchUpload, got {error_code}"

    finally:
        fixture.cleanup()


def test_complete_multipart_upload_single_part(s3_client, config):
    """
    Test CompleteMultipartUpload with single part

    Single part multipart upload is valid
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("complete-single")
        s3_client.create_bucket(bucket_name)

        key = "single-part-obj"
        upload_id = s3_client.create_multipart_upload(bucket_name, key)

        # Upload single 10MB part
        part_size = 10 * 1024 * 1024
        part_data = b"z" * part_size
        response = s3_client.client.upload_part(
            Bucket=bucket_name,
            Key=key,
            UploadId=upload_id,
            PartNumber=1,
            Body=part_data,
        )

        # Complete with single part
        complete_response = s3_client.client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={"Parts": [{"PartNumber": 1, "ETag": response["ETag"]}]},
        )

        # Verify object exists and has correct size
        assert complete_response["Key"] == key

        head_response = s3_client.client.head_object(Bucket=bucket_name, Key=key)
        assert head_response["ContentLength"] == part_size

    finally:
        fixture.cleanup()
