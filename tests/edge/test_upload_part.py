#!/usr/bin/env python3
"""
S3 UploadPart Tests

Tests UploadPart API edge cases and error conditions:
- Part number validation
- Upload ID validation
- Error conditions
- Success responses

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


def test_upload_part_non_existing_bucket(s3_client, config):
    """
    Test UploadPart on non-existing bucket

    Should return NoSuchBucket error
    """
    fixture = TestFixture(s3_client, config)

    try:
        with pytest.raises(ClientError) as exc_info:
            s3_client.client.upload_part(
                Bucket="non-existing-bucket-12345",
                Key="my-obj",
                UploadId="fake-upload-id",
                PartNumber=1,
                Body=b"test data",
            )

        error_code = exc_info.value.response["Error"]["Code"]
        assert error_code == "NoSuchBucket", f"Expected NoSuchBucket, got {error_code}"

    finally:
        fixture.cleanup()


def test_upload_part_invalid_part_number(s3_client, config):
    """
    Test UploadPart with invalid part numbers

    Part numbers must be 1-10000
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("upload-part-num")
        s3_client.create_bucket(bucket_name)

        key = "my-obj"
        upload_id = s3_client.create_multipart_upload(bucket_name, key)

        # Invalid part numbers
        invalid_part_numbers = [0, -1, 10001, 2300000]

        for part_num in invalid_part_numbers:
            with pytest.raises(ClientError) as exc_info:
                s3_client.client.upload_part(
                    Bucket=bucket_name,
                    Key=key,
                    UploadId=upload_id,
                    PartNumber=part_num,
                    Body=b"test data",
                )

            error_code = exc_info.value.response["Error"]["Code"]
            assert error_code in [
                "InvalidArgument",
                "InvalidPartNumber",
                "InvalidPart",
            ], f"Expected InvalidArgument/InvalidPartNumber/InvalidPart for {part_num}, got {error_code}"

    finally:
        fixture.cleanup()


def test_upload_part_non_existing_mp_upload(s3_client, config):
    """
    Test UploadPart with non-existing upload ID

    Should return NoSuchUpload error
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("upload-no-mp")
        s3_client.create_bucket(bucket_name)

        with pytest.raises(ClientError) as exc_info:
            s3_client.client.upload_part(
                Bucket=bucket_name,
                Key="my-obj",
                UploadId="non-existing-upload-id",
                PartNumber=1,
                Body=b"test data",
            )

        error_code = exc_info.value.response["Error"]["Code"]
        assert error_code == "NoSuchUpload", f"Expected NoSuchUpload, got {error_code}"

    finally:
        fixture.cleanup()


def test_upload_part_non_existing_key(s3_client, config):
    """
    Test UploadPart with wrong object key

    Upload ID is tied to specific key
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("upload-wrong-key")
        s3_client.create_bucket(bucket_name)

        key = "my-obj"
        upload_id = s3_client.create_multipart_upload(bucket_name, key)

        # Try to upload part with different key
        with pytest.raises(ClientError) as exc_info:
            s3_client.client.upload_part(
                Bucket=bucket_name,
                Key="non-existing-object-key",
                UploadId=upload_id,
                PartNumber=1,
                Body=b"test data",
            )

        error_code = exc_info.value.response["Error"]["Code"]
        assert error_code == "NoSuchUpload", f"Expected NoSuchUpload, got {error_code}"

    finally:
        fixture.cleanup()


def test_upload_part_success(s3_client, config):
    """
    Test UploadPart success

    Should return valid ETag
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("upload-success")
        s3_client.create_bucket(bucket_name)

        key = "my-obj"
        upload_id = s3_client.create_multipart_upload(bucket_name, key)

        # Upload a part
        part_data = b"a" * (5 * 1024 * 1024)  # 5MB minimum
        upload_response = s3_client.client.upload_part(
            Bucket=bucket_name,
            Key=key,
            UploadId=upload_id,
            PartNumber=1,
            Body=part_data,
        )

        # Verify response
        assert "ETag" in upload_response
        assert upload_response["ETag"] != ""

    finally:
        fixture.cleanup()


def test_upload_part_multiple_parts(s3_client, config):
    """
    Test uploading multiple parts

    Parts can be uploaded in any order
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("upload-multi")
        s3_client.create_bucket(bucket_name)

        key = "multi-part-obj"
        upload_id = s3_client.create_multipart_upload(bucket_name, key)

        part_data = b"x" * (5 * 1024 * 1024)  # 5MB per part

        # Upload parts in non-sequential order
        etags = {}
        for part_num in [3, 1, 2]:
            response = s3_client.client.upload_part(
                Bucket=bucket_name,
                Key=key,
                UploadId=upload_id,
                PartNumber=part_num,
                Body=part_data,
            )
            etags[part_num] = response["ETag"]

        # List parts to verify all uploaded
        list_response = s3_client.client.list_parts(
            Bucket=bucket_name, Key=key, UploadId=upload_id
        )

        parts = list_response.get("Parts", [])
        assert len(parts) == 3

        # Verify parts are listed in order
        part_numbers = [p["PartNumber"] for p in parts]
        assert part_numbers == [1, 2, 3]

    finally:
        fixture.cleanup()


def test_upload_part_overwrite_part(s3_client, config):
    """
    Test uploading same part number twice

    Later upload should overwrite earlier one
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("upload-overwrite")
        s3_client.create_bucket(bucket_name)

        key = "overwrite-part-obj"
        upload_id = s3_client.create_multipart_upload(bucket_name, key)

        part_num = 1
        part_size = 5 * 1024 * 1024

        # Upload part first time
        data1 = b"a" * part_size
        response1 = s3_client.client.upload_part(
            Bucket=bucket_name,
            Key=key,
            UploadId=upload_id,
            PartNumber=part_num,
            Body=data1,
        )
        etag1 = response1["ETag"]

        # Upload same part again with different data
        data2 = b"b" * part_size
        response2 = s3_client.client.upload_part(
            Bucket=bucket_name,
            Key=key,
            UploadId=upload_id,
            PartNumber=part_num,
            Body=data2,
        )
        etag2 = response2["ETag"]

        # ETags should be different
        assert etag1 != etag2

        # List parts - should only show one part
        list_response = s3_client.client.list_parts(
            Bucket=bucket_name, Key=key, UploadId=upload_id
        )

        parts = list_response.get("Parts", [])
        assert len(parts) == 1
        assert parts[0]["PartNumber"] == part_num
        assert parts[0]["ETag"] == etag2  # Should have latest ETag

    finally:
        fixture.cleanup()


def test_upload_part_empty_body(s3_client, config):
    """
    Test UploadPart with empty body

    Empty parts may be rejected or accepted depending on implementation
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("upload-empty")
        s3_client.create_bucket(bucket_name)

        key = "empty-part-obj"
        upload_id = s3_client.create_multipart_upload(bucket_name, key)

        # Try to upload empty part
        try:
            response = s3_client.client.upload_part(
                Bucket=bucket_name, Key=key, UploadId=upload_id, PartNumber=1, Body=b""
            )
            # If accepted, should have ETag
            assert "ETag" in response
        except ClientError as e:
            # Some implementations may reject empty parts
            error_code = e.response["Error"]["Code"]
            # Accept either behavior
            assert error_code in ["EntityTooSmall", "InvalidRequest"]

    finally:
        fixture.cleanup()


def test_upload_part_response_metadata(s3_client, config):
    """
    Test UploadPart response contains expected metadata

    Response should include ETag and server metadata
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("upload-meta")
        s3_client.create_bucket(bucket_name)

        key = "metadata-obj"
        upload_id = s3_client.create_multipart_upload(bucket_name, key)

        part_data = b"z" * (5 * 1024 * 1024)
        response = s3_client.client.upload_part(
            Bucket=bucket_name,
            Key=key,
            UploadId=upload_id,
            PartNumber=1,
            Body=part_data,
        )

        # Verify response structure
        assert "ETag" in response
        assert "ResponseMetadata" in response
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    finally:
        fixture.cleanup()


def test_upload_part_after_abort(s3_client, config):
    """
    Test UploadPart after aborting upload

    Should return NoSuchUpload error
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("upload-after-abort")
        s3_client.create_bucket(bucket_name)

        key = "aborted-obj"
        upload_id = s3_client.create_multipart_upload(bucket_name, key)

        # Abort the upload
        s3_client.abort_multipart_upload(bucket_name, key, upload_id)

        # Try to upload part after abort
        with pytest.raises(ClientError) as exc_info:
            s3_client.client.upload_part(
                Bucket=bucket_name,
                Key=key,
                UploadId=upload_id,
                PartNumber=1,
                Body=b"a" * (5 * 1024 * 1024),
            )

        error_code = exc_info.value.response["Error"]["Code"]
        assert error_code == "NoSuchUpload", f"Expected NoSuchUpload, got {error_code}"

    finally:
        fixture.cleanup()
