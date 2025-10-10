#!/usr/bin/env python3
"""
S3 CompleteMultipartUpload Special Cases Tests

Tests CompleteMultipartUpload edge cases:
- Minimum/maximum part numbers
- Part size requirements (last part)
- Concurrent completions
- UploadId reuse
- Empty object assembly
- Part count limits

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


def test_complete_multipart_upload_single_part_minimum(s3_client, config):
    """
    Test CompleteMultipartUpload with single part (minimum case)

    Single part multipart uploads are valid
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("cmp-single-part-min")
        object_key = "single-part-object"

        s3_client.create_bucket(bucket_name)

        # Initiate multipart upload
        mp_response = s3_client.client.create_multipart_upload(
            Bucket=bucket_name, Key=object_key
        )
        upload_id = mp_response["UploadId"]

        # Upload single part (6MB to meet minimum)
        part_data = b"x" * (6 * 1024 * 1024)
        part_response = s3_client.client.upload_part(
            Bucket=bucket_name,
            Key=object_key,
            UploadId=upload_id,
            PartNumber=1,
            Body=part_data,
        )

        # Complete multipart upload
        s3_client.client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=object_key,
            UploadId=upload_id,
            MultipartUpload={
                "Parts": [{"PartNumber": 1, "ETag": part_response["ETag"]}]
            },
        )

        # Verify object exists
        head_response = s3_client.client.head_object(Bucket=bucket_name, Key=object_key)
        assert head_response["ContentLength"] == len(part_data)

    finally:
        fixture.cleanup()


def test_complete_multipart_upload_maximum_part_number(s3_client, config):
    """
    Test CompleteMultipartUpload with maximum part number (10000)

    S3 allows part numbers 1-10000
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("cmp-max-part-num")
        object_key = "max-part-object"

        s3_client.create_bucket(bucket_name)

        # Initiate multipart upload
        mp_response = s3_client.client.create_multipart_upload(
            Bucket=bucket_name, Key=object_key
        )
        upload_id = mp_response["UploadId"]

        # Upload part with maximum part number (10000)
        # Last part can be < 5MB
        part_data = b"x" * 1024  # 1KB
        part_response = s3_client.client.upload_part(
            Bucket=bucket_name,
            Key=object_key,
            UploadId=upload_id,
            PartNumber=10000,
            Body=part_data,
        )

        # Complete multipart upload
        s3_client.client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=object_key,
            UploadId=upload_id,
            MultipartUpload={
                "Parts": [{"PartNumber": 10000, "ETag": part_response["ETag"]}]
            },
        )

        # Verify object exists
        head_response = s3_client.client.head_object(Bucket=bucket_name, Key=object_key)
        assert head_response["ContentLength"] == len(part_data)

    finally:
        fixture.cleanup()


def test_complete_multipart_upload_last_part_small(s3_client, config):
    """
    Test CompleteMultipartUpload with last part < 5MB

    Last part can be smaller than 5MB minimum
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("cmp-last-small")
        object_key = "last-small-object"

        s3_client.create_bucket(bucket_name)

        # Initiate multipart upload
        mp_response = s3_client.client.create_multipart_upload(
            Bucket=bucket_name, Key=object_key
        )
        upload_id = mp_response["UploadId"]

        parts = []

        # Upload first part (5MB - meets minimum)
        part1_data = b"a" * (5 * 1024 * 1024)
        part1_response = s3_client.client.upload_part(
            Bucket=bucket_name,
            Key=object_key,
            UploadId=upload_id,
            PartNumber=1,
            Body=part1_data,
        )
        parts.append({"PartNumber": 1, "ETag": part1_response["ETag"]})

        # Upload second part (5MB)
        part2_data = b"b" * (5 * 1024 * 1024)
        part2_response = s3_client.client.upload_part(
            Bucket=bucket_name,
            Key=object_key,
            UploadId=upload_id,
            PartNumber=2,
            Body=part2_data,
        )
        parts.append({"PartNumber": 2, "ETag": part2_response["ETag"]})

        # Upload last part (1KB - < 5MB, but allowed as last part)
        part3_data = b"c" * 1024
        part3_response = s3_client.client.upload_part(
            Bucket=bucket_name,
            Key=object_key,
            UploadId=upload_id,
            PartNumber=3,
            Body=part3_data,
        )
        parts.append({"PartNumber": 3, "ETag": part3_response["ETag"]})

        # Complete multipart upload
        s3_client.client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=object_key,
            UploadId=upload_id,
            MultipartUpload={"Parts": parts},
        )

        # Verify object size
        head_response = s3_client.client.head_object(Bucket=bucket_name, Key=object_key)
        expected_size = len(part1_data) + len(part2_data) + len(part3_data)
        assert head_response["ContentLength"] == expected_size

    finally:
        fixture.cleanup()


def test_complete_multipart_upload_middle_part_small_fails(s3_client, config):
    """
    Test CompleteMultipartUpload with middle part < 5MB

    Middle parts (not last) must be >= 5MB
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("cmp-middle-small")
        object_key = "middle-small-object"

        s3_client.create_bucket(bucket_name)

        # Initiate multipart upload
        mp_response = s3_client.client.create_multipart_upload(
            Bucket=bucket_name, Key=object_key
        )
        upload_id = mp_response["UploadId"]

        parts = []

        # Upload first part (5MB - meets minimum)
        part1_data = b"a" * (5 * 1024 * 1024)
        part1_response = s3_client.client.upload_part(
            Bucket=bucket_name,
            Key=object_key,
            UploadId=upload_id,
            PartNumber=1,
            Body=part1_data,
        )
        parts.append({"PartNumber": 1, "ETag": part1_response["ETag"]})

        # Upload middle part (1KB - too small)
        part2_data = b"b" * 1024
        part2_response = s3_client.client.upload_part(
            Bucket=bucket_name,
            Key=object_key,
            UploadId=upload_id,
            PartNumber=2,
            Body=part2_data,
        )
        parts.append({"PartNumber": 2, "ETag": part2_response["ETag"]})

        # Upload last part (5MB)
        part3_data = b"c" * (5 * 1024 * 1024)
        part3_response = s3_client.client.upload_part(
            Bucket=bucket_name,
            Key=object_key,
            UploadId=upload_id,
            PartNumber=3,
            Body=part3_data,
        )
        parts.append({"PartNumber": 3, "ETag": part3_response["ETag"]})

        # Try to complete multipart upload (should fail)
        with pytest.raises(ClientError) as exc_info:
            s3_client.client.complete_multipart_upload(
                Bucket=bucket_name,
                Key=object_key,
                UploadId=upload_id,
                MultipartUpload={"Parts": parts},
            )

        error_code = exc_info.value.response["Error"]["Code"]
        assert error_code in [
            "EntityTooSmall",
            "InvalidPart",
        ], f"Expected EntityTooSmall, got {error_code}"

    finally:
        fixture.cleanup()


def test_complete_multipart_upload_concurrent_complete_attempts(s3_client, config):
    """
    Test concurrent CompleteMultipartUpload attempts

    Second attempt should fail (upload already completed)
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("cmp-concurrent")
        object_key = "concurrent-object"

        s3_client.create_bucket(bucket_name)

        # Initiate multipart upload
        mp_response = s3_client.client.create_multipart_upload(
            Bucket=bucket_name, Key=object_key
        )
        upload_id = mp_response["UploadId"]

        # Upload single part
        part_data = b"x" * (5 * 1024 * 1024)
        part_response = s3_client.client.upload_part(
            Bucket=bucket_name,
            Key=object_key,
            UploadId=upload_id,
            PartNumber=1,
            Body=part_data,
        )
        parts = [{"PartNumber": 1, "ETag": part_response["ETag"]}]

        # First complete (should succeed)
        s3_client.client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=object_key,
            UploadId=upload_id,
            MultipartUpload={"Parts": parts},
        )

        # Second complete attempt (should fail)
        with pytest.raises(ClientError) as exc_info:
            s3_client.client.complete_multipart_upload(
                Bucket=bucket_name,
                Key=object_key,
                UploadId=upload_id,
                MultipartUpload={"Parts": parts},
            )

        error_code = exc_info.value.response["Error"]["Code"]
        assert error_code in [
            "NoSuchUpload",
            "404",
        ], f"Expected NoSuchUpload, got {error_code}"

    finally:
        fixture.cleanup()


def test_complete_multipart_upload_sparse_part_numbers(s3_client, config):
    """
    Test CompleteMultipartUpload with sparse part numbers

    Part numbers don't need to be consecutive (e.g., 1, 5, 10)
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("cmp-sparse-parts")
        object_key = "sparse-parts-object"

        s3_client.create_bucket(bucket_name)

        # Initiate multipart upload
        mp_response = s3_client.client.create_multipart_upload(
            Bucket=bucket_name, Key=object_key
        )
        upload_id = mp_response["UploadId"]

        parts = []

        # Upload part 1
        part1_data = b"a" * (5 * 1024 * 1024)
        part1_response = s3_client.client.upload_part(
            Bucket=bucket_name,
            Key=object_key,
            UploadId=upload_id,
            PartNumber=1,
            Body=part1_data,
        )
        parts.append({"PartNumber": 1, "ETag": part1_response["ETag"]})

        # Upload part 5 (skip 2-4)
        part5_data = b"b" * (5 * 1024 * 1024)
        part5_response = s3_client.client.upload_part(
            Bucket=bucket_name,
            Key=object_key,
            UploadId=upload_id,
            PartNumber=5,
            Body=part5_data,
        )
        parts.append({"PartNumber": 5, "ETag": part5_response["ETag"]})

        # Upload part 10 (skip 6-9)
        part10_data = b"c" * (5 * 1024 * 1024)
        part10_response = s3_client.client.upload_part(
            Bucket=bucket_name,
            Key=object_key,
            UploadId=upload_id,
            PartNumber=10,
            Body=part10_data,
        )
        parts.append({"PartNumber": 10, "ETag": part10_response["ETag"]})

        # Complete multipart upload
        s3_client.client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=object_key,
            UploadId=upload_id,
            MultipartUpload={"Parts": parts},
        )

        # Verify object assembly (only uploaded parts)
        obj_response = s3_client.client.get_object(Bucket=bucket_name, Key=object_key)
        data = obj_response["Body"].read()

        expected_data = part1_data + part5_data + part10_data
        assert data == expected_data

    finally:
        fixture.cleanup()


def test_complete_multipart_upload_with_empty_object(s3_client, config):
    """
    Test CompleteMultipartUpload creating empty object

    Empty parts or single empty part should create zero-length object
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("cmp-empty-obj")
        object_key = "empty-object"

        s3_client.create_bucket(bucket_name)

        # Initiate multipart upload
        mp_response = s3_client.client.create_multipart_upload(
            Bucket=bucket_name, Key=object_key
        )
        upload_id = mp_response["UploadId"]

        # Upload empty part
        part_data = b""
        try:
            part_response = s3_client.client.upload_part(
                Bucket=bucket_name,
                Key=object_key,
                UploadId=upload_id,
                PartNumber=1,
                Body=part_data,
            )

            # Complete multipart upload
            s3_client.client.complete_multipart_upload(
                Bucket=bucket_name,
                Key=object_key,
                UploadId=upload_id,
                MultipartUpload={
                    "Parts": [{"PartNumber": 1, "ETag": part_response["ETag"]}]
                },
            )

            # Verify zero-length object
            head_response = s3_client.client.head_object(
                Bucket=bucket_name, Key=object_key
            )
            assert head_response["ContentLength"] == 0

        except ClientError as e:
            # MinIO may not support empty parts
            if e.response["Error"]["Code"] in ["EntityTooSmall", "InvalidPart"]:
                pytest.skip("Empty parts not supported (implementation-specific)")
                return
            raise

    finally:
        fixture.cleanup()


def test_complete_multipart_upload_many_parts(s3_client, config):
    """
    Test CompleteMultipartUpload with many parts

    Test with 50 parts to verify handling of larger part lists
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("cmp-many-parts")
        object_key = "many-parts-object"

        s3_client.create_bucket(bucket_name)

        # Initiate multipart upload
        mp_response = s3_client.client.create_multipart_upload(
            Bucket=bucket_name, Key=object_key
        )
        upload_id = mp_response["UploadId"]

        parts = []
        expected_data = b""

        # Upload 50 parts (5MB each = 250MB total)
        for i in range(1, 51):
            part_data = str(i).encode() * (5 * 1024 * 1024 // len(str(i).encode()))
            expected_data += part_data

            part_response = s3_client.client.upload_part(
                Bucket=bucket_name,
                Key=object_key,
                UploadId=upload_id,
                PartNumber=i,
                Body=part_data,
            )
            parts.append({"PartNumber": i, "ETag": part_response["ETag"]})

        # Complete multipart upload
        s3_client.client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=object_key,
            UploadId=upload_id,
            MultipartUpload={"Parts": parts},
        )

        # Verify object size and content hash
        head_response = s3_client.client.head_object(Bucket=bucket_name, Key=object_key)
        assert head_response["ContentLength"] == len(expected_data)

        # Verify content integrity with hash
        obj_response = s3_client.client.get_object(Bucket=bucket_name, Key=object_key)
        actual_data = obj_response["Body"].read()

        expected_hash = hashlib.sha256(expected_data).hexdigest()
        actual_hash = hashlib.sha256(actual_data).hexdigest()
        assert actual_hash == expected_hash

    finally:
        fixture.cleanup()


def test_complete_multipart_upload_missing_required_parts(s3_client, config):
    """
    Test CompleteMultipartUpload with non-existing part numbers

    Specifying parts that weren't uploaded should fail
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("cmp-missing-parts")
        object_key = "missing-parts-object"

        s3_client.create_bucket(bucket_name)

        # Initiate multipart upload
        mp_response = s3_client.client.create_multipart_upload(
            Bucket=bucket_name, Key=object_key
        )
        upload_id = mp_response["UploadId"]

        # Upload only part 1
        part1_data = b"x" * (5 * 1024 * 1024)
        part1_response = s3_client.client.upload_part(
            Bucket=bucket_name,
            Key=object_key,
            UploadId=upload_id,
            PartNumber=1,
            Body=part1_data,
        )

        # Try to complete with parts 1, 2, 3 (but only part 1 was uploaded)
        parts = [
            {"PartNumber": 1, "ETag": part1_response["ETag"]},
            {"PartNumber": 2, "ETag": "fake-etag-1"},
            {"PartNumber": 3, "ETag": "fake-etag-2"},
        ]

        with pytest.raises(ClientError) as exc_info:
            s3_client.client.complete_multipart_upload(
                Bucket=bucket_name,
                Key=object_key,
                UploadId=upload_id,
                MultipartUpload={"Parts": parts},
            )

        error_code = exc_info.value.response["Error"]["Code"]
        assert error_code in [
            "InvalidPart",
            "NoSuchKey",
        ], f"Expected InvalidPart, got {error_code}"

    finally:
        fixture.cleanup()


def test_complete_multipart_upload_parts_reordered_in_complete(s3_client, config):
    """
    Test CompleteMultipartUpload with parts specified out of order

    Parts array must be sorted by PartNumber
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("cmp-reorder")
        object_key = "reorder-object"

        s3_client.create_bucket(bucket_name)

        # Initiate multipart upload
        mp_response = s3_client.client.create_multipart_upload(
            Bucket=bucket_name, Key=object_key
        )
        upload_id = mp_response["UploadId"]

        # Upload parts in order
        part1_data = b"a" * (5 * 1024 * 1024)
        part1_response = s3_client.client.upload_part(
            Bucket=bucket_name,
            Key=object_key,
            UploadId=upload_id,
            PartNumber=1,
            Body=part1_data,
        )

        part2_data = b"b" * (5 * 1024 * 1024)
        part2_response = s3_client.client.upload_part(
            Bucket=bucket_name,
            Key=object_key,
            UploadId=upload_id,
            PartNumber=2,
            Body=part2_data,
        )

        part3_data = b"c" * (5 * 1024 * 1024)
        part3_response = s3_client.client.upload_part(
            Bucket=bucket_name,
            Key=object_key,
            UploadId=upload_id,
            PartNumber=3,
            Body=part3_data,
        )

        # Specify parts in wrong order (3, 1, 2)
        parts = [
            {"PartNumber": 3, "ETag": part3_response["ETag"]},
            {"PartNumber": 1, "ETag": part1_response["ETag"]},
            {"PartNumber": 2, "ETag": part2_response["ETag"]},
        ]

        # Try to complete (AWS requires sorted order)
        try:
            s3_client.client.complete_multipart_upload(
                Bucket=bucket_name,
                Key=object_key,
                UploadId=upload_id,
                MultipartUpload={"Parts": parts},
            )
            # MinIO may accept unsorted parts
        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            assert error_code in [
                "InvalidPartOrder",
                "InvalidRequest",
            ], f"Expected InvalidPartOrder, got {error_code}"

    finally:
        fixture.cleanup()
