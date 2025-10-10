#!/usr/bin/env python3
"""
S3 ListParts Tests

Tests ListParts API for multipart uploads:
- Upload ID and key validation
- Pagination with MaxParts and PartNumberMarker
- Part metadata verification
- Error conditions

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


def test_list_parts_incorrect_upload_id(s3_client, config):
    """
    Test ListParts with invalid upload ID

    Should return NoSuchUpload error
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("list-parts-bad-id")
        s3_client.create_bucket(bucket_name)

        with pytest.raises(ClientError) as exc_info:
            s3_client.client.list_parts(
                Bucket=bucket_name, Key="my-obj", UploadId="invalid-upload-id"
            )

        error_code = exc_info.value.response["Error"]["Code"]
        assert error_code == "NoSuchUpload", f"Expected NoSuchUpload, got {error_code}"

    finally:
        fixture.cleanup()


def test_list_parts_incorrect_object_key(s3_client, config):
    """
    Test ListParts with wrong object key

    Upload ID is tied to specific key
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("list-parts-bad-key")
        s3_client.create_bucket(bucket_name)

        key = "my-obj"
        upload_id = s3_client.create_multipart_upload(bucket_name, key)

        # Try to list parts with different key
        with pytest.raises(ClientError) as exc_info:
            s3_client.client.list_parts(
                Bucket=bucket_name, Key="incorrect-object-key", UploadId=upload_id
            )

        error_code = exc_info.value.response["Error"]["Code"]
        assert error_code == "NoSuchUpload", f"Expected NoSuchUpload, got {error_code}"

    finally:
        fixture.cleanup()


def test_list_parts_invalid_max_parts(s3_client, config):
    """
    Test ListParts with negative MaxParts

    Should return InvalidArgument error
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("list-parts-inv-max")
        s3_client.create_bucket(bucket_name)

        key = "my-obj"
        upload_id = s3_client.create_multipart_upload(bucket_name, key)

        with pytest.raises(ClientError) as exc_info:
            s3_client.client.list_parts(
                Bucket=bucket_name, Key=key, UploadId=upload_id, MaxParts=-3
            )

        error_code = exc_info.value.response["Error"]["Code"]
        assert (
            error_code == "InvalidArgument"
        ), f"Expected InvalidArgument, got {error_code}"

    finally:
        fixture.cleanup()


def test_list_parts_default_max_parts(s3_client, config):
    """
    Test ListParts default MaxParts value

    Default should be 1000
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("list-parts-default")
        s3_client.create_bucket(bucket_name)

        key = "my-obj"
        upload_id = s3_client.create_multipart_upload(bucket_name, key)

        response = s3_client.client.list_parts(
            Bucket=bucket_name, Key=key, UploadId=upload_id
        )

        assert "MaxParts" in response
        # MinIO uses 10000, AWS S3 uses 1000
        assert response["MaxParts"] in [1000, 10000]

    finally:
        fixture.cleanup()


def test_list_parts_truncated(s3_client, config):
    """
    Test ListParts pagination with MaxParts

    Should support pagination with NextPartNumberMarker
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("list-parts-trunc")
        s3_client.create_bucket(bucket_name)

        key = "my-obj"
        upload_id = s3_client.create_multipart_upload(bucket_name, key)

        # Upload 5 parts
        part_size = 5 * 1024 * 1024
        parts = []
        for part_num in range(1, 6):
            part_data = b"x" * part_size
            response = s3_client.client.upload_part(
                Bucket=bucket_name,
                Key=key,
                UploadId=upload_id,
                PartNumber=part_num,
                Body=part_data,
            )
            parts.append({"PartNumber": part_num, "ETag": response["ETag"]})

        # List first 3 parts
        response1 = s3_client.client.list_parts(
            Bucket=bucket_name, Key=key, UploadId=upload_id, MaxParts=3
        )

        assert response1["IsTruncated"] is True
        assert response1["MaxParts"] == 3
        assert len(response1["Parts"]) == 3
        assert "NextPartNumberMarker" in response1
        assert int(response1["NextPartNumberMarker"]) == 3

        # Verify first 3 parts
        for i in range(3):
            assert response1["Parts"][i]["PartNumber"] == i + 1
            assert response1["Parts"][i]["ETag"] == parts[i]["ETag"]

        # List remaining parts
        response2 = s3_client.client.list_parts(
            Bucket=bucket_name,
            Key=key,
            UploadId=upload_id,
            PartNumberMarker=response1["NextPartNumberMarker"],
        )

        assert "PartNumberMarker" in response2
        assert response2["PartNumberMarker"] == response1["NextPartNumberMarker"]
        assert len(response2["Parts"]) == 2

        # Verify remaining parts (parts 4 and 5)
        for i in range(2):
            assert response2["Parts"][i]["PartNumber"] == i + 4
            assert response2["Parts"][i]["ETag"] == parts[i + 3]["ETag"]

    finally:
        fixture.cleanup()


def test_list_parts_success(s3_client, config):
    """
    Test successful ListParts operation

    Should list all uploaded parts with metadata
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("list-parts-success")
        s3_client.create_bucket(bucket_name)

        key = "my-obj"
        upload_id = s3_client.create_multipart_upload(bucket_name, key)

        # Upload 5 parts
        part_size = 5 * 1024 * 1024
        uploaded_parts = []
        for part_num in range(1, 6):
            part_data = b"x" * part_size
            response = s3_client.client.upload_part(
                Bucket=bucket_name,
                Key=key,
                UploadId=upload_id,
                PartNumber=part_num,
                Body=part_data,
            )
            uploaded_parts.append(
                {"PartNumber": part_num, "ETag": response["ETag"], "Size": part_size}
            )

        # List all parts
        response = s3_client.client.list_parts(
            Bucket=bucket_name, Key=key, UploadId=upload_id
        )

        # Verify response
        assert "Parts" in response
        assert len(response["Parts"]) == 5
        assert response.get("StorageClass") == "STANDARD"

        # Verify each part
        for i, part in enumerate(response["Parts"]):
            assert part["PartNumber"] == uploaded_parts[i]["PartNumber"]
            assert part["ETag"] == uploaded_parts[i]["ETag"]
            assert part["Size"] == uploaded_parts[i]["Size"]

    finally:
        fixture.cleanup()


def test_list_parts_empty_upload(s3_client, config):
    """
    Test ListParts on upload with no parts

    Should return empty parts list
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("list-parts-empty")
        s3_client.create_bucket(bucket_name)

        key = "my-obj"
        upload_id = s3_client.create_multipart_upload(bucket_name, key)

        # List parts without uploading any
        response = s3_client.client.list_parts(
            Bucket=bucket_name, Key=key, UploadId=upload_id
        )

        assert "Parts" not in response or len(response.get("Parts", [])) == 0
        assert response.get("IsTruncated", False) is False

    finally:
        fixture.cleanup()


def test_list_parts_after_abort(s3_client, config):
    """
    Test ListParts after aborting upload

    Should return NoSuchUpload error
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("list-parts-aborted")
        s3_client.create_bucket(bucket_name)

        key = "my-obj"
        upload_id = s3_client.create_multipart_upload(bucket_name, key)

        # Upload a part
        part_data = b"x" * (5 * 1024 * 1024)
        s3_client.client.upload_part(
            Bucket=bucket_name,
            Key=key,
            UploadId=upload_id,
            PartNumber=1,
            Body=part_data,
        )

        # Abort the upload
        s3_client.abort_multipart_upload(bucket_name, key, upload_id)

        # Try to list parts after abort
        with pytest.raises(ClientError) as exc_info:
            s3_client.client.list_parts(Bucket=bucket_name, Key=key, UploadId=upload_id)

        error_code = exc_info.value.response["Error"]["Code"]
        assert error_code == "NoSuchUpload", f"Expected NoSuchUpload, got {error_code}"

    finally:
        fixture.cleanup()


def test_list_parts_part_number_marker(s3_client, config):
    """
    Test ListParts with PartNumberMarker

    Should list parts after the specified marker
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("list-parts-marker")
        s3_client.create_bucket(bucket_name)

        key = "my-obj"
        upload_id = s3_client.create_multipart_upload(bucket_name, key)

        # Upload 5 parts
        part_size = 5 * 1024 * 1024
        for part_num in range(1, 6):
            part_data = b"x" * part_size
            s3_client.client.upload_part(
                Bucket=bucket_name,
                Key=key,
                UploadId=upload_id,
                PartNumber=part_num,
                Body=part_data,
            )

        # List parts after part 2
        response = s3_client.client.list_parts(
            Bucket=bucket_name, Key=key, UploadId=upload_id, PartNumberMarker=2
        )

        # Should get parts 3, 4, 5
        assert len(response["Parts"]) == 3
        assert response["Parts"][0]["PartNumber"] == 3
        assert response["Parts"][1]["PartNumber"] == 4
        assert response["Parts"][2]["PartNumber"] == 5

    finally:
        fixture.cleanup()
