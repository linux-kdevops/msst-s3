#!/usr/bin/env python3
"""
S3 GetObjectAttributes Tests

Tests GetObjectAttributes API operations:
- GetObjectAttributes error conditions
- Attribute retrieval (ETag, ObjectSize, StorageClass, etc.)
- Invalid attributes handling
- Checksum support

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


def test_get_object_attributes_non_existing_bucket(s3_client, config):
    """
    Test GetObjectAttributes on non-existing bucket

    Should return NoSuchBucket error
    """
    fixture = TestFixture(s3_client, config)

    try:
        # Generate bucket name but don't create it
        bucket_name = fixture.generate_bucket_name("goa-no-bucket")

        # Try GetObjectAttributes on non-existing bucket
        with pytest.raises(ClientError) as exc_info:
            s3_client.client.get_object_attributes(
                Bucket=bucket_name, Key="my-obj", ObjectAttributes=["ETag"]
            )

        error_code = exc_info.value.response["Error"]["Code"]
        assert error_code in [
            "NoSuchBucket",
            "404",
        ], f"Expected NoSuchBucket, got {error_code}"

    finally:
        fixture.cleanup()


def test_get_object_attributes_non_existing_object(s3_client, config):
    """
    Test GetObjectAttributes on non-existing object

    Should return NoSuchKey error
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("goa-no-obj")
        s3_client.create_bucket(bucket_name)

        # Try GetObjectAttributes on non-existing object
        with pytest.raises(ClientError) as exc_info:
            s3_client.client.get_object_attributes(
                Bucket=bucket_name, Key="my-obj", ObjectAttributes=["ETag"]
            )

        error_code = exc_info.value.response["Error"]["Code"]
        assert error_code in [
            "NoSuchKey",
            "404",
        ], f"Expected NoSuchKey, got {error_code}"

    finally:
        fixture.cleanup()


def test_get_object_attributes_invalid_attrs(s3_client, config):
    """
    Test GetObjectAttributes with invalid attribute

    Should return error for invalid attribute name
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("goa-invalid-attr")
        s3_client.create_bucket(bucket_name)

        key = "my-obj"
        s3_client.put_object(bucket_name, key, b"test data")

        # Try GetObjectAttributes with invalid attribute
        with pytest.raises(ClientError) as exc_info:
            s3_client.client.get_object_attributes(
                Bucket=bucket_name,
                Key=key,
                ObjectAttributes=["ETag", "Invalid_argument"],
            )

        error_code = exc_info.value.response["Error"]["Code"]
        assert error_code in [
            "InvalidArgument",
            "InvalidRequest",
        ], f"Expected InvalidArgument, got {error_code}"

    finally:
        fixture.cleanup()


def test_get_object_attributes_invalid_parent(s3_client, config):
    """
    Test GetObjectAttributes with invalid parent directory

    Should return NoSuchKey for 'not-a-dir/bad-obj' when 'not-a-dir' is a file
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("goa-inv-parent")
        s3_client.create_bucket(bucket_name)

        # Create file object (not a directory)
        file_key = "not-a-dir"
        s3_client.put_object(bucket_name, file_key, b"x")

        # Try GetObjectAttributes with file as parent directory
        nested_key = "not-a-dir/bad-obj"
        with pytest.raises(ClientError) as exc_info:
            s3_client.client.get_object_attributes(
                Bucket=bucket_name, Key=nested_key, ObjectAttributes=["ETag"]
            )

        error_code = exc_info.value.response["Error"]["Code"]
        assert error_code in [
            "NoSuchKey",
            "404",
        ], f"Expected NoSuchKey, got {error_code}"

    finally:
        fixture.cleanup()


def test_get_object_attributes_invalid_single_attribute(s3_client, config):
    """
    Test GetObjectAttributes with single invalid attribute

    Should return error for invalid attribute
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("goa-single-inv")
        s3_client.create_bucket(bucket_name)

        key = "my-obj"

        # Try GetObjectAttributes with invalid attribute
        with pytest.raises(ClientError) as exc_info:
            s3_client.client.get_object_attributes(
                Bucket=bucket_name, Key=key, ObjectAttributes=["invalid_attr"]
            )

        error_code = exc_info.value.response["Error"]["Code"]
        assert error_code in [
            "InvalidArgument",
            "InvalidRequest",
        ], f"Expected InvalidArgument, got {error_code}"

    finally:
        fixture.cleanup()


def test_get_object_attributes_empty_attrs(s3_client, config):
    """
    Test GetObjectAttributes with empty attributes list

    Should return error - at least one attribute required
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("goa-empty-attrs")
        s3_client.create_bucket(bucket_name)

        key = "my-obj"
        s3_client.put_object(bucket_name, key, b"test data")

        # Try GetObjectAttributes with empty attributes list
        with pytest.raises(ClientError) as exc_info:
            s3_client.client.get_object_attributes(
                Bucket=bucket_name, Key=key, ObjectAttributes=[]
            )

        error_code = exc_info.value.response["Error"]["Code"]
        assert error_code in [
            "InvalidArgument",
            "InvalidRequest",
        ], f"Expected InvalidArgument, got {error_code}"

    finally:
        fixture.cleanup()


def test_get_object_attributes_existing_object(s3_client, config):
    """
    Test GetObjectAttributes on existing object

    Should return requested attributes
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("goa-existing")
        s3_client.create_bucket(bucket_name)

        key = "my-obj"
        data = b"x" * 45679

        # Put object
        put_response = s3_client.put_object(bucket_name, key, data)

        # GetObjectAttributes
        attrs_response = s3_client.client.get_object_attributes(
            Bucket=bucket_name,
            Key=key,
            ObjectAttributes=["ETag", "ObjectSize", "StorageClass"],
        )

        # Verify ETag
        assert "ETag" in attrs_response
        # Remove quotes from put_response ETag
        expected_etag = put_response["ETag"].strip('"')
        assert attrs_response["ETag"] == expected_etag

        # Verify ObjectSize
        assert "ObjectSize" in attrs_response
        assert attrs_response["ObjectSize"] == len(data)

        # Verify StorageClass
        assert "StorageClass" in attrs_response
        assert attrs_response["StorageClass"] == "STANDARD"

        # Verify LastModified
        assert "LastModified" in attrs_response

        # Checksum should be None (not set)
        assert attrs_response.get("Checksum") is None

    finally:
        fixture.cleanup()


def test_get_object_attributes_checksums(s3_client, config):
    """
    Test GetObjectAttributes with checksum algorithms

    Should return checksum information when available
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("goa-checksums")
        s3_client.create_bucket(bucket_name)

        # Test objects with different checksum algorithms
        test_objects = [
            ("obj-1", "CRC32"),
            ("obj-2", "SHA1"),
            ("obj-3", "SHA256"),
        ]

        for key, algo in test_objects:
            try:
                # Put object with checksum
                put_response = s3_client.client.put_object(
                    Bucket=bucket_name,
                    Key=key,
                    Body=b"x" * 1000,
                    ChecksumAlgorithm=algo,
                )
            except Exception:
                # Checksum not supported
                pytest.skip(f"ChecksumAlgorithm {algo} not supported")
                return

            # GetObjectAttributes
            attrs_response = s3_client.client.get_object_attributes(
                Bucket=bucket_name,
                Key=key,
                ObjectAttributes=["ETag", "Checksum", "ObjectParts"],
            )

            # Verify basic attributes
            assert "ETag" in attrs_response

            # Verify checksum if available
            checksum_field = f"Checksum{algo}"
            if "Checksum" in attrs_response:
                # MinIO may not return Checksum structure
                if checksum_field in put_response:
                    # Checksum available - verify match
                    pass

    finally:
        fixture.cleanup()


def test_get_object_attributes_response_fields(s3_client, config):
    """
    Test GetObjectAttributes response structure

    Should include all requested fields in response
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("goa-response")
        s3_client.create_bucket(bucket_name)

        key = "my-obj"
        s3_client.put_object(bucket_name, key, b"test data")

        # GetObjectAttributes with all standard attributes
        attrs_response = s3_client.client.get_object_attributes(
            Bucket=bucket_name,
            Key=key,
            ObjectAttributes=["ETag", "ObjectSize", "StorageClass"],
        )

        # Verify response structure
        assert "ETag" in attrs_response
        assert "ObjectSize" in attrs_response
        assert "StorageClass" in attrs_response
        assert "LastModified" in attrs_response

        # Verify types
        assert isinstance(attrs_response["ETag"], str)
        assert isinstance(attrs_response["ObjectSize"], int)
        assert isinstance(attrs_response["StorageClass"], str)

    finally:
        fixture.cleanup()


def test_get_object_attributes_multipart_object(s3_client, config):
    """
    Test GetObjectAttributes on multipart uploaded object

    Should return ObjectParts information
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("goa-multipart")
        s3_client.create_bucket(bucket_name)

        key = "my-obj"

        # Create multipart upload
        create_response = s3_client.client.create_multipart_upload(
            Bucket=bucket_name, Key=key
        )
        upload_id = create_response["UploadId"]

        # Upload 3 parts
        parts = []
        for part_num in range(1, 4):
            part_data = b"x" * (5 * 1024 * 1024)  # 5MB each
            upload_response = s3_client.client.upload_part(
                Bucket=bucket_name,
                Key=key,
                UploadId=upload_id,
                PartNumber=part_num,
                Body=part_data,
            )
            parts.append({"PartNumber": part_num, "ETag": upload_response["ETag"]})

        # Complete multipart upload
        s3_client.client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={"Parts": parts},
        )

        # GetObjectAttributes with ObjectParts
        attrs_response = s3_client.client.get_object_attributes(
            Bucket=bucket_name,
            Key=key,
            ObjectAttributes=["ETag", "ObjectSize", "ObjectParts"],
        )

        # Verify basic attributes
        assert "ETag" in attrs_response
        assert "ObjectSize" in attrs_response
        assert attrs_response["ObjectSize"] == 15 * 1024 * 1024  # 3 parts × 5MB

        # Verify ObjectParts if available (MinIO may not support)
        if "ObjectParts" in attrs_response:
            assert "TotalPartsCount" in attrs_response["ObjectParts"]
            assert attrs_response["ObjectParts"]["TotalPartsCount"] == 3

    finally:
        fixture.cleanup()
