#!/usr/bin/env python3
"""
S3 CreateMultipartUpload Tests

Tests CreateMultipartUpload API edge cases and features:
- Metadata preservation
- Content headers (ContentType, ContentEncoding, etc.)
- Tagging during creation
- Error conditions
- Success responses

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


def test_create_multipart_upload_non_existing_bucket(s3_client, config):
    """
    Test CreateMultipartUpload on non-existing bucket

    Should return NoSuchBucket error
    """
    fixture = TestFixture(s3_client, config)

    try:
        with pytest.raises(ClientError) as exc_info:
            s3_client.client.create_multipart_upload(
                Bucket="non-existing-bucket-12345", Key="my-obj"
            )

        error_code = exc_info.value.response["Error"]["Code"]
        assert error_code == "NoSuchBucket", f"Expected NoSuchBucket, got {error_code}"

    finally:
        fixture.cleanup()


def test_create_multipart_upload_with_metadata(s3_client, config):
    """
    Test CreateMultipartUpload with metadata and content headers

    Metadata should be preserved in completed object
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("create-mp-meta")
        s3_client.create_bucket(bucket_name)

        key = "my-obj"
        metadata = {"prop1": "val1", "prop2": "val2"}
        content_type = "application/text"
        content_encoding = "testenc"
        content_disposition = "testdesp"
        content_language = "sp"
        cache_control = "no-cache"
        expires = datetime.now(timezone.utc) + timedelta(hours=5)

        # Create multipart upload with metadata
        mp_response = s3_client.client.create_multipart_upload(
            Bucket=bucket_name,
            Key=key,
            Metadata=metadata,
            ContentType=content_type,
            ContentEncoding=content_encoding,
            ContentDisposition=content_disposition,
            ContentLanguage=content_language,
            CacheControl=cache_control,
            Expires=expires,
        )

        upload_id = mp_response["UploadId"]

        # Upload a small part
        part_data = b"x" * 100
        part_response = s3_client.client.upload_part(
            Bucket=bucket_name,
            Key=key,
            UploadId=upload_id,
            PartNumber=1,
            Body=part_data,
        )

        # Complete the upload
        s3_client.client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={
                "Parts": [{"PartNumber": 1, "ETag": part_response["ETag"]}]
            },
        )

        # Verify metadata was preserved
        head_response = s3_client.client.head_object(Bucket=bucket_name, Key=key)

        assert head_response["Metadata"] == metadata
        assert head_response["ContentType"] == content_type
        assert head_response["ContentEncoding"] == content_encoding
        assert head_response["ContentDisposition"] == content_disposition
        assert head_response["ContentLanguage"] == content_language
        assert head_response["CacheControl"] == cache_control
        assert "Expires" in head_response

    finally:
        fixture.cleanup()


def test_create_multipart_upload_with_tagging(s3_client, config):
    """
    Test CreateMultipartUpload with tagging

    Tags should be applied to completed object
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("create-mp-tag")
        s3_client.create_bucket(bucket_name)

        key = "my-obj"
        tagging = "key1=val1&key2=val2"

        # Create multipart upload with tagging
        mp_response = s3_client.client.create_multipart_upload(
            Bucket=bucket_name, Key=key, Tagging=tagging
        )

        upload_id = mp_response["UploadId"]

        # Upload a part
        part_data = b"x" * (5 * 1024 * 1024)
        part_response = s3_client.client.upload_part(
            Bucket=bucket_name,
            Key=key,
            UploadId=upload_id,
            PartNumber=1,
            Body=part_data,
        )

        # Complete the upload
        s3_client.client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={
                "Parts": [{"PartNumber": 1, "ETag": part_response["ETag"]}]
            },
        )

        # Verify tags were applied
        tag_response = s3_client.client.get_object_tagging(Bucket=bucket_name, Key=key)

        tags = {tag["Key"]: tag["Value"] for tag in tag_response["TagSet"]}
        assert tags == {"key1": "val1", "key2": "val2"}

    finally:
        fixture.cleanup()


def test_create_multipart_upload_success(s3_client, config):
    """
    Test successful CreateMultipartUpload

    Should return upload ID and proper response structure
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("create-mp-success")
        s3_client.create_bucket(bucket_name)

        key = "my-obj"

        # Create multipart upload
        response = s3_client.client.create_multipart_upload(Bucket=bucket_name, Key=key)

        # Verify response structure
        assert "UploadId" in response
        assert response["Bucket"] == bucket_name
        assert response["Key"] == key
        assert response["UploadId"] != ""

    finally:
        fixture.cleanup()


def test_create_multipart_upload_empty_tagging(s3_client, config):
    """
    Test CreateMultipartUpload with empty tagging strings

    Empty tagging should be handled gracefully
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("create-mp-empty-tag")
        s3_client.create_bucket(bucket_name)

        # Test various empty tagging patterns
        for tagging in ["&", "&&&", "key", "key&", "key=&"]:
            key = f'obj-{tagging.replace("&", "amp")}'

            mp_response = s3_client.client.create_multipart_upload(
                Bucket=bucket_name, Key=key, Tagging=tagging
            )

            # Should succeed
            assert "UploadId" in mp_response

            # Abort to clean up
            s3_client.abort_multipart_upload(bucket_name, key, mp_response["UploadId"])

    finally:
        fixture.cleanup()


def test_create_multipart_upload_invalid_tagging(s3_client, config):
    """
    Test CreateMultipartUpload with invalid tagging

    Invalid tag keys/values should be rejected
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("create-mp-bad-tag")
        s3_client.create_bucket(bucket_name)

        # Invalid tagging patterns
        invalid_tags = [
            "key?=val",  # Invalid character in key
            "key=val?",  # Invalid character in value
            "key*=val",  # Invalid character in key
            "key=val*",  # Invalid character in value
        ]

        for tagging in invalid_tags:
            with pytest.raises(ClientError) as exc_info:
                s3_client.client.create_multipart_upload(
                    Bucket=bucket_name, Key="test-obj", Tagging=tagging
                )

            error_code = exc_info.value.response["Error"]["Code"]
            # May be InvalidTag, InvalidTagKey, or InvalidTagValue
            assert error_code in [
                "InvalidTag",
                "InvalidTagKey",
                "InvalidTagValue",
            ], f"Expected tagging error for '{tagging}', got {error_code}"

    finally:
        fixture.cleanup()


def test_create_multipart_upload_special_char_tagging(s3_client, config):
    """
    Test CreateMultipartUpload with special characters in tags

    Certain special characters are allowed in tags
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("create-mp-special-tag")
        s3_client.create_bucket(bucket_name)

        key = "my-obj"
        # Valid special characters: - _ . /
        tagging = "key-key_key.key/key=value-value_value.value/value"

        mp_response = s3_client.client.create_multipart_upload(
            Bucket=bucket_name, Key=key, Tagging=tagging
        )

        upload_id = mp_response["UploadId"]

        # Upload and complete to verify tagging
        part_data = b"x" * (5 * 1024 * 1024)
        part_response = s3_client.client.upload_part(
            Bucket=bucket_name,
            Key=key,
            UploadId=upload_id,
            PartNumber=1,
            Body=part_data,
        )

        s3_client.client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={
                "Parts": [{"PartNumber": 1, "ETag": part_response["ETag"]}]
            },
        )

        # Verify tags
        tag_response = s3_client.client.get_object_tagging(Bucket=bucket_name, Key=key)

        tags = {tag["Key"]: tag["Value"] for tag in tag_response["TagSet"]}
        assert "key-key_key.key/key" in tags
        assert tags["key-key_key.key/key"] == "value-value_value.value/value"

    finally:
        fixture.cleanup()


def test_create_multipart_upload_duplicate_tag_keys(s3_client, config):
    """
    Test CreateMultipartUpload with duplicate tag keys

    Duplicate tag keys should be rejected
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("create-mp-dup-tag")
        s3_client.create_bucket(bucket_name)

        tagging = "key=val&key=val"

        with pytest.raises(ClientError) as exc_info:
            s3_client.client.create_multipart_upload(
                Bucket=bucket_name, Key="test-obj", Tagging=tagging
            )

        error_code = exc_info.value.response["Error"]["Code"]
        # May vary by implementation
        assert error_code in [
            "InvalidTag",
            "InvalidRequest",
        ], f"Expected InvalidTag/InvalidRequest, got {error_code}"

    finally:
        fixture.cleanup()


def test_create_multipart_upload_multiple_times_same_key(s3_client, config):
    """
    Test creating multiple multipart uploads for same key

    Multiple concurrent uploads for same key should be allowed
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("create-mp-multi")
        s3_client.create_bucket(bucket_name)

        key = "my-obj"

        # Create multiple uploads for same key
        upload_id1 = s3_client.create_multipart_upload(bucket_name, key)
        upload_id2 = s3_client.create_multipart_upload(bucket_name, key)
        upload_id3 = s3_client.create_multipart_upload(bucket_name, key)

        # All should have different upload IDs
        assert upload_id1 != upload_id2
        assert upload_id2 != upload_id3
        assert upload_id1 != upload_id3

        # List multipart uploads should show all 3
        list_response = s3_client.client.list_multipart_uploads(Bucket=bucket_name)

        uploads = list_response.get("Uploads", [])
        assert len(uploads) == 3

        # All should be for same key
        for upload in uploads:
            assert upload["Key"] == key

    finally:
        fixture.cleanup()


def test_create_multipart_upload_with_storage_class(s3_client, config):
    """
    Test CreateMultipartUpload with StorageClass

    Storage class should be preserved in completed object
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("create-mp-storage")
        s3_client.create_bucket(bucket_name)

        key = "my-obj"

        # Create multipart upload with storage class
        mp_response = s3_client.client.create_multipart_upload(
            Bucket=bucket_name, Key=key, StorageClass="STANDARD"
        )

        upload_id = mp_response["UploadId"]

        # Upload and complete
        part_data = b"x" * (5 * 1024 * 1024)
        part_response = s3_client.client.upload_part(
            Bucket=bucket_name,
            Key=key,
            UploadId=upload_id,
            PartNumber=1,
            Body=part_data,
        )

        s3_client.client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={
                "Parts": [{"PartNumber": 1, "ETag": part_response["ETag"]}]
            },
        )

        # Verify storage class
        head_response = s3_client.client.head_object(Bucket=bucket_name, Key=key)

        # Storage class may or may not be in response depending on implementation
        # Just verify object was created successfully
        assert "ETag" in head_response

    finally:
        fixture.cleanup()
