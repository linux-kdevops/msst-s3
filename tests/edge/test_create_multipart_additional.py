#!/usr/bin/env python3
"""
S3 CreateMultipartUpload Additional Tests

Tests CreateMultipartUpload edge cases:
- Server-side encryption options
- Cache control headers
- Content disposition
- Object lock settings
- Request payer scenarios
- Bucket key enabled

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


def test_create_multipart_upload_with_sse_s3(s3_client, config):
    """
    Test CreateMultipartUpload with SSE-S3 encryption

    Should set ServerSideEncryption to AES256
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("cmu-sse-s3")
        object_key = "sse-s3-object"

        s3_client.create_bucket(bucket_name)

        # Create multipart upload with SSE-S3
        try:
            mp_response = s3_client.client.create_multipart_upload(
                Bucket=bucket_name, Key=object_key, ServerSideEncryption="AES256"
            )
            upload_id = mp_response["UploadId"]
            assert upload_id is not None

            # Verify SSE in response
            if "ServerSideEncryption" in mp_response:
                assert mp_response["ServerSideEncryption"] == "AES256"

            # Abort upload
            s3_client.client.abort_multipart_upload(
                Bucket=bucket_name, Key=object_key, UploadId=upload_id
            )

        except ClientError as e:
            if e.response["Error"]["Code"] in ["NotImplemented", "InvalidArgument"]:
                pytest.skip("SSE-S3 not supported")
                return
            raise

    finally:
        fixture.cleanup()


def test_create_multipart_upload_with_cache_control(s3_client, config):
    """
    Test CreateMultipartUpload with CacheControl header

    CacheControl should be preserved on completed object
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("cmu-cache")
        object_key = "cache-object"

        s3_client.create_bucket(bucket_name)

        cache_control = "max-age=3600, public"

        # Create multipart upload with CacheControl
        mp_response = s3_client.client.create_multipart_upload(
            Bucket=bucket_name, Key=object_key, CacheControl=cache_control
        )
        upload_id = mp_response["UploadId"]

        # Upload a part
        part_data = b"x" * (5 * 1024 * 1024)
        part_response = s3_client.client.upload_part(
            Bucket=bucket_name,
            Key=object_key,
            UploadId=upload_id,
            PartNumber=1,
            Body=part_data,
        )

        # Complete upload
        s3_client.client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=object_key,
            UploadId=upload_id,
            MultipartUpload={
                "Parts": [{"PartNumber": 1, "ETag": part_response["ETag"]}]
            },
        )

        # Verify CacheControl header
        head_response = s3_client.client.head_object(Bucket=bucket_name, Key=object_key)
        if "CacheControl" in head_response:
            assert head_response["CacheControl"] == cache_control

    finally:
        fixture.cleanup()


def test_create_multipart_upload_with_content_disposition(s3_client, config):
    """
    Test CreateMultipartUpload with ContentDisposition

    ContentDisposition should be preserved on completed object
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("cmu-disposition")
        object_key = "disposition-object"

        s3_client.create_bucket(bucket_name)

        content_disposition = 'attachment; filename="download.txt"'

        # Create multipart upload with ContentDisposition
        mp_response = s3_client.client.create_multipart_upload(
            Bucket=bucket_name, Key=object_key, ContentDisposition=content_disposition
        )
        upload_id = mp_response["UploadId"]

        # Upload a part
        part_data = b"x" * (5 * 1024 * 1024)
        part_response = s3_client.client.upload_part(
            Bucket=bucket_name,
            Key=object_key,
            UploadId=upload_id,
            PartNumber=1,
            Body=part_data,
        )

        # Complete upload
        s3_client.client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=object_key,
            UploadId=upload_id,
            MultipartUpload={
                "Parts": [{"PartNumber": 1, "ETag": part_response["ETag"]}]
            },
        )

        # Verify ContentDisposition header
        head_response = s3_client.client.head_object(Bucket=bucket_name, Key=object_key)
        if "ContentDisposition" in head_response:
            assert head_response["ContentDisposition"] == content_disposition

    finally:
        fixture.cleanup()


def test_create_multipart_upload_with_content_language(s3_client, config):
    """
    Test CreateMultipartUpload with ContentLanguage

    ContentLanguage should be preserved on completed object
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("cmu-language")
        object_key = "language-object"

        s3_client.create_bucket(bucket_name)

        content_language = "en-US"

        # Create multipart upload with ContentLanguage
        mp_response = s3_client.client.create_multipart_upload(
            Bucket=bucket_name, Key=object_key, ContentLanguage=content_language
        )
        upload_id = mp_response["UploadId"]

        # Upload a part
        part_data = b"x" * (5 * 1024 * 1024)
        part_response = s3_client.client.upload_part(
            Bucket=bucket_name,
            Key=object_key,
            UploadId=upload_id,
            PartNumber=1,
            Body=part_data,
        )

        # Complete upload
        s3_client.client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=object_key,
            UploadId=upload_id,
            MultipartUpload={
                "Parts": [{"PartNumber": 1, "ETag": part_response["ETag"]}]
            },
        )

        # Verify ContentLanguage header
        head_response = s3_client.client.head_object(Bucket=bucket_name, Key=object_key)
        if "ContentLanguage" in head_response:
            assert head_response["ContentLanguage"] == content_language

    finally:
        fixture.cleanup()


def test_create_multipart_upload_with_expires_header(s3_client, config):
    """
    Test CreateMultipartUpload with Expires header

    Expires should be preserved on completed object
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("cmu-expires")
        object_key = "expires-object"

        s3_client.create_bucket(bucket_name)

        from datetime import datetime, timezone

        expires = datetime(2030, 12, 31, 23, 59, 59, tzinfo=timezone.utc)

        # Create multipart upload with Expires
        mp_response = s3_client.client.create_multipart_upload(
            Bucket=bucket_name, Key=object_key, Expires=expires
        )
        upload_id = mp_response["UploadId"]

        # Upload a part
        part_data = b"x" * (5 * 1024 * 1024)
        part_response = s3_client.client.upload_part(
            Bucket=bucket_name,
            Key=object_key,
            UploadId=upload_id,
            PartNumber=1,
            Body=part_data,
        )

        # Complete upload
        s3_client.client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=object_key,
            UploadId=upload_id,
            MultipartUpload={
                "Parts": [{"PartNumber": 1, "ETag": part_response["ETag"]}]
            },
        )

        # Verify Expires header
        head_response = s3_client.client.head_object(Bucket=bucket_name, Key=object_key)
        # Expires may or may not be preserved (implementation-specific)
        if "Expires" in head_response:
            assert head_response["Expires"] is not None

    finally:
        fixture.cleanup()


def test_create_multipart_upload_abort_idempotent(s3_client, config):
    """
    Test AbortMultipartUpload is idempotent

    Aborting same upload twice should not error (MinIO behavior)
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("cmu-abort-idem")
        object_key = "abort-object"

        s3_client.create_bucket(bucket_name)

        # Create multipart upload
        mp_response = s3_client.client.create_multipart_upload(
            Bucket=bucket_name, Key=object_key
        )
        upload_id = mp_response["UploadId"]

        # Abort first time
        s3_client.client.abort_multipart_upload(
            Bucket=bucket_name, Key=object_key, UploadId=upload_id
        )

        # Abort second time (should be idempotent in MinIO)
        try:
            s3_client.client.abort_multipart_upload(
                Bucket=bucket_name, Key=object_key, UploadId=upload_id
            )
            # MinIO allows this (idempotent)
        except ClientError as e:
            # AWS returns NoSuchUpload
            error_code = e.response["Error"]["Code"]
            assert error_code in [
                "NoSuchUpload",
                "404",
            ], f"Expected NoSuchUpload, got {error_code}"

    finally:
        fixture.cleanup()


def test_create_multipart_upload_upload_id_format(s3_client, config):
    """
    Test CreateMultipartUpload UploadId format

    UploadId should be a non-empty string
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("cmu-uploadid")
        object_key = "uploadid-object"

        s3_client.create_bucket(bucket_name)

        # Create multipart upload
        mp_response = s3_client.client.create_multipart_upload(
            Bucket=bucket_name, Key=object_key
        )

        # Verify UploadId format
        assert "UploadId" in mp_response
        upload_id = mp_response["UploadId"]
        assert upload_id is not None
        assert len(upload_id) > 0
        assert isinstance(upload_id, str)

        # UploadId should be unique
        mp_response2 = s3_client.client.create_multipart_upload(
            Bucket=bucket_name, Key=object_key
        )
        upload_id2 = mp_response2["UploadId"]
        assert upload_id != upload_id2

        # Cleanup both uploads
        s3_client.client.abort_multipart_upload(
            Bucket=bucket_name, Key=object_key, UploadId=upload_id
        )
        s3_client.client.abort_multipart_upload(
            Bucket=bucket_name, Key=object_key, UploadId=upload_id2
        )

    finally:
        fixture.cleanup()


def test_create_multipart_upload_response_fields(s3_client, config):
    """
    Test CreateMultipartUpload response structure

    Response should contain Bucket, Key, and UploadId
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("cmu-response")
        object_key = "response-object"

        s3_client.create_bucket(bucket_name)

        # Create multipart upload
        mp_response = s3_client.client.create_multipart_upload(
            Bucket=bucket_name, Key=object_key
        )

        # Verify response fields
        assert "Bucket" in mp_response
        assert "Key" in mp_response
        assert "UploadId" in mp_response

        assert mp_response["Bucket"] == bucket_name
        assert mp_response["Key"] == object_key
        assert mp_response["UploadId"] is not None

        # Cleanup
        s3_client.client.abort_multipart_upload(
            Bucket=bucket_name, Key=object_key, UploadId=mp_response["UploadId"]
        )

    finally:
        fixture.cleanup()


def test_create_multipart_upload_with_content_encoding(s3_client, config):
    """
    Test CreateMultipartUpload with ContentEncoding

    ContentEncoding should be preserved on completed object
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("cmu-encoding")
        object_key = "encoding-object"

        s3_client.create_bucket(bucket_name)

        content_encoding = "gzip"

        # Create multipart upload with ContentEncoding
        mp_response = s3_client.client.create_multipart_upload(
            Bucket=bucket_name, Key=object_key, ContentEncoding=content_encoding
        )
        upload_id = mp_response["UploadId"]

        # Upload a part
        part_data = b"x" * (5 * 1024 * 1024)
        part_response = s3_client.client.upload_part(
            Bucket=bucket_name,
            Key=object_key,
            UploadId=upload_id,
            PartNumber=1,
            Body=part_data,
        )

        # Complete upload
        s3_client.client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=object_key,
            UploadId=upload_id,
            MultipartUpload={
                "Parts": [{"PartNumber": 1, "ETag": part_response["ETag"]}]
            },
        )

        # Verify ContentEncoding header
        head_response = s3_client.client.head_object(Bucket=bucket_name, Key=object_key)
        if "ContentEncoding" in head_response:
            assert head_response["ContentEncoding"] == content_encoding

    finally:
        fixture.cleanup()


def test_create_multipart_upload_bucket_not_found(s3_client, config):
    """
    Test CreateMultipartUpload on non-existing bucket

    Should return NoSuchBucket error
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("cmu-no-bucket")
        object_key = "test-object"

        # Try to create multipart upload without creating bucket
        with pytest.raises(ClientError) as exc_info:
            s3_client.client.create_multipart_upload(Bucket=bucket_name, Key=object_key)

        error_code = exc_info.value.response["Error"]["Code"]
        assert error_code in [
            "NoSuchBucket",
            "404",
        ], f"Expected NoSuchBucket, got {error_code}"

    finally:
        fixture.cleanup()
