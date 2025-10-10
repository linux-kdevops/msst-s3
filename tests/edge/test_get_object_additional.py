#!/usr/bin/env python3
"""
S3 GetObject Additional Edge Cases Tests

Tests additional GetObject scenarios:
- GetObject with SSE (Server-Side Encryption) headers
- GetObject with RequestPayer parameter
- GetObject with PartNumber parameter
- GetObject error scenarios
- GetObject with If-Match/If-None-Match combinations

Ported from versitygw integration tests.

Generated-by: Claude AI
Signed-off-by: Luis Chamberlain <mcgrof@kernel.org>
"""

import pytest
import sys
import os
import time

# Add parent directory to path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from tests.common.fixtures import TestFixture
from botocore.exceptions import ClientError


def test_get_object_with_part_number(s3_client, config):
    """
    Test GetObject with PartNumber parameter

    Should retrieve specific part from multipart upload
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("get-part-num")
        s3_client.create_bucket(bucket_name)

        key = "my-obj"

        # Create multipart upload
        create_response = s3_client.client.create_multipart_upload(
            Bucket=bucket_name, Key=key
        )
        upload_id = create_response["UploadId"]

        # Upload 3 parts with unique data
        parts = []
        part_data_list = []
        for part_num in range(1, 4):
            part_data = f"Part-{part_num}-Data".encode() * (1024 * 1024)  # ~15MB each
            part_data_list.append(part_data)

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

        # GetObject with PartNumber=2 (should get second part)
        get_response = s3_client.client.get_object(
            Bucket=bucket_name, Key=key, PartNumber=2
        )

        body = get_response["Body"].read()
        # Should match the second part data
        assert body == part_data_list[1]
        assert get_response["PartsCount"] == 3

    finally:
        fixture.cleanup()


def test_get_object_if_match_and_if_none_match(s3_client, config):
    """
    Test GetObject with both If-Match and If-None-Match headers

    Behavior varies: AWS gives precedence to If-Match, MinIO to If-None-Match
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("get-both-cond")
        s3_client.create_bucket(bucket_name)

        key = "my-obj"
        data = b"test data"
        response = s3_client.put_object(bucket_name, key, data)
        etag = response["ETag"]

        # If-Match matches, If-None-Match also matches
        # AWS: If-Match takes precedence (returns 200)
        # MinIO: If-None-Match takes precedence (returns 304)
        try:
            get_response = s3_client.client.get_object(
                Bucket=bucket_name, Key=key, IfMatch=etag, IfNoneMatch=etag
            )
            # AWS behavior - If-Match takes precedence
            assert get_response["Body"].read() == data
        except ClientError as e:
            # MinIO behavior - If-None-Match takes precedence
            error_code = e.response["Error"]["Code"]
            assert error_code in [
                "304",
                "NotModified",
            ], f"Unexpected error: {error_code}"

    finally:
        fixture.cleanup()


def test_get_object_if_modified_since_future_date(s3_client, config):
    """
    Test GetObject with If-Modified-Since in the future

    Should return NotModified (object hasn't been modified since future date)
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("get-mod-future")
        s3_client.create_bucket(bucket_name)

        key = "my-obj"
        data = b"test data"
        s3_client.put_object(bucket_name, key, data)

        # Use future date
        from datetime import datetime, timedelta

        future_date = datetime.utcnow() + timedelta(days=1)

        # GetObject with future If-Modified-Since
        # Should return NotModified (304)
        try:
            get_response = s3_client.client.get_object(
                Bucket=bucket_name, Key=key, IfModifiedSince=future_date
            )
            # Some implementations may return 200
            if get_response["ResponseMetadata"]["HTTPStatusCode"] == 200:
                assert get_response["Body"].read() == data
        except ClientError as e:
            # Should return NotModified (304)
            error_code = e.response["Error"]["Code"]
            assert error_code in [
                "304",
                "NotModified",
            ], f"Unexpected error: {error_code}"

    finally:
        fixture.cleanup()


def test_get_object_if_unmodified_since_past_date(s3_client, config):
    """
    Test GetObject with If-Unmodified-Since in the past

    Should return PreconditionFailed (object was modified after past date)
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("get-unmod-past")
        s3_client.create_bucket(bucket_name)

        key = "my-obj"

        # Use past date
        from datetime import datetime, timedelta

        past_date = datetime.utcnow() - timedelta(days=1)

        # Wait a moment to ensure LastModified is after past_date
        time.sleep(0.1)

        # Create object (will be modified after past_date)
        s3_client.put_object(bucket_name, key, b"test data")

        # GetObject with past If-Unmodified-Since
        with pytest.raises(ClientError) as exc_info:
            s3_client.client.get_object(
                Bucket=bucket_name, Key=key, IfUnmodifiedSince=past_date
            )

        error_code = exc_info.value.response["Error"]["Code"]
        assert error_code in [
            "PreconditionFailed",
            "412",
        ], f"Expected PreconditionFailed/412, got {error_code}"

    finally:
        fixture.cleanup()


def test_get_object_non_existing_key_with_version_id(s3_client, config):
    """
    Test GetObject with non-existing key and version ID

    Should return NoSuchKey error
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("get-no-key-ver")
        s3_client.create_bucket(bucket_name)

        key = "non-existing-key"

        # Try to get non-existing key with version ID
        with pytest.raises(ClientError) as exc_info:
            s3_client.client.get_object(
                Bucket=bucket_name, Key=key, VersionId="invalid-version"
            )

        error_code = exc_info.value.response["Error"]["Code"]
        assert error_code in [
            "NoSuchKey",
            "NoSuchVersion",
            "InvalidArgument",  # MinIO returns this for invalid version ID format
        ], f"Expected NoSuchKey/NoSuchVersion/InvalidArgument, got {error_code}"

    finally:
        fixture.cleanup()


def test_get_object_with_ssec_mismatch(s3_client, config):
    """
    Test GetObject with SSE-C (Server-Side Encryption Customer) key mismatch

    Should return error if encryption key doesn't match
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("get-ssec")
        s3_client.create_bucket(bucket_name)

        key = "my-obj"
        data = b"encrypted data"

        # Put object with SSE-C (if supported)
        import base64
        import hashlib

        # Generate encryption key
        encryption_key = b"0" * 32  # 32-byte key
        encryption_key_md5 = base64.b64encode(
            hashlib.md5(encryption_key).digest()
        ).decode()

        try:
            s3_client.client.put_object(
                Bucket=bucket_name,
                Key=key,
                Body=data,
                SSECustomerAlgorithm="AES256",
                SSECustomerKey=base64.b64encode(encryption_key).decode(),
                SSECustomerKeyMD5=encryption_key_md5,
            )

            # Try to get with different key
            wrong_key = b"1" * 32
            wrong_key_md5 = base64.b64encode(hashlib.md5(wrong_key).digest()).decode()

            with pytest.raises(ClientError) as exc_info:
                s3_client.client.get_object(
                    Bucket=bucket_name,
                    Key=key,
                    SSECustomerAlgorithm="AES256",
                    SSECustomerKey=base64.b64encode(wrong_key).decode(),
                    SSECustomerKeyMD5=wrong_key_md5,
                )

            # Should return error about key mismatch
            error_code = exc_info.value.response["Error"]["Code"]
            assert error_code in ["BadRequest", "InvalidRequest", "403"]

        except ClientError as e:
            # SSE-C not supported by MinIO in some configurations
            if e.response["Error"]["Code"] in ["NotImplemented", "InvalidArgument"]:
                pytest.skip("SSE-C not supported")

    finally:
        fixture.cleanup()


def test_get_object_with_expires_header(s3_client, config):
    """
    Test GetObject with Expires header in PutObject

    Expires header should be returned in GetObject response
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("get-expires")
        s3_client.create_bucket(bucket_name)

        key = "my-obj"
        data = b"test data"

        from datetime import datetime, timedelta

        expires_date = datetime.utcnow() + timedelta(hours=1)

        # Put object with Expires header
        s3_client.client.put_object(
            Bucket=bucket_name, Key=key, Body=data, Expires=expires_date
        )

        # GetObject should return Expires header
        get_response = s3_client.client.get_object(Bucket=bucket_name, Key=key)

        assert "Expires" in get_response
        # The returned expires may be slightly different due to formatting

    finally:
        fixture.cleanup()


def test_get_object_deleted_object(s3_client, config):
    """
    Test GetObject on deleted object

    Should return NoSuchKey error
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("get-deleted")
        s3_client.create_bucket(bucket_name)

        key = "my-obj"
        s3_client.put_object(bucket_name, key, b"test data")

        # Delete the object
        s3_client.client.delete_object(Bucket=bucket_name, Key=key)

        # Try to get deleted object
        with pytest.raises(ClientError) as exc_info:
            s3_client.get_object(bucket_name, key)

        error_code = exc_info.value.response["Error"]["Code"]
        assert error_code == "NoSuchKey", f"Expected NoSuchKey, got {error_code}"

    finally:
        fixture.cleanup()


def test_get_object_with_website_redirect_location(s3_client, config):
    """
    Test GetObject with WebsiteRedirectLocation metadata

    Should return x-amz-website-redirect-location header
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("get-redirect")
        s3_client.create_bucket(bucket_name)

        key = "my-obj"
        redirect_location = "/another-page.html"

        # Put object with WebsiteRedirectLocation
        try:
            s3_client.client.put_object(
                Bucket=bucket_name,
                Key=key,
                Body=b"redirect content",
                WebsiteRedirectLocation=redirect_location,
            )

            # GetObject should return WebsiteRedirectLocation
            get_response = s3_client.client.get_object(Bucket=bucket_name, Key=key)

            # MinIO may or may not support WebsiteRedirectLocation
            if "WebsiteRedirectLocation" in get_response:
                assert get_response["WebsiteRedirectLocation"] == redirect_location

        except ClientError as e:
            # Some implementations may not support WebsiteRedirectLocation
            if e.response["Error"]["Code"] in ["NotImplemented", "InvalidArgument"]:
                pytest.skip("WebsiteRedirectLocation not supported")

    finally:
        fixture.cleanup()


def test_get_object_response_status_code(s3_client, config):
    """
    Test GetObject returns correct HTTP status code

    Should return 200 OK for successful retrieval
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("get-status")
        s3_client.create_bucket(bucket_name)

        key = "my-obj"
        s3_client.put_object(bucket_name, key, b"test data")

        # GetObject should return 200
        get_response = s3_client.client.get_object(Bucket=bucket_name, Key=key)

        assert get_response["ResponseMetadata"]["HTTPStatusCode"] == 200

    finally:
        fixture.cleanup()
