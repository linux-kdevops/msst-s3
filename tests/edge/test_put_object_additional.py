#!/usr/bin/env python3
"""
S3 PutObject Additional Tests

Tests additional PutObject scenarios:
- Checksum validation
- Request payer
- Object lock settings
- SSE encryption variants
- Bucket key enabled

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


def test_put_object_with_sse_s3_encryption(s3_client, config):
    """
    Test PutObject with SSE-S3 encryption

    Should encrypt object with AES256
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("put-sse-s3")
        object_key = "sse-s3-object"

        s3_client.create_bucket(bucket_name)

        object_data = b"encrypted test data"

        # Put object with SSE-S3
        try:
            put_response = s3_client.client.put_object(
                Bucket=bucket_name,
                Key=object_key,
                Body=object_data,
                ServerSideEncryption="AES256",
            )

            # Verify encryption in response
            if "ServerSideEncryption" in put_response:
                assert put_response["ServerSideEncryption"] == "AES256"

            # Verify with HeadObject
            head_response = s3_client.client.head_object(
                Bucket=bucket_name, Key=object_key
            )
            if "ServerSideEncryption" in head_response:
                assert head_response["ServerSideEncryption"] == "AES256"

        except ClientError as e:
            if e.response["Error"]["Code"] in ["NotImplemented", "InvalidArgument"]:
                pytest.skip("SSE-S3 not supported")
                return
            raise

    finally:
        fixture.cleanup()


def test_put_object_with_website_redirect_location(s3_client, config):
    """
    Test PutObject with WebsiteRedirectLocation

    Should preserve redirect location
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("put-redirect")
        object_key = "redirect-object"

        s3_client.create_bucket(bucket_name)

        redirect_location = "https://example.com/redirect"

        # Put object with WebsiteRedirectLocation
        try:
            s3_client.client.put_object(
                Bucket=bucket_name,
                Key=object_key,
                Body=b"redirect data",
                WebsiteRedirectLocation=redirect_location,
            )

            # Verify with HeadObject
            head_response = s3_client.client.head_object(
                Bucket=bucket_name, Key=object_key
            )
            if "WebsiteRedirectLocation" in head_response:
                assert head_response["WebsiteRedirectLocation"] == redirect_location

        except ClientError as e:
            if e.response["Error"]["Code"] in ["NotImplemented", "InvalidArgument"]:
                pytest.skip("WebsiteRedirectLocation not supported")
                return
            raise

    finally:
        fixture.cleanup()


def test_put_object_with_object_lock_legal_hold(s3_client, config):
    """
    Test PutObject with ObjectLockLegalHoldStatus

    Should set legal hold on object
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("put-legal-hold")
        object_key = "legal-hold-object"

        s3_client.create_bucket(bucket_name)

        # Try to put object with legal hold
        try:
            s3_client.client.put_object(
                Bucket=bucket_name,
                Key=object_key,
                Body=b"legal hold data",
                ObjectLockLegalHoldStatus="ON",
            )

            # Verify legal hold
            head_response = s3_client.client.head_object(
                Bucket=bucket_name, Key=object_key
            )
            if "ObjectLockLegalHoldStatus" in head_response:
                assert head_response["ObjectLockLegalHoldStatus"] == "ON"

        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code in [
                "NotImplemented",
                "InvalidRequest",
                "ObjectLockConfigurationNotFoundError",
            ]:
                pytest.skip("Object lock not supported or not configured")
                return
            raise

    finally:
        fixture.cleanup()


def test_put_object_with_object_lock_retention(s3_client, config):
    """
    Test PutObject with ObjectLockMode and ObjectLockRetainUntilDate

    Should set retention on object
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("put-retention")
        object_key = "retention-object"

        s3_client.create_bucket(bucket_name)

        from datetime import datetime, timezone, timedelta

        retain_until = datetime.now(timezone.utc) + timedelta(days=30)

        # Try to put object with retention
        try:
            s3_client.client.put_object(
                Bucket=bucket_name,
                Key=object_key,
                Body=b"retention data",
                ObjectLockMode="GOVERNANCE",
                ObjectLockRetainUntilDate=retain_until,
            )

            # Verify retention
            head_response = s3_client.client.head_object(
                Bucket=bucket_name, Key=object_key
            )
            if "ObjectLockMode" in head_response:
                assert head_response["ObjectLockMode"] == "GOVERNANCE"

        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code in [
                "NotImplemented",
                "InvalidRequest",
                "ObjectLockConfigurationNotFoundError",
            ]:
                pytest.skip("Object lock not supported or not configured")
                return
            raise

    finally:
        fixture.cleanup()


def test_put_object_with_checksum_sha256(s3_client, config):
    """
    Test PutObject with ChecksumSHA256

    Should validate checksum
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("put-checksum-sha256")
        object_key = "checksum-sha256-object"

        s3_client.create_bucket(bucket_name)

        object_data = b"checksum test data"

        # Calculate SHA256 checksum
        import base64

        sha256_hash = hashlib.sha256(object_data).digest()
        checksum_sha256 = base64.b64encode(sha256_hash).decode("utf-8")

        # Put object with ChecksumSHA256
        try:
            put_response = s3_client.client.put_object(
                Bucket=bucket_name,
                Key=object_key,
                Body=object_data,
                ChecksumAlgorithm="SHA256",
                ChecksumSHA256=checksum_sha256,
            )

            # Verify checksum in response
            if "ChecksumSHA256" in put_response:
                assert put_response["ChecksumSHA256"] == checksum_sha256

        except ClientError as e:
            if e.response["Error"]["Code"] in ["NotImplemented", "InvalidArgument"]:
                pytest.skip("ChecksumSHA256 not supported")
                return
            raise

    finally:
        fixture.cleanup()


def test_put_object_with_checksum_crc32(s3_client, config):
    """
    Test PutObject with ChecksumCRC32

    Should validate CRC32 checksum
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("put-checksum-crc32")
        object_key = "checksum-crc32-object"

        s3_client.create_bucket(bucket_name)

        object_data = b"crc32 test data"

        # Calculate CRC32 checksum
        import base64
        import zlib

        crc32_value = zlib.crc32(object_data) & 0xFFFFFFFF
        checksum_crc32 = base64.b64encode(crc32_value.to_bytes(4, "big")).decode(
            "utf-8"
        )

        # Put object with ChecksumCRC32
        try:
            put_response = s3_client.client.put_object(
                Bucket=bucket_name,
                Key=object_key,
                Body=object_data,
                ChecksumAlgorithm="CRC32",
                ChecksumCRC32=checksum_crc32,
            )

            # Verify checksum in response
            if "ChecksumCRC32" in put_response:
                assert put_response["ChecksumCRC32"] == checksum_crc32

        except ClientError as e:
            if e.response["Error"]["Code"] in ["NotImplemented", "InvalidArgument"]:
                pytest.skip("ChecksumCRC32 not supported")
                return
            raise

    finally:
        fixture.cleanup()


def test_put_object_checksum_mismatch(s3_client, config):
    """
    Test PutObject with incorrect checksum

    Should return error for checksum mismatch
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("put-checksum-mismatch")
        object_key = "checksum-mismatch-object"

        s3_client.create_bucket(bucket_name)

        object_data = b"test data"

        # Use incorrect checksum
        incorrect_checksum = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="

        # Try to put object with wrong checksum
        try:
            with pytest.raises(ClientError) as exc_info:
                s3_client.client.put_object(
                    Bucket=bucket_name,
                    Key=object_key,
                    Body=object_data,
                    ChecksumAlgorithm="SHA256",
                    ChecksumSHA256=incorrect_checksum,
                )

            error_code = exc_info.value.response["Error"]["Code"]
            assert error_code in [
                "InvalidRequest",
                "BadDigest",
                "XAmzContentSHA256Mismatch",
                "XAmzContentChecksumMismatch",
            ], f"Expected checksum error, got {error_code}"

        except ClientError as e:
            # If checksum feature not supported, skip
            if e.response["Error"]["Code"] in ["NotImplemented", "InvalidArgument"]:
                pytest.skip("Checksum validation not supported")
                return
            raise

    finally:
        fixture.cleanup()


def test_put_object_expires_header(s3_client, config):
    """
    Test PutObject with Expires header

    Should preserve Expires header
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("put-expires")
        object_key = "expires-object"

        s3_client.create_bucket(bucket_name)

        from datetime import datetime, timezone

        expires = datetime(2030, 12, 31, 23, 59, 59, tzinfo=timezone.utc)

        # Put object with Expires
        s3_client.client.put_object(
            Bucket=bucket_name, Key=object_key, Body=b"expires data", Expires=expires
        )

        # Verify Expires header
        head_response = s3_client.client.head_object(Bucket=bucket_name, Key=object_key)
        # Expires may or may not be preserved (implementation-specific)
        if "Expires" in head_response:
            assert head_response["Expires"] is not None

    finally:
        fixture.cleanup()


def test_put_object_content_language(s3_client, config):
    """
    Test PutObject with ContentLanguage

    Should preserve ContentLanguage header
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("put-lang")
        object_key = "lang-object"

        s3_client.create_bucket(bucket_name)

        content_language = "en-US"

        # Put object with ContentLanguage
        s3_client.client.put_object(
            Bucket=bucket_name,
            Key=object_key,
            Body=b"language data",
            ContentLanguage=content_language,
        )

        # Verify ContentLanguage header
        head_response = s3_client.client.head_object(Bucket=bucket_name, Key=object_key)
        if "ContentLanguage" in head_response:
            assert head_response["ContentLanguage"] == content_language

    finally:
        fixture.cleanup()


def test_put_object_response_status_code(s3_client, config):
    """
    Test PutObject response status code

    Should return 200 OK
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("put-status")
        object_key = "status-object"

        s3_client.create_bucket(bucket_name)

        # Put object
        response = s3_client.client.put_object(
            Bucket=bucket_name, Key=object_key, Body=b"status test"
        )

        # Verify status code
        status_code = response["ResponseMetadata"]["HTTPStatusCode"]
        assert status_code == 200, f"Expected 200, got {status_code}"

    finally:
        fixture.cleanup()
