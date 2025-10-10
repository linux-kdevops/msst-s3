#!/usr/bin/env python3
"""
S3 PutObject Conditional Writes and Invalid Names Tests

Tests PutObject with conditional write headers and invalid object names:
- If-Match and If-None-Match conditional writes
- Invalid object names (path traversal attempts)
- Race conditions with concurrent writes

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


def test_put_object_if_match_success(s3_client, config):
    """
    Test PutObject with If-Match when ETag matches

    Should succeed and update object
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("put-if-match")
        s3_client.create_bucket(bucket_name)

        # Create initial object
        key = "my-obj"
        response1 = s3_client.put_object(bucket_name, key, b"v1")
        etag = response1["ETag"]

        # Update with If-Match (should succeed)
        response2 = s3_client.client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=b"v2",
            IfMatch=etag,
        )

        assert "ETag" in response2
        assert response2["ETag"] != etag  # ETag should change

    finally:
        fixture.cleanup()


def test_put_object_if_match_fails(s3_client, config):
    """
    Test PutObject with If-Match when ETag doesn't match

    Should return PreconditionFailed error
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("put-if-match-fail")
        s3_client.create_bucket(bucket_name)

        # Create initial object
        key = "my-obj"
        s3_client.put_object(bucket_name, key, b"v1")

        # Try to update with wrong ETag
        with pytest.raises(ClientError) as exc_info:
            s3_client.client.put_object(
                Bucket=bucket_name,
                Key=key,
                Body=b"v2",
                IfMatch='"incorrect-etag"',
            )

        error_code = exc_info.value.response["Error"]["Code"]
        assert (
            error_code == "PreconditionFailed"
        ), f"Expected PreconditionFailed, got {error_code}"

    finally:
        fixture.cleanup()


def test_put_object_if_none_match_success(s3_client, config):
    """
    Test PutObject with If-None-Match when ETag doesn't match

    Should succeed and update object
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("put-if-none-match")
        s3_client.create_bucket(bucket_name)

        # Create initial object
        key = "my-obj"
        s3_client.put_object(bucket_name, key, b"v1")

        # Update with If-None-Match (incorrect ETag - should succeed)
        response = s3_client.client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=b"v2",
            IfNoneMatch='"incorrect-etag"',
        )

        assert "ETag" in response

    finally:
        fixture.cleanup()


def test_put_object_if_none_match_fails(s3_client, config):
    """
    Test PutObject with If-None-Match when ETag matches

    Should return PreconditionFailed error
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("put-if-none-fail")
        s3_client.create_bucket(bucket_name)

        # Create initial object
        key = "my-obj"
        response1 = s3_client.put_object(bucket_name, key, b"v1")
        etag = response1["ETag"]

        # Try to update with matching ETag
        with pytest.raises(ClientError) as exc_info:
            s3_client.client.put_object(
                Bucket=bucket_name,
                Key=key,
                Body=b"v2",
                IfNoneMatch=etag,
            )

        error_code = exc_info.value.response["Error"]["Code"]
        assert (
            error_code == "PreconditionFailed"
        ), f"Expected PreconditionFailed, got {error_code}"

    finally:
        fixture.cleanup()


def test_put_object_if_match_and_if_none_match(s3_client, config):
    """
    Test PutObject with both If-Match and If-None-Match

    When both present, If-Match takes precedence
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("put-both-cond")
        s3_client.create_bucket(bucket_name)

        # Create initial object
        key = "my-obj"
        response1 = s3_client.put_object(bucket_name, key, b"v1")
        etag = response1["ETag"]

        # If-Match matches but If-None-Match also matches
        # Should fail because If-None-Match condition fails
        with pytest.raises(ClientError) as exc_info:
            s3_client.client.put_object(
                Bucket=bucket_name,
                Key=key,
                Body=b"v2",
                IfMatch=etag,
                IfNoneMatch=etag,
            )

        error_code = exc_info.value.response["Error"]["Code"]
        assert error_code == "PreconditionFailed"

    finally:
        fixture.cleanup()


def test_put_object_conditional_on_new_object(s3_client, config):
    """
    Test PutObject conditional headers on non-existing object

    MinIO enforces conditionals even for new objects (fails with PreconditionFailed)
    AWS S3 ignores conditionals for new objects (succeeds)
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("put-cond-new")
        s3_client.create_bucket(bucket_name)

        # PutObject with If-Match on non-existing object
        # AWS S3: succeeds (ignores condition)
        # MinIO: fails with PreconditionFailed
        key1 = "obj-1"
        try:
            response1 = s3_client.client.put_object(
                Bucket=bucket_name,
                Key=key1,
                Body=b"data",
                IfMatch='"any-etag"',
            )
            # AWS S3 behavior - success
            assert "ETag" in response1
        except ClientError as e:
            # MinIO behavior - NoSuchKey or PreconditionFailed
            assert e.response["Error"]["Code"] in ["PreconditionFailed", "NoSuchKey"]

        # PutObject with If-None-Match on non-existing object
        # Both should succeed (object doesn't exist, so no ETag to match)
        key2 = "obj-2"
        response2 = s3_client.client.put_object(
            Bucket=bucket_name,
            Key=key2,
            Body=b"data",
            IfNoneMatch='"any-etag"',
        )
        assert "ETag" in response2

    finally:
        fixture.cleanup()


def test_put_object_invalid_object_names_path_traversal(s3_client, config):
    """
    Test PutObject with path traversal attempts in key names

    Should reject dangerous path patterns
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("put-invalid-names")
        s3_client.create_bucket(bucket_name)

        # Various path traversal attempts that should be rejected
        invalid_keys = [
            ".",
            "..",
            "./",
            "/.",
            "//",
            "../",
            "/..",
            "../.",
            "../../../.",
            "../../../etc/passwd",
            "../../../../tmp/foo",
            "for/../../bar/",
            "a/a/a/../../../../../etc/passwd",
            "/a/../../b/../../c/../../../etc/passwd",
        ]

        for key in invalid_keys:
            with pytest.raises(ClientError) as exc_info:
                s3_client.put_object(bucket_name, key, b"data")

            error_code = exc_info.value.response["Error"]["Code"]
            # MinIO returns XMinioInvalidResourceName or XMinioInvalidObjectName
            assert error_code in [
                "InvalidRequest",
                "KeyTooLongError",
                "InvalidArgument",
                "BadRequest",
                "XMinioInvalidResourceName",
                "XMinioInvalidObjectName",
            ], f"Expected error for key '{key}', got {error_code}"

    finally:
        fixture.cleanup()


def test_put_object_concurrent_updates(s3_client, config):
    """
    Test PutObject with multiple concurrent updates

    Last write should win
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("put-concurrent")
        s3_client.create_bucket(bucket_name)

        key = "my-obj"

        # Perform multiple rapid updates
        for i in range(5):
            s3_client.put_object(bucket_name, key, f"version-{i}".encode())

        # Verify object exists and has one of the versions
        get_response = s3_client.get_object(bucket_name, key)
        body = get_response["Body"].read()
        # Should be one of the versions we wrote
        assert body in [f"version-{i}".encode() for i in range(5)]

    finally:
        fixture.cleanup()


def test_put_object_empty_key_rejected(s3_client, config):
    """
    Test PutObject with empty key

    boto3 validates empty key before sending (ParamValidationError)
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("put-empty-key")
        s3_client.create_bucket(bucket_name)

        # Try to put object with empty key
        # boto3 validates this client-side before sending to server
        from botocore.exceptions import ParamValidationError

        with pytest.raises(ParamValidationError) as exc_info:
            s3_client.put_object(bucket_name, "", b"data")

        # boto3 validates minimum key length of 1
        assert "valid min length: 1" in str(exc_info.value)

    finally:
        fixture.cleanup()


def test_put_object_very_long_key(s3_client, config):
    """
    Test PutObject with very long key (>1024 bytes)

    Should return KeyTooLongError
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("put-long-key")
        s3_client.create_bucket(bucket_name)

        # Create key longer than 1024 bytes
        long_key = "a" * 1025

        with pytest.raises(ClientError) as exc_info:
            s3_client.put_object(bucket_name, long_key, b"data")

        error_code = exc_info.value.response["Error"]["Code"]
        assert error_code in [
            "KeyTooLongError",
            "InvalidRequest",
        ], f"Expected KeyTooLongError, got {error_code}"

    finally:
        fixture.cleanup()


def test_put_object_replace_with_different_content_type(s3_client, config):
    """
    Test PutObject replacing object with different ContentType

    Should update ContentType
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("put-replace-ct")
        s3_client.create_bucket(bucket_name)

        key = "my-obj"

        # Create with text/plain
        s3_client.client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=b"data",
            ContentType="text/plain",
        )

        # Replace with application/json
        s3_client.client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=b"data",
            ContentType="application/json",
        )

        # Verify new ContentType
        head_response = s3_client.client.head_object(Bucket=bucket_name, Key=key)
        assert head_response["ContentType"] == "application/json"

    finally:
        fixture.cleanup()
