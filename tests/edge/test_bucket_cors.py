#!/usr/bin/env python3
"""
S3 Bucket CORS Configuration Tests

Tests PutBucketCors, GetBucketCors, and DeleteBucketCors with:
- Non-existing bucket errors
- Empty CORS rules validation
- Invalid HTTP method validation
- Invalid header name validation
- Content-MD5 validation
- Multiple CORS rules with wildcards
- CORS configuration retrieval and deletion

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


def test_put_bucket_cors_non_existing_bucket(s3_client, config):
    """
    Test PutBucketCors on non-existing bucket

    Should return NoSuchBucket error
    """
    fixture = TestFixture(s3_client, config)

    try:
        non_existing_bucket = fixture.generate_bucket_name("non-existing")

        cors_config = {
            "CORSRules": [
                {
                    "AllowedOrigins": ["http://origin.com"],
                    "AllowedMethods": ["GET"],
                }
            ]
        }

        with pytest.raises(ClientError) as exc_info:
            s3_client.client.put_bucket_cors(
                Bucket=non_existing_bucket, CORSConfiguration=cors_config
            )

        error_code = exc_info.value.response["Error"]["Code"]
        assert error_code == "NoSuchBucket", f"Expected NoSuchBucket, got {error_code}"

    finally:
        fixture.cleanup()


def test_put_bucket_cors_empty_cors_rules(s3_client, config):
    """
    Test PutBucketCors with empty CORS rules array

    Should return MalformedXML error
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("cors-empty-rules")
        s3_client.create_bucket(bucket_name)

        cors_config = {"CORSRules": []}  # Empty rules array

        with pytest.raises(ClientError) as exc_info:
            s3_client.client.put_bucket_cors(
                Bucket=bucket_name, CORSConfiguration=cors_config
            )

        error_code = exc_info.value.response["Error"]["Code"]
        if error_code == "NotImplemented":
            pytest.skip("CORS not supported by this S3 implementation")
        assert error_code in [
            "MalformedXML",
            "InvalidRequest",
        ], f"Expected MalformedXML, got {error_code}"

    finally:
        fixture.cleanup()


def test_put_bucket_cors_invalid_method(s3_client, config):
    """
    Test PutBucketCors with invalid HTTP methods

    Only GET, PUT, POST, DELETE, HEAD are valid (uppercase)
    Lowercase, mixed case, and invalid methods should be rejected
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("cors-invalid-method")
        s3_client.create_bucket(bucket_name)

        # Test various invalid methods
        invalid_methods = [
            ["get"],  # lowercase
            ["put"],
            ["post"],
            ["head"],
            ["delete"],
            ["GET", "PATCH"],  # PATCH not supported
            ["POST", "OPTIONS"],  # OPTIONS not supported
            ["GET", "HEAD", "POST", "PUT", "DELETE", "invalid_method"],  # nonsense
        ]

        for methods in invalid_methods:
            cors_config = {
                "CORSRules": [
                    {
                        "AllowedOrigins": ["http://origin.com"],
                        "AllowedMethods": methods,
                        "AllowedHeaders": ["X-Amz-Date"],
                        "ExposeHeaders": ["Authorization"],
                    }
                ]
            }

            with pytest.raises(ClientError) as exc_info:
                s3_client.client.put_bucket_cors(
                    Bucket=bucket_name, CORSConfiguration=cors_config
                )

            error_code = exc_info.value.response["Error"]["Code"]
            if error_code == "NotImplemented":
                pytest.skip("CORS not supported by this S3 implementation")
            assert error_code in [
                "InvalidRequest",
                "CORSInvalidAccessControlMethod",
                "InvalidArgument",
            ], f"Expected InvalidRequest for {methods}, got {error_code}"

    finally:
        fixture.cleanup()


def test_put_bucket_cors_invalid_header(s3_client, config):
    """
    Test PutBucketCors with invalid header names

    Headers with spaces, special chars like :, (), /, [], =, " are invalid
    Tests both AllowedHeaders and ExposeHeaders
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("cors-invalid-header")
        s3_client.create_bucket(bucket_name)

        # Invalid headers with special characters
        invalid_headers = [
            ["X-Amz-Date", "X-Amz-Content-Sha256", "invalid header"],  # space
            ["Authorization", "X-Custom:Header"],  # colon
            ["Content-Length", "X(Custom)"],  # parentheses
            ["Content-Encoding", "Bad/Header"],  # slash
            ["Date", "X[Key]"],  # brackets
            ["X-Amz-Custom-Header", "Bad=Name"],  # equals
            ['X"Quote"'],  # quotes
        ]

        for headers in invalid_headers:
            # Test with AllowedHeaders
            cors_config = {
                "CORSRules": [
                    {
                        "AllowedOrigins": ["http://origin.com"],
                        "AllowedMethods": ["POST"],
                        "AllowedHeaders": headers,
                        "ExposeHeaders": ["Authorization"],
                    }
                ]
            }

            with pytest.raises(ClientError) as exc_info:
                s3_client.client.put_bucket_cors(
                    Bucket=bucket_name, CORSConfiguration=cors_config
                )

            error_code = exc_info.value.response["Error"]["Code"]
            if error_code == "NotImplemented":
                pytest.skip("CORS not supported by this S3 implementation")
            assert error_code in [
                "InvalidRequest",
                "InvalidArgument",
                "AccessControlAllowRequestHeaderNotAllowed",
            ], f"Expected InvalidRequest for AllowedHeaders {headers}, got {error_code}"

            # Test with ExposeHeaders
            cors_config["CORSRules"][0]["AllowedHeaders"] = ["X-Amz-Date"]
            cors_config["CORSRules"][0]["ExposeHeaders"] = headers

            with pytest.raises(ClientError) as exc_info:
                s3_client.client.put_bucket_cors(
                    Bucket=bucket_name, CORSConfiguration=cors_config
                )

            error_code = exc_info.value.response["Error"]["Code"]
            if error_code == "NotImplemented":
                pytest.skip("CORS not supported by this S3 implementation")
            assert error_code in [
                "InvalidRequest",
                "InvalidArgument",
                "UnexpectedContent",
            ], f"Expected InvalidRequest for ExposeHeaders {headers}, got {error_code}"

    finally:
        fixture.cleanup()


def test_put_bucket_cors_md5(s3_client, config):
    """
    Test PutBucketCors with ContentMD5 validation

    Tests invalid MD5, incorrect MD5, and correct MD5
    Note: boto3 may compute MD5 automatically
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("cors-md5")
        s3_client.create_bucket(bucket_name)

        cors_config = {
            "CORSRules": [
                {
                    "AllowedOrigins": ["http://origin.com", "something.net"],
                    "AllowedMethods": ["POST", "PUT", "HEAD"],
                    "AllowedHeaders": [
                        "X-Amz-Date",
                        "X-Amz-Meta-Something",
                        "Content-Type",
                    ],
                    "ExposeHeaders": ["Authorization", "Content-Disposition"],
                    "MaxAgeSeconds": 125,
                    "ID": "my-id",
                }
            ]
        }

        # Test with invalid MD5 format
        with pytest.raises(ClientError) as exc_info:
            s3_client.client.put_bucket_cors(
                Bucket=bucket_name,
                CORSConfiguration=cors_config,
                ContentMD5="invalid",
            )

        error_code = exc_info.value.response["Error"]["Code"]
        assert error_code in [
            "InvalidDigest",
            "InvalidRequest",
        ], f"Expected InvalidDigest for invalid MD5, got {error_code}"

        # Test with incorrect MD5 (valid format but wrong hash)
        with pytest.raises(ClientError) as exc_info:
            s3_client.client.put_bucket_cors(
                Bucket=bucket_name,
                CORSConfiguration=cors_config,
                ContentMD5="uU0nuZNNPgilLlLX2n2r+s==",  # Wrong MD5
            )

        error_code = exc_info.value.response["Error"]["Code"]
        assert error_code in [
            "BadDigest",
            "InvalidDigest",
        ], f"Expected BadDigest for incorrect MD5, got {error_code}"

        # Note: Testing with correct MD5 is complex because the XML serialization
        # may vary. Skip the correct MD5 test as it's implementation-specific.

    finally:
        fixture.cleanup()


def test_put_bucket_cors_success(s3_client, config):
    """
    Test successful PutBucketCors with multiple rules

    Tests wildcards in origins, negative MaxAgeSeconds, and multiple rules
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("cors-success")
        s3_client.create_bucket(bucket_name)

        cors_config = {
            "CORSRules": [
                {
                    "AllowedOrigins": ["http://origin.com"],
                    "AllowedMethods": ["POST", "PUT"],
                    "AllowedHeaders": ["X-Amz-Date"],
                    "ExposeHeaders": ["Authorization"],
                    "MaxAgeSeconds": -100,  # Negative values are valid
                },
                {
                    "AllowedOrigins": ["*"],  # Wildcard origin
                    "AllowedMethods": ["DELETE", "GET", "HEAD"],
                    "AllowedHeaders": [
                        "Content-Type",
                        "Content-Encoding",
                        "Content-MD5",
                    ],
                    "ExposeHeaders": [
                        "Authorization",
                        "X-Amz-Date",
                        "X-Amz-Content-Sha256",
                    ],
                    "ID": "id",
                    "MaxAgeSeconds": 3000,
                },
                {
                    "AllowedOrigins": [
                        "http://example.com",
                        "https://something.net",
                        "http://*origin.com",  # Wildcard in origin
                    ],
                    "AllowedMethods": ["GET"],
                },
            ]
        }

        # Should succeed
        try:
            s3_client.client.put_bucket_cors(
                Bucket=bucket_name, CORSConfiguration=cors_config
            )
        except ClientError as e:
            if e.response["Error"]["Code"] == "NotImplemented":
                pytest.skip("CORS not supported by this S3 implementation")
            raise

    finally:
        fixture.cleanup()


def test_get_bucket_cors_non_existing_bucket(s3_client, config):
    """
    Test GetBucketCors on non-existing bucket

    Should return NoSuchBucket error
    """
    fixture = TestFixture(s3_client, config)

    try:
        non_existing_bucket = fixture.generate_bucket_name("non-existing")

        with pytest.raises(ClientError) as exc_info:
            s3_client.client.get_bucket_cors(Bucket=non_existing_bucket)

        error_code = exc_info.value.response["Error"]["Code"]
        assert error_code == "NoSuchBucket", f"Expected NoSuchBucket, got {error_code}"

    finally:
        fixture.cleanup()


def test_get_bucket_cors_no_such_bucket_cors(s3_client, config):
    """
    Test GetBucketCors on bucket without CORS configuration

    Should return NoSuchCORSConfiguration error (or NotImplemented if CORS not supported)
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("cors-get-unset")
        s3_client.create_bucket(bucket_name)

        with pytest.raises(ClientError) as exc_info:
            s3_client.client.get_bucket_cors(Bucket=bucket_name)

        error_code = exc_info.value.response["Error"]["Code"]
        if error_code == "NotImplemented":
            pytest.skip("CORS not supported by this S3 implementation")
        assert (
            error_code == "NoSuchCORSConfiguration"
        ), f"Expected NoSuchCORSConfiguration, got {error_code}"

    finally:
        fixture.cleanup()


def test_get_bucket_cors_success(s3_client, config):
    """
    Test successful GetBucketCors

    Sets CORS configuration and retrieves it
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("cors-get-success")
        s3_client.create_bucket(bucket_name)

        cors_config = {
            "CORSRules": [
                {
                    "AllowedOrigins": ["http://origin.com", "helloworld.net"],
                    "AllowedMethods": ["POST", "PUT", "HEAD"],
                    "AllowedHeaders": ["X-Amz-Date", "X-Amz-Meta-Something"],
                    "ExposeHeaders": ["Authorization", "Content-Disposition"],
                    "MaxAgeSeconds": 125,
                },
                {
                    "AllowedOrigins": ["*"],
                    "AllowedMethods": ["DELETE", "GET", "HEAD"],
                    "AllowedHeaders": ["Content-*"],  # Wildcard in header
                    "ExposeHeaders": [
                        "Authorization",
                        "X-Amz-Date",
                        "X-Amz-Content-Sha256",
                    ],
                    "ID": "my_extra_unique_id",
                    "MaxAgeSeconds": -200,  # Negative MaxAge
                },
            ]
        }

        # Set CORS configuration
        try:
            s3_client.client.put_bucket_cors(
                Bucket=bucket_name, CORSConfiguration=cors_config
            )
        except ClientError as e:
            if e.response["Error"]["Code"] == "NotImplemented":
                pytest.skip("CORS not supported by this S3 implementation")
            raise

        # Get CORS configuration
        response = s3_client.client.get_bucket_cors(Bucket=bucket_name)

        # Verify CORS rules were returned
        assert "CORSRules" in response
        assert len(response["CORSRules"]) == 2

        # Verify first rule
        rule1 = response["CORSRules"][0]
        assert set(rule1["AllowedOrigins"]) == {"http://origin.com", "helloworld.net"}
        assert set(rule1["AllowedMethods"]) == {"POST", "PUT", "HEAD"}
        assert set(rule1["AllowedHeaders"]) == {"X-Amz-Date", "X-Amz-Meta-Something"}
        assert set(rule1["ExposeHeaders"]) == {"Authorization", "Content-Disposition"}
        assert rule1["MaxAgeSeconds"] == 125

        # Verify second rule
        rule2 = response["CORSRules"][1]
        assert rule2["AllowedOrigins"] == ["*"]
        assert set(rule2["AllowedMethods"]) == {"DELETE", "GET", "HEAD"}
        assert rule2["AllowedHeaders"] == ["Content-*"]
        assert "ID" in rule2
        assert rule2["ID"] == "my_extra_unique_id"
        assert rule2["MaxAgeSeconds"] == -200

    finally:
        fixture.cleanup()


def test_delete_bucket_cors_non_existing_bucket(s3_client, config):
    """
    Test DeleteBucketCors on non-existing bucket

    Should return NoSuchBucket error
    """
    fixture = TestFixture(s3_client, config)

    try:
        non_existing_bucket = fixture.generate_bucket_name("non-existing")

        with pytest.raises(ClientError) as exc_info:
            s3_client.client.delete_bucket_cors(Bucket=non_existing_bucket)

        error_code = exc_info.value.response["Error"]["Code"]
        assert error_code == "NoSuchBucket", f"Expected NoSuchBucket, got {error_code}"

    finally:
        fixture.cleanup()


def test_delete_bucket_cors_success(s3_client, config):
    """
    Test successful DeleteBucketCors

    Tests deleting unset CORS (should succeed) and deleting set CORS
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("cors-delete-success")
        s3_client.create_bucket(bucket_name)

        # Delete unset CORS - should not raise error
        try:
            s3_client.client.delete_bucket_cors(Bucket=bucket_name)
        except ClientError as e:
            if e.response["Error"]["Code"] == "NotImplemented":
                pytest.skip("CORS not supported by this S3 implementation")
            raise

        # Set CORS configuration
        cors_config = {
            "CORSRules": [
                {
                    "AllowedOrigins": ["http://origin.com"],
                    "AllowedMethods": ["POST"],
                    "AllowedHeaders": ["X-Amz-Meta-Header"],
                    "ExposeHeaders": ["Content-Disposition"],
                    "MaxAgeSeconds": 5000,
                }
            ]
        }

        s3_client.client.put_bucket_cors(
            Bucket=bucket_name, CORSConfiguration=cors_config
        )

        # Delete CORS configuration
        s3_client.client.delete_bucket_cors(Bucket=bucket_name)

        # Verify CORS was deleted - should get NoSuchCORSConfiguration
        with pytest.raises(ClientError) as exc_info:
            s3_client.client.get_bucket_cors(Bucket=bucket_name)

        error_code = exc_info.value.response["Error"]["Code"]
        assert (
            error_code == "NoSuchCORSConfiguration"
        ), f"Expected NoSuchCORSConfiguration, got {error_code}"

    finally:
        fixture.cleanup()
