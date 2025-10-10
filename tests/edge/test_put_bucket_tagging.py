#!/usr/bin/env python3
"""
S3 PutBucketTagging Tests

Tests PutBucketTagging API operations:
- PutBucketTagging error conditions
- Tag validation (key/value length, duplicate keys, count limits)
- GetBucketTagging to verify settings
- DeleteBucketTagging

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


def test_put_bucket_tagging_non_existing_bucket(s3_client, config):
    """
    Test PutBucketTagging on non-existing bucket

    Should return NoSuchBucket error
    """
    fixture = TestFixture(s3_client, config)

    try:
        # Generate bucket name but don't create it
        bucket_name = fixture.generate_bucket_name("tag-no-bucket")

        # Try PutBucketTagging on non-existing bucket
        with pytest.raises(ClientError) as exc_info:
            s3_client.client.put_bucket_tagging(
                Bucket=bucket_name, Tagging={"TagSet": []}
            )

        error_code = exc_info.value.response["Error"]["Code"]
        assert error_code in [
            "NoSuchBucket",
            "404",
        ], f"Expected NoSuchBucket, got {error_code}"

    finally:
        fixture.cleanup()


def test_put_bucket_tagging_long_tags(s3_client, config):
    """
    Test PutBucketTagging with overly long tag keys/values

    Tag keys max 128 chars, values max 256 chars
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("tag-long")
        s3_client.create_bucket(bucket_name)

        # Test key too long (>128 characters)
        long_key = "a" * 200
        with pytest.raises(ClientError) as exc_info:
            s3_client.client.put_bucket_tagging(
                Bucket=bucket_name,
                Tagging={"TagSet": [{"Key": long_key, "Value": "val"}]},
            )

        error_code = exc_info.value.response["Error"]["Code"]
        assert error_code in [
            "InvalidTag",
            "InvalidArgument",
            "ValidationException",
            "MalformedXML",  # MinIO returns this
        ], f"Expected InvalidTag, got {error_code}"

        # Test value too long (>256 characters)
        long_value = "a" * 300
        with pytest.raises(ClientError) as exc_info:
            s3_client.client.put_bucket_tagging(
                Bucket=bucket_name,
                Tagging={"TagSet": [{"Key": "key", "Value": long_value}]},
            )

        error_code = exc_info.value.response["Error"]["Code"]
        assert error_code in [
            "InvalidTag",
            "InvalidArgument",
            "ValidationException",
            "MalformedXML",  # MinIO returns this
        ], f"Expected InvalidTag, got {error_code}"

    finally:
        fixture.cleanup()


def test_put_bucket_tagging_duplicate_keys(s3_client, config):
    """
    Test PutBucketTagging with duplicate tag keys

    Should return error - tag keys must be unique
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("tag-dup")
        s3_client.create_bucket(bucket_name)

        # Try PutBucketTagging with duplicate keys
        with pytest.raises(ClientError) as exc_info:
            s3_client.client.put_bucket_tagging(
                Bucket=bucket_name,
                Tagging={
                    "TagSet": [
                        {"Key": "key", "Value": "value"},
                        {"Key": "key", "Value": "value-1"},  # Duplicate key
                        {"Key": "key-1", "Value": "value-2"},
                        {"Key": "key-2", "Value": "value-3"},
                    ]
                },
            )

        error_code = exc_info.value.response["Error"]["Code"]
        assert error_code in [
            "InvalidTag",
            "InvalidArgument",
            "MalformedXML",  # MinIO returns this for duplicate keys
        ], f"Expected InvalidTag, got {error_code}"

    finally:
        fixture.cleanup()


def test_put_bucket_tagging_tag_count_limit(s3_client, config):
    """
    Test PutBucketTagging with too many tags

    Maximum 50 tags per bucket
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("tag-limit")
        s3_client.create_bucket(bucket_name)

        # Create 51 tags (exceeds 50 tag limit)
        tag_set = [{"Key": f"key-{i}", "Value": f"value-{i}"} for i in range(51)]

        # Try PutBucketTagging with too many tags
        with pytest.raises(ClientError) as exc_info:
            s3_client.client.put_bucket_tagging(
                Bucket=bucket_name, Tagging={"TagSet": tag_set}
            )

        error_code = exc_info.value.response["Error"]["Code"]
        assert error_code in [
            "InvalidTag",
            "BadRequest",
            "InvalidArgument",
            "MalformedXML",  # MinIO returns this for too many tags
        ], f"Expected InvalidTag/BadRequest, got {error_code}"

    finally:
        fixture.cleanup()


def test_put_bucket_tagging_success(s3_client, config):
    """
    Test PutBucketTagging with valid tags

    Should succeed and tags should be retrievable
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("tag-success")
        s3_client.create_bucket(bucket_name)

        # Put bucket tags
        s3_client.client.put_bucket_tagging(
            Bucket=bucket_name,
            Tagging={
                "TagSet": [
                    {"Key": "key1", "Value": "val1"},
                    {"Key": "key2", "Value": "val2"},
                ]
            },
        )

        # Verify with GetBucketTagging
        tag_response = s3_client.client.get_bucket_tagging(Bucket=bucket_name)

        # Verify tags
        assert "TagSet" in tag_response
        tags_dict = {tag["Key"]: tag["Value"] for tag in tag_response["TagSet"]}
        assert tags_dict == {"key1": "val1", "key2": "val2"}

    finally:
        fixture.cleanup()


def test_put_bucket_tagging_success_status(s3_client, config):
    """
    Test PutBucketTagging response status code

    Should return 200 OK or 204 No Content
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("tag-status")
        s3_client.create_bucket(bucket_name)

        # Put bucket tags
        response = s3_client.client.put_bucket_tagging(
            Bucket=bucket_name,
            Tagging={"TagSet": [{"Key": "key", "Value": "val"}]},
        )

        # Should return 200 or 204
        status_code = response["ResponseMetadata"]["HTTPStatusCode"]
        assert status_code in [200, 204], f"Expected 200/204, got {status_code}"

    finally:
        fixture.cleanup()


def test_get_bucket_tagging_non_existing_bucket(s3_client, config):
    """
    Test GetBucketTagging on non-existing bucket

    Should return NoSuchBucket error (MinIO may return NoSuchTagSet)
    """
    fixture = TestFixture(s3_client, config)

    try:
        # Generate bucket name but don't create it
        bucket_name = fixture.generate_bucket_name("get-tag-no-bucket")

        # Try GetBucketTagging on non-existing bucket
        with pytest.raises(ClientError) as exc_info:
            s3_client.client.get_bucket_tagging(Bucket=bucket_name)

        error_code = exc_info.value.response["Error"]["Code"]
        assert error_code in [
            "NoSuchBucket",
            "404",
            "NoSuchTagSet",  # MinIO returns this instead of NoSuchBucket
            "NoSuchTagSetError",
        ], f"Expected NoSuchBucket, got {error_code}"

    finally:
        fixture.cleanup()


def test_get_bucket_tagging_no_tags(s3_client, config):
    """
    Test GetBucketTagging on bucket with no tags

    Should return NoSuchTagSet error
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("get-tag-empty")
        s3_client.create_bucket(bucket_name)

        # Try GetBucketTagging on bucket with no tags
        with pytest.raises(ClientError) as exc_info:
            s3_client.client.get_bucket_tagging(Bucket=bucket_name)

        error_code = exc_info.value.response["Error"]["Code"]
        assert error_code in [
            "NoSuchTagSet",
            "NoSuchTagSetError",
            "404",
        ], f"Expected NoSuchTagSet, got {error_code}"

    finally:
        fixture.cleanup()


def test_delete_bucket_tagging_success(s3_client, config):
    """
    Test DeleteBucketTagging

    Should remove all tags from bucket
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("del-tag-success")
        s3_client.create_bucket(bucket_name)

        # Put bucket tags
        s3_client.client.put_bucket_tagging(
            Bucket=bucket_name,
            Tagging={"TagSet": [{"Key": "key1", "Value": "val1"}]},
        )

        # Verify tags exist
        tag_response = s3_client.client.get_bucket_tagging(Bucket=bucket_name)
        assert len(tag_response["TagSet"]) > 0

        # Delete bucket tags
        s3_client.client.delete_bucket_tagging(Bucket=bucket_name)

        # Verify tags are gone
        with pytest.raises(ClientError) as exc_info:
            s3_client.client.get_bucket_tagging(Bucket=bucket_name)

        error_code = exc_info.value.response["Error"]["Code"]
        assert error_code in [
            "NoSuchTagSet",
            "NoSuchTagSetError",
            "404",
        ], f"Expected NoSuchTagSet, got {error_code}"

    finally:
        fixture.cleanup()


def test_put_bucket_tagging_update(s3_client, config):
    """
    Test updating bucket tags

    PutBucketTagging replaces all existing tags
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("tag-update")
        s3_client.create_bucket(bucket_name)

        # Put initial tags
        s3_client.client.put_bucket_tagging(
            Bucket=bucket_name,
            Tagging={"TagSet": [{"Key": "key1", "Value": "val1"}]},
        )

        # Update tags (replaces existing)
        s3_client.client.put_bucket_tagging(
            Bucket=bucket_name,
            Tagging={
                "TagSet": [
                    {"Key": "key2", "Value": "val2"},
                    {"Key": "key3", "Value": "val3"},
                ]
            },
        )

        # Verify updated tags
        tag_response = s3_client.client.get_bucket_tagging(Bucket=bucket_name)
        tags_dict = {tag["Key"]: tag["Value"] for tag in tag_response["TagSet"]}

        # Should have new tags, not old ones
        assert "key1" not in tags_dict
        assert tags_dict == {"key2": "val2", "key3": "val3"}

    finally:
        fixture.cleanup()
