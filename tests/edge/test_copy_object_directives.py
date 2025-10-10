#!/usr/bin/env python3
"""
S3 CopyObject Directives and Edge Cases Tests

Tests CopyObject with metadata/tagging directives and edge cases:
- MetadataDirective (COPY/REPLACE) validation
- TaggingDirective (COPY/REPLACE) validation
- CopySource format variations (with/without leading slash)
- Invalid directive values
- Copy to itself with directives

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


def test_copy_object_copy_to_itself_invalid_metadata_directive(s3_client, config):
    """
    Test CopyObject to itself with invalid MetadataDirective

    Should return InvalidArgument error
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("copy-invalid-dir")
        s3_client.create_bucket(bucket_name)

        # Create source object
        key = "my-obj"
        s3_client.put_object(bucket_name, key, b"test data")

        # Try to copy to itself with invalid directive
        with pytest.raises(ClientError) as exc_info:
            s3_client.client.copy_object(
                Bucket=bucket_name,
                Key=key,
                CopySource=f"{bucket_name}/{key}",
                MetadataDirective="invalid",
            )

        error_code = exc_info.value.response["Error"]["Code"]
        assert error_code in [
            "InvalidArgument",
            "InvalidRequest",
        ], f"Expected InvalidArgument/InvalidRequest, got {error_code}"

    finally:
        fixture.cleanup()


def test_copy_object_invalid_tagging_directive(s3_client, config):
    """
    Test CopyObject with invalid TaggingDirective

    Should return InvalidArgument error
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("copy-invalid-tag-dir")
        s3_client.create_bucket(bucket_name)

        # Create source object
        key = "src-obj"
        s3_client.put_object(bucket_name, key, b"test data")

        # Try to copy with invalid tagging directive
        with pytest.raises(ClientError) as exc_info:
            s3_client.client.copy_object(
                Bucket=bucket_name,
                Key="dest-obj",
                CopySource=f"{bucket_name}/{key}",
                TaggingDirective="invalid",
            )

        error_code = exc_info.value.response["Error"]["Code"]
        assert error_code in [
            "InvalidArgument",
            "InvalidRequest",
        ], f"Expected InvalidArgument/InvalidRequest, got {error_code}"

    finally:
        fixture.cleanup()


def test_copy_object_copy_source_starting_with_slash(s3_client, config):
    """
    Test CopyObject with CopySource starting with slash

    Leading slash should be accepted (e.g., "/bucket/key")
    """
    fixture = TestFixture(s3_client, config)

    try:
        src_bucket = fixture.generate_bucket_name("copy-slash-src")
        dest_bucket = fixture.generate_bucket_name("copy-slash-dest")
        s3_client.create_bucket(src_bucket)
        s3_client.create_bucket(dest_bucket)

        # Create source object with known data
        key = "src-obj"
        data = b"x" * 1234567
        data_hash = hashlib.sha256(data).digest()
        s3_client.put_object(src_bucket, key, data)

        # Copy with leading slash in CopySource
        s3_client.client.copy_object(
            Bucket=dest_bucket,
            Key=key,
            CopySource=f"/{src_bucket}/{key}",  # Leading slash
        )

        # Verify destination
        get_response = s3_client.get_object(dest_bucket, key)
        assert get_response["ContentLength"] == len(data)

        body = get_response["Body"].read()
        assert hashlib.sha256(body).digest() == data_hash

    finally:
        fixture.cleanup()


def test_copy_object_invalid_copy_source_no_slash(s3_client, config):
    """
    Test CopyObject with invalid CopySource (missing slash)

    CopySource must be "bucket/key" or "/bucket/key"
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("copy-no-slash")
        s3_client.create_bucket(bucket_name)

        # Create source object
        key = "src-obj"
        s3_client.put_object(bucket_name, key, b"test data")

        # Try various invalid formats
        invalid_sources = [
            f"{bucket_name}{key}",  # No slash between bucket and key
            bucket_name,  # Just bucket name
            key,  # Just key
        ]

        for source in invalid_sources:
            with pytest.raises(ClientError) as exc_info:
                s3_client.client.copy_object(
                    Bucket=bucket_name,
                    Key="dest-obj",
                    CopySource=source,
                )

            error_code = exc_info.value.response["Error"]["Code"]
            # Various error codes possible depending on format
            assert error_code in [
                "InvalidArgument",
                "InvalidRequest",
                "NoSuchKey",
                "NoSuchBucket",
            ], f"Expected error for '{source}', got {error_code}"

    finally:
        fixture.cleanup()


def test_copy_object_non_existing_dir_object(s3_client, config):
    """
    Test CopyObject with non-existing directory-style object

    Should return NoSuchKey error
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("copy-no-dir")
        s3_client.create_bucket(bucket_name)

        # Try to copy non-existing directory object
        with pytest.raises(ClientError) as exc_info:
            s3_client.client.copy_object(
                Bucket=bucket_name,
                Key="dest-obj",
                CopySource=f"{bucket_name}/non-existing-dir/",
            )

        error_code = exc_info.value.response["Error"]["Code"]
        assert error_code == "NoSuchKey", f"Expected NoSuchKey, got {error_code}"

    finally:
        fixture.cleanup()


def test_copy_object_metadata_directive_copy(s3_client, config):
    """
    Test CopyObject with MetadataDirective=COPY

    Should preserve source metadata (default behavior)
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("copy-meta-copy")
        s3_client.create_bucket(bucket_name)

        # Create source with metadata
        src_key = "src-obj"
        metadata = {"key1": "value1", "key2": "value2"}
        s3_client.client.put_object(
            Bucket=bucket_name,
            Key=src_key,
            Body=b"test data",
            Metadata=metadata,
            ContentType="text/plain",
        )

        # Copy with COPY directive (explicit)
        dest_key = "dest-obj"
        s3_client.client.copy_object(
            Bucket=bucket_name,
            Key=dest_key,
            CopySource=f"{bucket_name}/{src_key}",
            MetadataDirective="COPY",
        )

        # Verify metadata was preserved
        head_response = s3_client.client.head_object(Bucket=bucket_name, Key=dest_key)
        assert head_response["Metadata"] == metadata
        assert head_response["ContentType"] == "text/plain"

    finally:
        fixture.cleanup()


def test_copy_object_metadata_directive_replace(s3_client, config):
    """
    Test CopyObject with MetadataDirective=REPLACE

    Should use new metadata provided in request
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("copy-meta-replace")
        s3_client.create_bucket(bucket_name)

        # Create source with metadata
        src_key = "src-obj"
        s3_client.client.put_object(
            Bucket=bucket_name,
            Key=src_key,
            Body=b"test data",
            Metadata={"old": "metadata"},
            ContentType="text/plain",
        )

        # Copy with REPLACE directive and new metadata
        dest_key = "dest-obj"
        new_metadata = {"new": "metadata", "another": "value"}
        s3_client.client.copy_object(
            Bucket=bucket_name,
            Key=dest_key,
            CopySource=f"{bucket_name}/{src_key}",
            MetadataDirective="REPLACE",
            Metadata=new_metadata,
            ContentType="application/json",
        )

        # Verify new metadata
        head_response = s3_client.client.head_object(Bucket=bucket_name, Key=dest_key)
        assert head_response["Metadata"] == new_metadata
        assert head_response["ContentType"] == "application/json"

    finally:
        fixture.cleanup()


def test_copy_object_tagging_directive_copy(s3_client, config):
    """
    Test CopyObject with TaggingDirective=COPY

    Should preserve source tags
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("copy-tag-copy")
        s3_client.create_bucket(bucket_name)

        # Create source with tags
        src_key = "src-obj"
        s3_client.client.put_object(
            Bucket=bucket_name,
            Key=src_key,
            Body=b"test data",
            Tagging="tag1=value1&tag2=value2",
        )

        # Copy with COPY tagging directive
        dest_key = "dest-obj"
        s3_client.client.copy_object(
            Bucket=bucket_name,
            Key=dest_key,
            CopySource=f"{bucket_name}/{src_key}",
            TaggingDirective="COPY",
        )

        # Verify tags were preserved
        tag_response = s3_client.client.get_object_tagging(
            Bucket=bucket_name, Key=dest_key
        )
        tags = {tag["Key"]: tag["Value"] for tag in tag_response["TagSet"]}
        assert tags == {"tag1": "value1", "tag2": "value2"}

    finally:
        fixture.cleanup()


def test_copy_object_tagging_directive_replace(s3_client, config):
    """
    Test CopyObject with TaggingDirective=REPLACE

    Should use new tags provided in request
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("copy-tag-replace")
        s3_client.create_bucket(bucket_name)

        # Create source with tags
        src_key = "src-obj"
        s3_client.client.put_object(
            Bucket=bucket_name,
            Key=src_key,
            Body=b"test data",
            Tagging="old=tag",
        )

        # Copy with REPLACE tagging directive and new tags
        dest_key = "dest-obj"
        s3_client.client.copy_object(
            Bucket=bucket_name,
            Key=dest_key,
            CopySource=f"{bucket_name}/{src_key}",
            TaggingDirective="REPLACE",
            Tagging="new=tag&another=tag",
        )

        # Verify new tags
        tag_response = s3_client.client.get_object_tagging(
            Bucket=bucket_name, Key=dest_key
        )
        tags = {tag["Key"]: tag["Value"] for tag in tag_response["TagSet"]}
        assert tags == {"new": "tag", "another": "tag"}

    finally:
        fixture.cleanup()


def test_copy_object_replace_content_headers(s3_client, config):
    """
    Test CopyObject replacing content headers

    Should update ContentType, ContentEncoding, etc. with REPLACE
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("copy-replace-headers")
        s3_client.create_bucket(bucket_name)

        # Create source with specific headers
        src_key = "src-obj"
        s3_client.client.put_object(
            Bucket=bucket_name,
            Key=src_key,
            Body=b"test data",
            ContentType="text/plain",
            ContentEncoding="gzip",
            ContentLanguage="en",
            CacheControl="no-cache",
        )

        # Copy with REPLACE and new headers
        dest_key = "dest-obj"
        s3_client.client.copy_object(
            Bucket=bucket_name,
            Key=dest_key,
            CopySource=f"{bucket_name}/{src_key}",
            MetadataDirective="REPLACE",
            ContentType="application/json",
            ContentEncoding="identity",
            ContentLanguage="fr",
            CacheControl="max-age=3600",
        )

        # Verify new headers
        head_response = s3_client.client.head_object(Bucket=bucket_name, Key=dest_key)
        assert head_response["ContentType"] == "application/json"
        assert head_response["ContentEncoding"] == "identity"
        assert head_response["ContentLanguage"] == "fr"
        assert head_response["CacheControl"] == "max-age=3600"

    finally:
        fixture.cleanup()


def test_copy_object_special_char_source(s3_client, config):
    """
    Test CopyObject with special characters in source key

    boto3 handles URL encoding automatically
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("copy-special")
        s3_client.create_bucket(bucket_name)

        # Create source with space in key (boto3 handles encoding)
        src_key = "my obj with spaces"
        data = b"test data"
        s3_client.put_object(bucket_name, src_key, data)

        # Copy with special characters (boto3 handles URL encoding)
        dest_key = "dest-obj"
        s3_client.client.copy_object(
            Bucket=bucket_name,
            Key=dest_key,
            CopySource=f"{bucket_name}/{src_key}",  # boto3 encodes automatically
        )

        # Verify destination
        get_response = s3_client.get_object(bucket_name, dest_key)
        assert get_response["Body"].read() == data

    finally:
        fixture.cleanup()
