#!/usr/bin/env python3
"""
S3 CopyObject Edge Case Tests

Tests CopyObject API edge cases:
- Non-existing source/destination buckets
- Copy to self with different directives
- Invalid copy sources
- Tagging directives
- Error conditions

Ported from versitygw integration tests.

Generated-by: Claude AI
Signed-off-by: Luis Chamberlain <mcgrof@kernel.org>
"""

import pytest
import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from tests.common.fixtures import TestFixture
from botocore.exceptions import ClientError


def test_copy_object_success(s3_client, config):
    """
    Test basic CopyObject operation

    Object should be copied successfully
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('copy-success')
        s3_client.create_bucket(bucket_name)

        source_key = 'source-object'
        dest_key = 'dest-object'
        data = fixture.generate_random_data(256)

        s3_client.put_object(bucket_name, source_key, data)

        # Copy object
        copy_source = {'Bucket': bucket_name, 'Key': source_key}
        s3_client.client.copy_object(
            CopySource=copy_source,
            Bucket=bucket_name,
            Key=dest_key
        )

        # Verify destination exists
        get_response = s3_client.get_object(bucket_name, dest_key)
        retrieved_data = get_response['Body'].read()

        assert retrieved_data == data

    finally:
        fixture.cleanup()


def test_copy_object_non_existing_source(s3_client, config):
    """
    Test CopyObject with non-existing source

    Should return NoSuchKey error
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('copy-nosrc')
        s3_client.create_bucket(bucket_name)

        copy_source = {'Bucket': bucket_name, 'Key': 'non-existing-source'}

        with pytest.raises(ClientError) as exc_info:
            s3_client.client.copy_object(
                CopySource=copy_source,
                Bucket=bucket_name,
                Key='dest-object'
            )

        error_code = exc_info.value.response['Error']['Code']
        assert error_code == 'NoSuchKey', \
            f"Expected NoSuchKey, got {error_code}"

    finally:
        fixture.cleanup()


def test_copy_object_non_existing_dest_bucket(s3_client, config):
    """
    Test CopyObject to non-existing destination bucket

    Should return NoSuchBucket error
    """
    fixture = TestFixture(s3_client, config)

    try:
        source_bucket = fixture.generate_bucket_name('copy-src')
        s3_client.create_bucket(source_bucket)

        source_key = 'source-object'
        data = fixture.generate_random_data(100)
        s3_client.put_object(source_bucket, source_key, data)

        copy_source = {'Bucket': source_bucket, 'Key': source_key}

        with pytest.raises(ClientError) as exc_info:
            s3_client.client.copy_object(
                CopySource=copy_source,
                Bucket='non-existing-bucket-12345',
                Key='dest-object'
            )

        error_code = exc_info.value.response['Error']['Code']
        assert error_code == 'NoSuchBucket', \
            f"Expected NoSuchBucket, got {error_code}"

    finally:
        fixture.cleanup()


def test_copy_object_to_itself(s3_client, config):
    """
    Test copying object to itself (same bucket and key)

    Should succeed when using REPLACE directive
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('copy-self')
        s3_client.create_bucket(bucket_name)

        key = 'test-object'
        data = fixture.generate_random_data(128)
        old_metadata = {'version': '1'}

        s3_client.client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=data,
            Metadata=old_metadata
        )

        # Copy to itself with REPLACE directive and new metadata
        new_metadata = {'version': '2'}
        copy_source = {'Bucket': bucket_name, 'Key': key}

        s3_client.client.copy_object(
            CopySource=copy_source,
            Bucket=bucket_name,
            Key=key,
            MetadataDirective='REPLACE',
            Metadata=new_metadata
        )

        # Verify metadata was updated
        head_response = s3_client.head_object(bucket_name, key)
        retrieved_metadata = head_response.get('Metadata', {})

        assert retrieved_metadata == new_metadata

    finally:
        fixture.cleanup()


def test_copy_object_invalid_copy_source_format(s3_client, config):
    """
    Test CopyObject with invalid CopySource format

    Should return error for malformed source
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('copy-invalid-src')
        s3_client.create_bucket(bucket_name)

        # Invalid copy source format (missing bucket/key separator)
        with pytest.raises(ClientError) as exc_info:
            s3_client.client.copy_object(
                CopySource='invalid-format',
                Bucket=bucket_name,
                Key='dest-object'
            )

        error_code = exc_info.value.response['Error']['Code']
        assert error_code in ['InvalidArgument', 'InvalidRequest'], \
            f"Expected InvalidArgument or InvalidRequest, got {error_code}"

    finally:
        fixture.cleanup()


def test_copy_object_with_tagging_copy(s3_client, config):
    """
    Test CopyObject preserves tags with COPY directive

    Tags should be copied from source to destination
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('copy-tag-copy')
        s3_client.create_bucket(bucket_name)

        source_key = 'source-tagged'
        dest_key = 'dest-tagged'
        data = fixture.generate_random_data(100)

        # Create source with tags
        s3_client.client.put_object(
            Bucket=bucket_name,
            Key=source_key,
            Body=data,
            Tagging='source=true&env=test'
        )

        # Copy with default tagging directive (COPY)
        copy_source = {'Bucket': bucket_name, 'Key': source_key}
        s3_client.client.copy_object(
            CopySource=copy_source,
            Bucket=bucket_name,
            Key=dest_key
        )

        # Verify tags were copied
        try:
            tag_response = s3_client.client.get_object_tagging(
                Bucket=bucket_name,
                Key=dest_key
            )

            tags = {tag['Key']: tag['Value'] for tag in tag_response.get('TagSet', [])}
            assert 'source' in tags
            assert tags['source'] == 'true'

        except ClientError as e:
            if e.response['Error']['Code'] != 'NotImplemented':
                raise

    finally:
        fixture.cleanup()


def test_copy_object_with_tagging_replace(s3_client, config):
    """
    Test CopyObject with REPLACE tagging directive

    New tags should replace source tags
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('copy-tag-replace')
        s3_client.create_bucket(bucket_name)

        source_key = 'source-tagged'
        dest_key = 'dest-tagged'
        data = fixture.generate_random_data(100)

        # Create source with tags
        s3_client.client.put_object(
            Bucket=bucket_name,
            Key=source_key,
            Body=data,
            Tagging='source=true'
        )

        # Copy with REPLACE tagging directive
        copy_source = {'Bucket': bucket_name, 'Key': source_key}
        s3_client.client.copy_object(
            CopySource=copy_source,
            Bucket=bucket_name,
            Key=dest_key,
            TaggingDirective='REPLACE',
            Tagging='copied=true&version=2'
        )

        # Verify new tags
        try:
            tag_response = s3_client.client.get_object_tagging(
                Bucket=bucket_name,
                Key=dest_key
            )

            tags = {tag['Key']: tag['Value'] for tag in tag_response.get('TagSet', [])}
            assert 'copied' in tags
            assert tags['copied'] == 'true'
            assert 'source' not in tags  # Old tag should not be present

        except ClientError as e:
            if e.response['Error']['Code'] != 'NotImplemented':
                raise

    finally:
        fixture.cleanup()


def test_copy_object_preserves_content_type(s3_client, config):
    """
    Test CopyObject preserves ContentType

    ContentType should be copied from source
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('copy-content-type')
        s3_client.create_bucket(bucket_name)

        source_key = 'source-typed'
        dest_key = 'dest-typed'
        data = fixture.generate_random_data(100)
        content_type = 'application/json'

        s3_client.client.put_object(
            Bucket=bucket_name,
            Key=source_key,
            Body=data,
            ContentType=content_type
        )

        # Copy object
        copy_source = {'Bucket': bucket_name, 'Key': source_key}
        s3_client.client.copy_object(
            CopySource=copy_source,
            Bucket=bucket_name,
            Key=dest_key
        )

        # Verify ContentType preserved
        head_response = s3_client.head_object(bucket_name, dest_key)
        assert head_response['ContentType'] == content_type

    finally:
        fixture.cleanup()


def test_copy_object_large_object(s3_client, config):
    """
    Test CopyObject with large object (1MB)

    Large objects should be copied successfully
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('copy-large')
        s3_client.create_bucket(bucket_name)

        source_key = 'large-source'
        dest_key = 'large-dest'
        size = 1024 * 1024  # 1MB
        data = fixture.generate_random_data(size)

        s3_client.put_object(bucket_name, source_key, data)

        # Copy large object
        copy_source = {'Bucket': bucket_name, 'Key': source_key}
        s3_client.client.copy_object(
            CopySource=copy_source,
            Bucket=bucket_name,
            Key=dest_key
        )

        # Verify destination
        get_response = s3_client.get_object(bucket_name, dest_key)
        retrieved_data = get_response['Body'].read()

        assert len(retrieved_data) == size
        assert retrieved_data == data

    finally:
        fixture.cleanup()


def test_copy_object_returns_etag(s3_client, config):
    """
    Test CopyObject returns ETag in response

    Response should include ETag of copied object
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('copy-etag')
        s3_client.create_bucket(bucket_name)

        source_key = 'source-etag'
        dest_key = 'dest-etag'
        data = fixture.generate_random_data(128)

        s3_client.put_object(bucket_name, source_key, data)

        # Copy object
        copy_source = {'Bucket': bucket_name, 'Key': source_key}
        copy_response = s3_client.client.copy_object(
            CopySource=copy_source,
            Bucket=bucket_name,
            Key=dest_key
        )

        # Verify ETag in response
        assert 'CopyObjectResult' in copy_response or 'ETag' in copy_response

        # Verify destination has ETag
        head_response = s3_client.head_object(bucket_name, dest_key)
        assert 'ETag' in head_response
        assert head_response['ETag'] is not None

    finally:
        fixture.cleanup()
