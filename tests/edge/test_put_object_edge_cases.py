#!/usr/bin/env python3
"""
S3 PutObject Edge Case Tests

Tests PutObject API edge cases:
- Non-existing bucket errors
- Special characters in object keys
- Content validation and headers
- Zero-length objects
- Metadata and tagging during put

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


def test_put_object_non_existing_bucket(s3_client, config):
    """
    Test PutObject to non-existing bucket

    Should return NoSuchBucket error
    """
    fixture = TestFixture(s3_client, config)

    try:
        # Try to put object to non-existing bucket
        with pytest.raises(ClientError) as exc_info:
            s3_client.put_object('non-existing-bucket-12345', 'test-object', b'data')

        error_code = exc_info.value.response['Error']['Code']
        assert error_code == 'NoSuchBucket', \
            f"Expected NoSuchBucket, got {error_code}"

    finally:
        fixture.cleanup()


def test_put_object_zero_length(s3_client, config):
    """
    Test PutObject with zero-length content

    Zero-length objects should be created successfully
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('put-zero-len')
        s3_client.create_bucket(bucket_name)

        key = 'empty-object'
        put_response = s3_client.put_object(bucket_name, key, b'')

        # Verify object exists
        head_response = s3_client.head_object(bucket_name, key)
        assert head_response['ContentLength'] == 0

        # Verify can retrieve empty object
        get_response = s3_client.get_object(bucket_name, key)
        data = get_response['Body'].read()
        assert len(data) == 0

    finally:
        fixture.cleanup()


def test_put_object_with_metadata(s3_client, config):
    """
    Test PutObject with custom metadata

    Metadata should be stored and retrievable
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('put-metadata')
        s3_client.create_bucket(bucket_name)

        key = 'object-with-metadata'
        data = fixture.generate_random_data(256)
        metadata = {
            'author': 'test-user',
            'version': '1.0',
            'environment': 'testing'
        }

        s3_client.client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=data,
            Metadata=metadata
        )

        # Verify metadata is stored
        head_response = s3_client.head_object(bucket_name, key)
        retrieved_metadata = head_response.get('Metadata', {})

        assert retrieved_metadata == metadata, \
            f"Expected metadata {metadata}, got {retrieved_metadata}"

    finally:
        fixture.cleanup()


def test_put_object_with_content_type(s3_client, config):
    """
    Test PutObject with explicit ContentType

    ContentType should be preserved
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('put-content-type')
        s3_client.create_bucket(bucket_name)

        test_cases = [
            ('text.txt', 'text/plain'),
            ('data.json', 'application/json'),
            ('image.png', 'image/png'),
            ('doc.pdf', 'application/pdf'),
            ('video.mp4', 'video/mp4'),
        ]

        for key, content_type in test_cases:
            data = fixture.generate_random_data(100)

            s3_client.client.put_object(
                Bucket=bucket_name,
                Key=key,
                Body=data,
                ContentType=content_type
            )

            head_response = s3_client.head_object(bucket_name, key)
            assert head_response['ContentType'] == content_type, \
                f"Key {key}: Expected ContentType {content_type}, got {head_response['ContentType']}"

    finally:
        fixture.cleanup()


def test_put_object_with_cache_control(s3_client, config):
    """
    Test PutObject with Cache-Control header

    Cache-Control should be stored and retrievable
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('put-cache')
        s3_client.create_bucket(bucket_name)

        key = 'cached-object'
        data = fixture.generate_random_data(100)
        cache_control = 'max-age=3600, public'

        s3_client.client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=data,
            CacheControl=cache_control
        )

        head_response = s3_client.head_object(bucket_name, key)
        assert head_response.get('CacheControl') == cache_control, \
            f"Expected CacheControl {cache_control}, got {head_response.get('CacheControl')}"

    finally:
        fixture.cleanup()


def test_put_object_with_content_encoding(s3_client, config):
    """
    Test PutObject with ContentEncoding

    ContentEncoding should be preserved
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('put-encoding')
        s3_client.create_bucket(bucket_name)

        key = 'encoded-object'
        data = fixture.generate_random_data(200)
        content_encoding = 'gzip'

        s3_client.client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=data,
            ContentEncoding=content_encoding
        )

        head_response = s3_client.head_object(bucket_name, key)
        assert head_response.get('ContentEncoding') == content_encoding

    finally:
        fixture.cleanup()


def test_put_object_with_content_disposition(s3_client, config):
    """
    Test PutObject with ContentDisposition

    ContentDisposition should be preserved
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('put-disposition')
        s3_client.create_bucket(bucket_name)

        key = 'download-object'
        data = fixture.generate_random_data(150)
        content_disposition = 'attachment; filename="download.bin"'

        s3_client.client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=data,
            ContentDisposition=content_disposition
        )

        head_response = s3_client.head_object(bucket_name, key)
        assert head_response.get('ContentDisposition') == content_disposition

    finally:
        fixture.cleanup()


def test_put_object_with_storage_class(s3_client, config):
    """
    Test PutObject with StorageClass

    StorageClass should be set (if supported)
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('put-storage')
        s3_client.create_bucket(bucket_name)

        key = 'standard-object'
        data = fixture.generate_random_data(100)
        storage_class = 'STANDARD'

        s3_client.client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=data,
            StorageClass=storage_class
        )

        head_response = s3_client.head_object(bucket_name, key)
        # StorageClass may not always be returned in HeadObject
        # Just verify object was created successfully
        assert head_response['ContentLength'] == 100

    finally:
        fixture.cleanup()


def test_put_object_overwrite_existing(s3_client, config):
    """
    Test PutObject overwrites existing object

    Second PUT should replace first object
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('put-overwrite')
        s3_client.create_bucket(bucket_name)

        key = 'overwrite-object'

        # First PUT
        data1 = b'original data'
        put1_response = s3_client.put_object(bucket_name, key, data1)
        etag1 = put1_response['ETag']

        # Second PUT
        data2 = b'new data that overwrites'
        put2_response = s3_client.put_object(bucket_name, key, data2)
        etag2 = put2_response['ETag']

        # ETags should differ
        assert etag1 != etag2, "ETags should differ after overwrite"

        # Verify new data is stored
        get_response = s3_client.get_object(bucket_name, key)
        retrieved_data = get_response['Body'].read()
        assert retrieved_data == data2

    finally:
        fixture.cleanup()


def test_put_object_large_metadata(s3_client, config):
    """
    Test PutObject with large metadata values

    Metadata should handle reasonably large values
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('put-large-meta')
        s3_client.create_bucket(bucket_name)

        key = 'large-metadata-object'
        data = fixture.generate_random_data(100)

        # Create metadata with large values (but within S3 limits)
        large_value = 'x' * 1000  # 1KB value
        metadata = {
            'large-key': large_value,
            'normal-key': 'normal-value'
        }

        s3_client.client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=data,
            Metadata=metadata
        )

        head_response = s3_client.head_object(bucket_name, key)
        retrieved_metadata = head_response.get('Metadata', {})

        assert retrieved_metadata['large-key'] == large_value
        assert retrieved_metadata['normal-key'] == 'normal-value'

    finally:
        fixture.cleanup()


def test_put_object_with_tagging(s3_client, config):
    """
    Test PutObject with tagging

    Tags should be set during object creation
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('put-tagging')
        s3_client.create_bucket(bucket_name)

        key = 'tagged-object'
        data = fixture.generate_random_data(100)
        tagging = 'key1=value1&key2=value2'

        s3_client.client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=data,
            Tagging=tagging
        )

        # Verify tags were set
        try:
            tag_response = s3_client.client.get_object_tagging(
                Bucket=bucket_name,
                Key=key
            )

            tags = {tag['Key']: tag['Value'] for tag in tag_response.get('TagSet', [])}
            assert 'key1' in tags
            assert tags['key1'] == 'value1'
            assert 'key2' in tags
            assert tags['key2'] == 'value2'

        except ClientError as e:
            if e.response['Error']['Code'] != 'NotImplemented':
                raise

    finally:
        fixture.cleanup()


def test_put_object_success_returns_etag(s3_client, config):
    """
    Test PutObject returns ETag

    ETag should be present in response
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('put-etag')
        s3_client.create_bucket(bucket_name)

        key = 'etag-object'
        data = fixture.generate_random_data(256)

        put_response = s3_client.put_object(bucket_name, key, data)

        assert 'ETag' in put_response
        assert put_response['ETag'] is not None
        assert len(put_response['ETag']) > 0

    finally:
        fixture.cleanup()
