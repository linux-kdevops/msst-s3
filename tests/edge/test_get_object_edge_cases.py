#!/usr/bin/env python3
"""
S3 GetObject Edge Case Tests

Tests GetObject API edge cases:
- Non-existing objects and keys
- Directory-like object handling
- Content validation
- Response status codes
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


def test_get_object_non_existing_key(s3_client, config):
    """
    Test GetObject on non-existing key

    Should return NoSuchKey error
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('get-nokey')
        s3_client.create_bucket(bucket_name)

        with pytest.raises(ClientError) as exc_info:
            s3_client.get_object(bucket_name, 'non-existing-key')

        error_code = exc_info.value.response['Error']['Code']
        assert error_code == 'NoSuchKey', \
            f"Expected NoSuchKey, got {error_code}"

    finally:
        fixture.cleanup()


def test_get_object_success(s3_client, config):
    """
    Test basic GetObject operation

    Should retrieve object data correctly
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('get-success')
        s3_client.create_bucket(bucket_name)

        key = 'test-object'
        data = fixture.generate_random_data(1024)
        s3_client.put_object(bucket_name, key, data)

        # Get object
        get_response = s3_client.get_object(bucket_name, key)
        retrieved_data = get_response['Body'].read()

        assert len(retrieved_data) == 1024
        assert retrieved_data == data

    finally:
        fixture.cleanup()


def test_get_object_directory_object_noslash(s3_client, config):
    """
    Test GetObject for directory object without trailing slash

    Getting 'dir' should not retrieve 'dir/' (different objects)
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('get-dir-noslash')
        s3_client.create_bucket(bucket_name)

        # Create directory object with slash
        dir_key = 'my-obj/'
        s3_client.put_object(bucket_name, dir_key, b'')

        # Try to get without slash
        with pytest.raises(ClientError) as exc_info:
            s3_client.get_object(bucket_name, 'my-obj')

        error_code = exc_info.value.response['Error']['Code']
        assert error_code == 'NoSuchKey', \
            f"Expected NoSuchKey, got {error_code}"

    finally:
        fixture.cleanup()


def test_get_object_zero_length(s3_client, config):
    """
    Test GetObject on zero-length object

    Should retrieve empty content successfully
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('get-zero')
        s3_client.create_bucket(bucket_name)

        key = 'empty-object'
        s3_client.put_object(bucket_name, key, b'')

        # Get empty object
        get_response = s3_client.get_object(bucket_name, key)
        retrieved_data = get_response['Body'].read()

        assert len(retrieved_data) == 0
        assert get_response['ContentLength'] == 0

    finally:
        fixture.cleanup()


def test_get_object_with_metadata(s3_client, config):
    """
    Test GetObject returns metadata

    Metadata should be included in response
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('get-metadata')
        s3_client.create_bucket(bucket_name)

        key = 'object-with-meta'
        data = fixture.generate_random_data(256)
        metadata = {'author': 'test', 'version': '1.0'}

        s3_client.client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=data,
            Metadata=metadata
        )

        # Get object and verify metadata
        get_response = s3_client.get_object(bucket_name, key)

        retrieved_metadata = get_response.get('Metadata', {})
        assert retrieved_metadata == metadata

    finally:
        fixture.cleanup()


def test_get_object_with_content_type(s3_client, config):
    """
    Test GetObject returns ContentType

    ContentType should match what was set
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('get-content-type')
        s3_client.create_bucket(bucket_name)

        key = 'typed-object'
        data = fixture.generate_random_data(100)
        content_type = 'application/json'

        s3_client.client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=data,
            ContentType=content_type
        )

        # Get object and verify ContentType
        get_response = s3_client.get_object(bucket_name, key)
        assert get_response['ContentType'] == content_type

    finally:
        fixture.cleanup()


def test_get_object_returns_etag(s3_client, config):
    """
    Test GetObject returns ETag

    ETag should match PutObject ETag
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('get-etag')
        s3_client.create_bucket(bucket_name)

        key = 'etag-object'
        data = fixture.generate_random_data(128)

        put_response = s3_client.put_object(bucket_name, key, data)
        put_etag = put_response['ETag']

        # Get object and verify ETag
        get_response = s3_client.get_object(bucket_name, key)
        get_etag = get_response['ETag']

        assert get_etag == put_etag

    finally:
        fixture.cleanup()


def test_get_object_returns_last_modified(s3_client, config):
    """
    Test GetObject returns LastModified

    LastModified timestamp should be present
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('get-lastmod')
        s3_client.create_bucket(bucket_name)

        key = 'timestamped-object'
        data = fixture.generate_random_data(64)
        s3_client.put_object(bucket_name, key, data)

        # Get object and verify LastModified
        get_response = s3_client.get_object(bucket_name, key)

        assert 'LastModified' in get_response
        assert get_response['LastModified'] is not None

    finally:
        fixture.cleanup()


def test_get_object_content_length(s3_client, config):
    """
    Test GetObject ContentLength matches actual data

    ContentLength header should match body size
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('get-length')
        s3_client.create_bucket(bucket_name)

        key = 'sized-object'
        expected_size = 512
        data = fixture.generate_random_data(expected_size)
        s3_client.put_object(bucket_name, key, data)

        # Get object and verify ContentLength
        get_response = s3_client.get_object(bucket_name, key)
        retrieved_data = get_response['Body'].read()

        assert get_response['ContentLength'] == expected_size
        assert len(retrieved_data) == expected_size

    finally:
        fixture.cleanup()


def test_get_object_large_object(s3_client, config):
    """
    Test GetObject with large object (1MB)

    Large objects should be retrieved correctly
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('get-large')
        s3_client.create_bucket(bucket_name)

        key = 'large-object'
        size = 1024 * 1024  # 1MB
        data = fixture.generate_random_data(size)
        s3_client.put_object(bucket_name, key, data)

        # Get large object
        get_response = s3_client.get_object(bucket_name, key)
        retrieved_data = get_response['Body'].read()

        assert len(retrieved_data) == size
        assert retrieved_data == data

    finally:
        fixture.cleanup()


def test_get_object_with_cache_control(s3_client, config):
    """
    Test GetObject returns Cache-Control header

    Cache-Control should be preserved
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('get-cache')
        s3_client.create_bucket(bucket_name)

        key = 'cached-object'
        data = fixture.generate_random_data(100)
        cache_control = 'max-age=3600'

        s3_client.client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=data,
            CacheControl=cache_control
        )

        # Get object and verify Cache-Control
        get_response = s3_client.get_object(bucket_name, key)
        assert get_response.get('CacheControl') == cache_control

    finally:
        fixture.cleanup()


def test_get_object_response_status(s3_client, config):
    """
    Test GetObject HTTP response status

    Should return 200 OK
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('get-status')
        s3_client.create_bucket(bucket_name)

        key = 'status-object'
        data = fixture.generate_random_data(50)
        s3_client.put_object(bucket_name, key, data)

        # Get object and verify status
        get_response = s3_client.get_object(bucket_name, key)
        status_code = get_response['ResponseMetadata']['HTTPStatusCode']

        assert status_code == 200, f"Expected 200, got {status_code}"

    finally:
        fixture.cleanup()
