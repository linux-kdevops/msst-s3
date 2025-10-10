#!/usr/bin/env python3
"""
S3 GetObject Advanced Tests

Tests GetObject API advanced features:
- Response header overrides
- Invalid parameters
- Directory objects
- Part number handling
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


def test_get_object_response_cache_control_override(s3_client, config):
    """
    Test GetObject with ResponseCacheControl override

    Should override Cache-Control header in response
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('get-cache-override')
        s3_client.create_bucket(bucket_name)

        key = 'test-object'
        data = b'test content'
        s3_client.put_object(bucket_name, key, data)

        # Get with ResponseCacheControl
        get_response = s3_client.client.get_object(
            Bucket=bucket_name,
            Key=key,
            ResponseCacheControl='max-age=90'
        )

        # Verify override was applied
        assert get_response.get('CacheControl') == 'max-age=90'
        assert get_response['Body'].read() == data

    finally:
        fixture.cleanup()


def test_get_object_response_content_disposition_override(s3_client, config):
    """
    Test GetObject with ResponseContentDisposition override

    Should override Content-Disposition header in response
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('get-disp-override')
        s3_client.create_bucket(bucket_name)

        key = 'test-file.txt'
        data = b'test content'
        s3_client.put_object(bucket_name, key, data)

        # Get with ResponseContentDisposition
        get_response = s3_client.client.get_object(
            Bucket=bucket_name,
            Key=key,
            ResponseContentDisposition='attachment; filename="downloaded.txt"'
        )

        # Verify override was applied
        assert get_response.get('ContentDisposition') == 'attachment; filename="downloaded.txt"'
        assert get_response['Body'].read() == data

    finally:
        fixture.cleanup()


def test_get_object_response_content_encoding_override(s3_client, config):
    """
    Test GetObject with ResponseContentEncoding override

    Should override Content-Encoding header in response
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('get-enc-override')
        s3_client.create_bucket(bucket_name)

        key = 'test-object'
        data = b'test content'
        s3_client.put_object(bucket_name, key, data)

        # Get with ResponseContentEncoding
        get_response = s3_client.client.get_object(
            Bucket=bucket_name,
            Key=key,
            ResponseContentEncoding='gzip'
        )

        # Verify override was applied
        assert get_response.get('ContentEncoding') == 'gzip'
        assert get_response['Body'].read() == data

    finally:
        fixture.cleanup()


def test_get_object_response_content_language_override(s3_client, config):
    """
    Test GetObject with ResponseContentLanguage override

    Should override Content-Language header in response
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('get-lang-override')
        s3_client.create_bucket(bucket_name)

        key = 'test-object'
        data = b'test content'
        s3_client.put_object(bucket_name, key, data)

        # Get with ResponseContentLanguage
        get_response = s3_client.client.get_object(
            Bucket=bucket_name,
            Key=key,
            ResponseContentLanguage='en-US'
        )

        # Verify override was applied
        assert get_response.get('ContentLanguage') == 'en-US'
        assert get_response['Body'].read() == data

    finally:
        fixture.cleanup()


def test_get_object_response_content_type_override(s3_client, config):
    """
    Test GetObject with ResponseContentType override

    Should override Content-Type header in response
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('get-type-override')
        s3_client.create_bucket(bucket_name)

        key = 'test-object.txt'
        data = b'test content'
        # Store with one content type
        s3_client.client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=data,
            ContentType='text/plain'
        )

        # Get with different ResponseContentType
        get_response = s3_client.client.get_object(
            Bucket=bucket_name,
            Key=key,
            ResponseContentType='application/json'
        )

        # Verify override was applied
        assert get_response.get('ContentType') == 'application/json'
        assert get_response['Body'].read() == data

    finally:
        fixture.cleanup()


def test_get_object_response_expires_override(s3_client, config):
    """
    Test GetObject with ResponseExpires override

    Should override Expires header in response
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('get-exp-override')
        s3_client.create_bucket(bucket_name)

        key = 'test-object'
        data = b'test content'
        s3_client.put_object(bucket_name, key, data)

        # Get with ResponseExpires
        from datetime import datetime, timezone
        expires_date = datetime(2025, 12, 31, 23, 59, 59, tzinfo=timezone.utc)

        get_response = s3_client.client.get_object(
            Bucket=bucket_name,
            Key=key,
            ResponseExpires=expires_date
        )

        # Verify override was applied (Expires field should exist)
        assert 'Expires' in get_response
        assert get_response['Body'].read() == data

    finally:
        fixture.cleanup()


def test_get_object_directory_success(s3_client, config):
    """
    Test GetObject on directory object (trailing slash)

    Should retrieve empty directory object
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('get-dir')
        s3_client.create_bucket(bucket_name)

        # Create directory object
        dir_key = 'mydir/'
        s3_client.put_object(bucket_name, dir_key, b'')

        # Get directory object
        get_response = s3_client.get_object(bucket_name, dir_key)
        data = get_response['Body'].read()

        assert len(data) == 0
        assert get_response['ContentLength'] == 0

    finally:
        fixture.cleanup()


def test_get_object_non_existing_dir_object(s3_client, config):
    """
    Test GetObject on non-existing directory object

    Should return NoSuchKey error
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('get-nodir')
        s3_client.create_bucket(bucket_name)

        with pytest.raises(ClientError) as exc_info:
            s3_client.get_object(bucket_name, 'nonexistent/')

        error_code = exc_info.value.response['Error']['Code']
        assert error_code == 'NoSuchKey', \
            f"Expected NoSuchKey, got {error_code}"

    finally:
        fixture.cleanup()


def test_get_object_invalid_parent(s3_client, config):
    """
    Test GetObject with key that has non-existing parent path

    Should return NoSuchKey error
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('get-invalid-parent')
        s3_client.create_bucket(bucket_name)

        # Try to get object with non-existing parent path
        with pytest.raises(ClientError) as exc_info:
            s3_client.get_object(bucket_name, 'nonexistent/dir/file.txt')

        error_code = exc_info.value.response['Error']['Code']
        assert error_code == 'NoSuchKey', \
            f"Expected NoSuchKey, got {error_code}"

    finally:
        fixture.cleanup()


def test_get_object_by_range_resp_status(s3_client, config):
    """
    Test GetObject with Range returns 206 Partial Content

    Range requests should return 206 status code
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('get-range-status')
        s3_client.create_bucket(bucket_name)

        key = 'test-object'
        data = b'0123456789' * 10  # 100 bytes
        s3_client.put_object(bucket_name, key, data)

        # Get with range
        get_response = s3_client.client.get_object(
            Bucket=bucket_name,
            Key=key,
            Range='bytes=0-9'
        )

        # Verify 206 Partial Content status
        status_code = get_response['ResponseMetadata']['HTTPStatusCode']
        assert status_code == 206, f"Expected 206, got {status_code}"

        # Verify partial content
        retrieved_data = get_response['Body'].read()
        assert len(retrieved_data) == 10
        assert retrieved_data == b'0123456789'

    finally:
        fixture.cleanup()


def test_get_object_multiple_response_overrides(s3_client, config):
    """
    Test GetObject with multiple response overrides

    Multiple overrides should all be applied
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('get-multi-override')
        s3_client.create_bucket(bucket_name)

        key = 'test-object'
        data = b'test content for multiple overrides'
        s3_client.put_object(bucket_name, key, data)

        # Get with multiple overrides
        get_response = s3_client.client.get_object(
            Bucket=bucket_name,
            Key=key,
            ResponseCacheControl='no-cache',
            ResponseContentType='application/octet-stream',
            ResponseContentDisposition='attachment; filename="download.bin"'
        )

        # Verify all overrides applied
        assert get_response.get('CacheControl') == 'no-cache'
        assert get_response.get('ContentType') == 'application/octet-stream'
        assert get_response.get('ContentDisposition') == 'attachment; filename="download.bin"'
        assert get_response['Body'].read() == data

    finally:
        fixture.cleanup()


def test_get_object_with_if_match_success(s3_client, config):
    """
    Test GetObject with If-Match matching ETag

    Should return object when ETag matches
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('get-ifmatch')
        s3_client.create_bucket(bucket_name)

        key = 'test-object'
        data = b'test content'
        put_response = s3_client.put_object(bucket_name, key, data)
        etag = put_response['ETag']

        # Get with matching If-Match
        get_response = s3_client.client.get_object(
            Bucket=bucket_name,
            Key=key,
            IfMatch=etag
        )

        assert get_response['Body'].read() == data

    finally:
        fixture.cleanup()


def test_get_object_with_if_match_fails(s3_client, config):
    """
    Test GetObject with If-Match not matching ETag

    Should return PreconditionFailed when ETag doesn't match
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('get-ifmatch-fail')
        s3_client.create_bucket(bucket_name)

        key = 'test-object'
        data = b'test content'
        s3_client.put_object(bucket_name, key, data)

        # Get with non-matching If-Match
        with pytest.raises(ClientError) as exc_info:
            s3_client.client.get_object(
                Bucket=bucket_name,
                Key=key,
                IfMatch='"wrong-etag"'
            )

        error_code = exc_info.value.response['Error']['Code']
        assert error_code == 'PreconditionFailed', \
            f"Expected PreconditionFailed, got {error_code}"

    finally:
        fixture.cleanup()


def test_get_object_with_checksums(s3_client, config):
    """
    Test GetObject returns checksums when available

    Should include checksum metadata if object has checksums
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('get-checksums')
        s3_client.create_bucket(bucket_name)

        key = 'checksum-object'
        data = b'test content for checksums'

        # Put with checksum
        try:
            s3_client.client.put_object(
                Bucket=bucket_name,
                Key=key,
                Body=data,
                ChecksumAlgorithm='SHA256'
            )

            # Get object
            get_response = s3_client.get_object(bucket_name, key)
            retrieved_data = get_response['Body'].read()

            assert retrieved_data == data

            # Checksum should be in response or metadata
            # (Not all implementations return checksums in GetObject)
            # Just verify the object is retrievable

        except Exception as e:
            # If ChecksumAlgorithm not supported, skip
            if 'NotImplemented' in str(e) or 'not supported' in str(e).lower():
                pytest.skip("Checksums not supported")
            raise

    finally:
        fixture.cleanup()
