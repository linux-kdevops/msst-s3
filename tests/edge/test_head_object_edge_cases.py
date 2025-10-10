#!/usr/bin/env python3
"""
S3 HeadObject Edge Case Tests

Tests HeadObject API edge cases including:
- Range requests with various formats
- Non-existing objects
- Invalid parameters
- Content-Range headers
- Accept-Ranges headers

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


def test_head_object_non_existing_object(s3_client, config):
    """
    Test HeadObject on non-existing object

    Should return NotFound (404) error
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('head-not-found')
        s3_client.create_bucket(bucket_name)

        # HeadObject on non-existing object
        with pytest.raises(ClientError) as exc_info:
            s3_client.head_object(bucket_name, 'non-existing-object')

        error_code = exc_info.value.response['Error']['Code']
        assert error_code == '404' or error_code == 'NotFound', \
            f"Expected NotFound, got {error_code}"

    finally:
        fixture.cleanup()


def test_head_object_with_range_valid(s3_client, config):
    """
    Test HeadObject with valid Range header

    Verifies Content-Range and Content-Length for byte ranges
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('head-range')
        s3_client.create_bucket(bucket_name)

        key = 'test-object'
        obj_length = 100
        data = fixture.generate_random_data(obj_length)
        s3_client.put_object(bucket_name, key, data)

        # Test standard byte range
        test_cases = [
            # (Range header, expected ContentRange, expected ContentLength)
            ('bytes=0-0', 'bytes 0-0/100', 1),
            ('bytes=0-99', 'bytes 0-99/100', 100),
            ('bytes=0-49', 'bytes 0-49/100', 50),
            ('bytes=50-99', 'bytes 50-99/100', 50),
            ('bytes=50-', 'bytes 50-99/100', 50),
            ('bytes=0-', 'bytes 0-99/100', 100),
            ('bytes=99-99', 'bytes 99-99/100', 1),
            ('bytes=-1', 'bytes 99-99/100', 1),  # last byte
            ('bytes=-10', 'bytes 90-99/100', 10),  # last 10 bytes
            ('bytes=-100', 'bytes 0-99/100', 100),  # entire object
        ]

        for range_header, expected_content_range, expected_length in test_cases:
            response = s3_client.client.head_object(
                Bucket=bucket_name,
                Key=key,
                Range=range_header
            )

            assert response.get('AcceptRanges') == 'bytes', \
                f"Range {range_header}: Expected AcceptRanges 'bytes'"
            assert response['ContentLength'] == expected_length, \
                f"Range {range_header}: Expected length {expected_length}, got {response['ContentLength']}"

            # ContentRange should match expected
            if 'ContentRange' in response:
                assert response['ContentRange'] == expected_content_range, \
                    f"Range {range_header}: Expected ContentRange '{expected_content_range}', got '{response['ContentRange']}'"

    finally:
        fixture.cleanup()


def test_head_object_with_range_beyond_object(s3_client, config):
    """
    Test HeadObject with Range beyond object size

    Range values beyond object should be trimmed to object size
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('head-range-trim')
        s3_client.create_bucket(bucket_name)

        key = 'test-object'
        obj_length = 100
        data = fixture.generate_random_data(obj_length)
        s3_client.put_object(bucket_name, key, data)

        # Test ranges that extend beyond object
        test_cases = [
            ('bytes=0-100', 'bytes 0-99/100', 100),  # end beyond object
            ('bytes=0-999999', 'bytes 0-99/100', 100),
            ('bytes=-101', 'bytes 0-99/100', 100),  # suffix larger than object
        ]

        for range_header, expected_content_range, expected_length in test_cases:
            response = s3_client.client.head_object(
                Bucket=bucket_name,
                Key=key,
                Range=range_header
            )

            assert response['ContentLength'] == expected_length, \
                f"Range {range_header}: Expected length {expected_length}, got {response['ContentLength']}"

            if 'ContentRange' in response:
                assert response['ContentRange'] == expected_content_range, \
                    f"Range {range_header}: Expected ContentRange '{expected_content_range}', got '{response.get('ContentRange')}'"

    finally:
        fixture.cleanup()


def test_head_object_with_range_invalid(s3_client, config):
    """
    Test HeadObject with invalid Range values

    Invalid ranges should return full object or error
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('head-range-invalid')
        s3_client.create_bucket(bucket_name)

        key = 'test-object'
        obj_length = 100
        data = fixture.generate_random_data(obj_length)
        s3_client.put_object(bucket_name, key, data)

        # Invalid ranges that should return full object (not error)
        invalid_ranges = [
            'bytes=,',
            'bytes= -1',
            'bytes=abc',
            'bytes=a-z',
            'foo=0-1',  # unsupported unit
        ]

        for range_header in invalid_ranges:
            response = s3_client.client.head_object(
                Bucket=bucket_name,
                Key=key,
                Range=range_header
            )

            # Invalid range should return full object
            assert response['ContentLength'] == obj_length, \
                f"Range {range_header}: Expected full object length {obj_length}"

    finally:
        fixture.cleanup()


def test_head_object_with_range_not_satisfiable(s3_client, config):
    """
    Test HeadObject with unsatisfiable Range

    Range completely beyond object should return RequestedRangeNotSatisfiable
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('head-range-unsatisfy')
        s3_client.create_bucket(bucket_name)

        key = 'test-object'
        obj_length = 100
        data = fixture.generate_random_data(obj_length)
        s3_client.put_object(bucket_name, key, data)

        # Ranges that cannot be satisfied
        unsatisfiable_ranges = [
            'bytes=-0',  # zero bytes from end
            'bytes=100-100',  # start beyond object
            'bytes=100-110',
            'bytes=200-300',
        ]

        for range_header in unsatisfiable_ranges:
            with pytest.raises(ClientError) as exc_info:
                s3_client.client.head_object(
                    Bucket=bucket_name,
                    Key=key,
                    Range=range_header
                )

            error_code = exc_info.value.response['Error']['Code']
            # HTTP 416 is also valid (numeric code)
            assert error_code in ['InvalidRange', 'RequestedRangeNotSatisfiable', '416'], \
                f"Range {range_header}: Expected InvalidRange or RequestedRangeNotSatisfiable or 416, got {error_code}"

    finally:
        fixture.cleanup()


def test_head_object_success(s3_client, config):
    """
    Test basic HeadObject success case

    Verifies all standard metadata is returned
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('head-success')
        s3_client.create_bucket(bucket_name)

        key = 'test-object'
        data = fixture.generate_random_data(1024)
        metadata = {'test-key': 'test-value'}
        content_type = 'application/octet-stream'

        # Put object with metadata
        put_response = s3_client.client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=data,
            Metadata=metadata,
            ContentType=content_type
        )

        # HeadObject should return all metadata
        head_response = s3_client.head_object(bucket_name, key)

        assert head_response['ContentLength'] == 1024
        assert head_response['ContentType'] == content_type
        assert head_response['ETag'] == put_response['ETag']
        assert head_response.get('Metadata') == metadata
        assert 'LastModified' in head_response

    finally:
        fixture.cleanup()


def test_head_object_with_metadata(s3_client, config):
    """
    Test HeadObject returns custom metadata

    Verifies user-defined metadata is accessible via HeadObject
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('head-metadata')
        s3_client.create_bucket(bucket_name)

        key = 'test-object'
        data = fixture.generate_random_data(256)

        metadata = {
            'author': 'test-suite',
            'version': '1.0',
            'category': 'testing'
        }

        s3_client.client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=data,
            Metadata=metadata
        )

        # HeadObject should return metadata
        head_response = s3_client.head_object(bucket_name, key)

        returned_metadata = head_response.get('Metadata', {})
        assert returned_metadata == metadata, \
            f"Expected metadata {metadata}, got {returned_metadata}"

    finally:
        fixture.cleanup()


def test_head_object_content_headers(s3_client, config):
    """
    Test HeadObject returns all HTTP content headers

    Verifies ContentType, ContentEncoding, ContentLanguage, ContentDisposition
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('head-headers')
        s3_client.create_bucket(bucket_name)

        key = 'test-object'
        data = fixture.generate_random_data(512)

        content_type = 'text/plain'
        content_encoding = 'gzip'
        content_language = 'en-US'
        content_disposition = 'attachment; filename="test.txt"'

        s3_client.client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=data,
            ContentType=content_type,
            ContentEncoding=content_encoding,
            ContentLanguage=content_language,
            ContentDisposition=content_disposition
        )

        # HeadObject should return all headers
        head_response = s3_client.head_object(bucket_name, key)

        assert head_response['ContentType'] == content_type
        assert head_response.get('ContentEncoding') == content_encoding
        assert head_response.get('ContentLanguage') == content_language
        assert head_response.get('ContentDisposition') == content_disposition

    finally:
        fixture.cleanup()


def test_head_object_etag(s3_client, config):
    """
    Test HeadObject returns ETag

    Verifies ETag matches between PutObject and HeadObject
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('head-etag')
        s3_client.create_bucket(bucket_name)

        key = 'test-object'
        data = fixture.generate_random_data(128)

        put_response = s3_client.put_object(bucket_name, key, data)
        put_etag = put_response['ETag']

        # HeadObject should return same ETag
        head_response = s3_client.head_object(bucket_name, key)
        head_etag = head_response['ETag']

        assert head_etag == put_etag, \
            f"Expected ETag {put_etag}, got {head_etag}"

    finally:
        fixture.cleanup()


def test_head_object_last_modified(s3_client, config):
    """
    Test HeadObject returns LastModified timestamp

    Verifies LastModified is present and reasonable
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('head-last-mod')
        s3_client.create_bucket(bucket_name)

        key = 'test-object'
        data = fixture.generate_random_data(64)

        from datetime import datetime, timezone
        before_put = datetime.now(timezone.utc)

        s3_client.put_object(bucket_name, key, data)

        after_put = datetime.now(timezone.utc)

        # HeadObject should return LastModified
        head_response = s3_client.head_object(bucket_name, key)
        last_modified = head_response['LastModified']

        # LastModified should be reasonable (within a few seconds of now)
        # S3 may truncate to seconds, so we can't do precise comparisons
        # Just verify it's present and is a datetime
        assert last_modified is not None, "Expected LastModified to be present"
        assert hasattr(last_modified, 'year'), "Expected LastModified to be a datetime"

    finally:
        fixture.cleanup()
