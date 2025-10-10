#!/usr/bin/env python3
"""
S3 Conditional Request Tests

Tests HTTP conditional request headers for S3 operations:
- If-Match / If-None-Match (ETag-based)
- If-Modified-Since / If-Unmodified-Since (time-based)
- HeadObject conditional reads
- GetObject conditional reads
- CopyObject conditional operations

Ported from versitygw integration tests.

Generated-by: Claude AI
Signed-off-by: Luis Chamberlain <mcgrof@kernel.org>
"""

import pytest
from datetime import datetime, timedelta, timezone
import time
import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from tests.common.fixtures import TestFixture
from botocore.exceptions import ClientError


def test_head_object_if_match_success(s3_client, config):
    """
    Test HeadObject with If-Match header (matching ETag)

    Should return object metadata when ETag matches
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('head-if-match')
        s3_client.create_bucket(bucket_name)

        key = 'test-object'
        data = fixture.generate_random_data(100)

        # Create object
        put_response = s3_client.put_object(bucket_name, key, data)
        etag = put_response['ETag']

        # HeadObject with matching ETag
        head_response = s3_client.client.head_object(
            Bucket=bucket_name,
            Key=key,
            IfMatch=etag
        )

        # Should succeed
        assert head_response['ETag'] == etag
        assert head_response['ContentLength'] == 100

    finally:
        fixture.cleanup()


def test_head_object_if_match_fails(s3_client, config):
    """
    Test HeadObject with If-Match header (non-matching ETag)

    Should return PreconditionFailed when ETag doesn't match
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('head-if-match-fail')
        s3_client.create_bucket(bucket_name)

        key = 'test-object'
        data = fixture.generate_random_data(100)

        # Create object
        s3_client.put_object(bucket_name, key, data)

        # HeadObject with non-matching ETag
        wrong_etag = '"00000000000000000000000000000000"'

        with pytest.raises(ClientError) as exc_info:
            s3_client.client.head_object(
                Bucket=bucket_name,
                Key=key,
                IfMatch=wrong_etag
            )

        error_code = exc_info.value.response['Error']['Code']
        assert error_code == 'PreconditionFailed' or error_code == '412', \
            f"Expected PreconditionFailed, got {error_code}"

    finally:
        fixture.cleanup()


def test_head_object_if_none_match_returns_not_modified(s3_client, config):
    """
    Test HeadObject with If-None-Match header (matching ETag)

    Should return NotModified (304) when ETag matches
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('head-if-none-match')
        s3_client.create_bucket(bucket_name)

        key = 'test-object'
        data = fixture.generate_random_data(100)

        # Create object
        put_response = s3_client.put_object(bucket_name, key, data)
        etag = put_response['ETag']

        # HeadObject with If-None-Match and matching ETag
        with pytest.raises(ClientError) as exc_info:
            s3_client.client.head_object(
                Bucket=bucket_name,
                Key=key,
                IfNoneMatch=etag
            )

        error_code = exc_info.value.response['Error']['Code']
        assert error_code == 'NotModified' or error_code == '304', \
            f"Expected NotModified, got {error_code}"

    finally:
        fixture.cleanup()


def test_head_object_if_modified_since_not_modified(s3_client, config):
    """
    Test HeadObject with If-Modified-Since (object not modified)

    Should return NotModified when object hasn't been modified since timestamp
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('head-if-mod-since')
        s3_client.create_bucket(bucket_name)

        key = 'test-object'
        data = fixture.generate_random_data(100)

        # Create object
        s3_client.put_object(bucket_name, key, data)

        # Wait a moment to ensure timestamp is definitely in the past
        time.sleep(1)

        # Future timestamp (object is older than this)
        future_time = datetime.now(timezone.utc) + timedelta(hours=1)

        # HeadObject with future If-Modified-Since
        with pytest.raises(ClientError) as exc_info:
            s3_client.client.head_object(
                Bucket=bucket_name,
                Key=key,
                IfModifiedSince=future_time
            )

        error_code = exc_info.value.response['Error']['Code']
        assert error_code == 'NotModified' or error_code == '304', \
            f"Expected NotModified, got {error_code}"

    finally:
        fixture.cleanup()


def test_head_object_if_unmodified_since_success(s3_client, config):
    """
    Test HeadObject with If-Unmodified-Since (object not modified)

    Should succeed when object hasn't been modified since timestamp
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('head-if-unmod')
        s3_client.create_bucket(bucket_name)

        key = 'test-object'
        data = fixture.generate_random_data(100)

        # Create object
        put_response = s3_client.put_object(bucket_name, key, data)

        # Future timestamp (object is older than this)
        future_time = datetime.now(timezone.utc) + timedelta(hours=1)

        # HeadObject with future If-Unmodified-Since should succeed
        head_response = s3_client.client.head_object(
            Bucket=bucket_name,
            Key=key,
            IfUnmodifiedSince=future_time
        )

        assert head_response['ETag'] == put_response['ETag']
        assert head_response['ContentLength'] == 100

    finally:
        fixture.cleanup()


def test_get_object_if_match_success(s3_client, config):
    """
    Test GetObject with If-Match header (matching ETag)

    Should return object data when ETag matches
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('get-if-match')
        s3_client.create_bucket(bucket_name)

        key = 'test-object'
        data = fixture.generate_random_data(256)

        # Create object
        put_response = s3_client.put_object(bucket_name, key, data)
        etag = put_response['ETag']

        # GetObject with matching ETag
        get_response = s3_client.client.get_object(
            Bucket=bucket_name,
            Key=key,
            IfMatch=etag
        )

        # Should succeed and return data
        retrieved_data = get_response['Body'].read()
        assert len(retrieved_data) == 256
        assert retrieved_data == data

    finally:
        fixture.cleanup()


def test_get_object_if_match_fails(s3_client, config):
    """
    Test GetObject with If-Match header (non-matching ETag)

    Should return PreconditionFailed when ETag doesn't match
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('get-if-match-fail')
        s3_client.create_bucket(bucket_name)

        key = 'test-object'
        data = fixture.generate_random_data(256)

        # Create object
        s3_client.put_object(bucket_name, key, data)

        # GetObject with non-matching ETag
        wrong_etag = '"00000000000000000000000000000000"'

        with pytest.raises(ClientError) as exc_info:
            s3_client.client.get_object(
                Bucket=bucket_name,
                Key=key,
                IfMatch=wrong_etag
            )

        error_code = exc_info.value.response['Error']['Code']
        assert error_code == 'PreconditionFailed' or error_code == '412', \
            f"Expected PreconditionFailed, got {error_code}"

    finally:
        fixture.cleanup()


def test_get_object_if_none_match_returns_not_modified(s3_client, config):
    """
    Test GetObject with If-None-Match header (matching ETag)

    Should return NotModified when ETag matches
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('get-if-none-match')
        s3_client.create_bucket(bucket_name)

        key = 'test-object'
        data = fixture.generate_random_data(256)

        # Create object
        put_response = s3_client.put_object(bucket_name, key, data)
        etag = put_response['ETag']

        # GetObject with If-None-Match and matching ETag
        with pytest.raises(ClientError) as exc_info:
            s3_client.client.get_object(
                Bucket=bucket_name,
                Key=key,
                IfNoneMatch=etag
            )

        error_code = exc_info.value.response['Error']['Code']
        assert error_code == 'NotModified' or error_code == '304', \
            f"Expected NotModified, got {error_code}"

    finally:
        fixture.cleanup()


def test_get_object_if_modified_since_not_modified(s3_client, config):
    """
    Test GetObject with If-Modified-Since (object not modified)

    Should return NotModified when object hasn't been modified since timestamp
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('get-if-mod-since')
        s3_client.create_bucket(bucket_name)

        key = 'test-object'
        data = fixture.generate_random_data(256)

        # Create object
        s3_client.put_object(bucket_name, key, data)

        # Wait a moment
        time.sleep(1)

        # Future timestamp
        future_time = datetime.now(timezone.utc) + timedelta(hours=1)

        # GetObject with future If-Modified-Since
        with pytest.raises(ClientError) as exc_info:
            s3_client.client.get_object(
                Bucket=bucket_name,
                Key=key,
                IfModifiedSince=future_time
            )

        error_code = exc_info.value.response['Error']['Code']
        assert error_code == 'NotModified' or error_code == '304', \
            f"Expected NotModified, got {error_code}"

    finally:
        fixture.cleanup()


def test_copy_object_if_match_success(s3_client, config):
    """
    Test CopyObject with CopySourceIfMatch (matching ETag)

    Should copy object when source ETag matches
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('copy-if-match')
        s3_client.create_bucket(bucket_name)

        source_key = 'source-object'
        dest_key = 'dest-object'
        data = fixture.generate_random_data(128)

        # Create source object
        put_response = s3_client.put_object(bucket_name, source_key, data)
        etag = put_response['ETag']

        # CopyObject with matching ETag
        copy_source = {'Bucket': bucket_name, 'Key': source_key}
        copy_response = s3_client.client.copy_object(
            CopySource=copy_source,
            Bucket=bucket_name,
            Key=dest_key,
            CopySourceIfMatch=etag
        )

        # Should succeed
        assert 'CopyObjectResult' in copy_response or 'ETag' in copy_response

        # Verify destination object exists
        head_response = s3_client.head_object(bucket_name, dest_key)
        assert head_response['ContentLength'] == 128

    finally:
        fixture.cleanup()


def test_copy_object_if_match_fails(s3_client, config):
    """
    Test CopyObject with CopySourceIfMatch (non-matching ETag)

    Should return PreconditionFailed when source ETag doesn't match
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('copy-if-match-fail')
        s3_client.create_bucket(bucket_name)

        source_key = 'source-object'
        dest_key = 'dest-object'
        data = fixture.generate_random_data(128)

        # Create source object
        s3_client.put_object(bucket_name, source_key, data)

        # CopyObject with non-matching ETag
        wrong_etag = '"00000000000000000000000000000000"'
        copy_source = {'Bucket': bucket_name, 'Key': source_key}

        with pytest.raises(ClientError) as exc_info:
            s3_client.client.copy_object(
                CopySource=copy_source,
                Bucket=bucket_name,
                Key=dest_key,
                CopySourceIfMatch=wrong_etag
            )

        error_code = exc_info.value.response['Error']['Code']
        assert error_code == 'PreconditionFailed' or error_code == '412', \
            f"Expected PreconditionFailed, got {error_code}"

    finally:
        fixture.cleanup()


def test_copy_object_if_none_match_fails(s3_client, config):
    """
    Test CopyObject with CopySourceIfNoneMatch (matching ETag)

    Should return PreconditionFailed when source ETag matches
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('copy-if-none-match')
        s3_client.create_bucket(bucket_name)

        source_key = 'source-object'
        dest_key = 'dest-object'
        data = fixture.generate_random_data(128)

        # Create source object
        put_response = s3_client.put_object(bucket_name, source_key, data)
        etag = put_response['ETag']

        # CopyObject with If-None-Match and matching ETag
        copy_source = {'Bucket': bucket_name, 'Key': source_key}

        with pytest.raises(ClientError) as exc_info:
            s3_client.client.copy_object(
                CopySource=copy_source,
                Bucket=bucket_name,
                Key=dest_key,
                CopySourceIfNoneMatch=etag
            )

        error_code = exc_info.value.response['Error']['Code']
        assert error_code == 'PreconditionFailed' or error_code == '412', \
            f"Expected PreconditionFailed, got {error_code}"

    finally:
        fixture.cleanup()


def test_copy_object_if_modified_since_success(s3_client, config):
    """
    Test CopyObject with CopySourceIfModifiedSince (object modified)

    Should copy object when source has been modified since timestamp
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('copy-if-mod-since')
        s3_client.create_bucket(bucket_name)

        source_key = 'source-object'
        dest_key = 'dest-object'
        data = fixture.generate_random_data(128)

        # Past timestamp (object is newer than this)
        past_time = datetime.now(timezone.utc) - timedelta(hours=1)

        # Create source object (after past_time)
        s3_client.put_object(bucket_name, source_key, data)

        # CopyObject with past If-Modified-Since should succeed
        copy_source = {'Bucket': bucket_name, 'Key': source_key}
        copy_response = s3_client.client.copy_object(
            CopySource=copy_source,
            Bucket=bucket_name,
            Key=dest_key,
            CopySourceIfModifiedSince=past_time
        )

        # Should succeed
        assert 'CopyObjectResult' in copy_response or 'ETag' in copy_response

    finally:
        fixture.cleanup()


def test_copy_object_if_unmodified_since_fails(s3_client, config):
    """
    Test CopyObject with CopySourceIfUnmodifiedSince (object modified)

    Should return PreconditionFailed when source has been modified after timestamp
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('copy-if-unmod-fail')
        s3_client.create_bucket(bucket_name)

        source_key = 'source-object'
        dest_key = 'dest-object'
        data = fixture.generate_random_data(128)

        # Past timestamp (object will be newer than this)
        past_time = datetime.now(timezone.utc) - timedelta(hours=1)

        # Create source object (after past_time)
        s3_client.put_object(bucket_name, source_key, data)

        # CopyObject with past If-Unmodified-Since should fail
        copy_source = {'Bucket': bucket_name, 'Key': source_key}

        with pytest.raises(ClientError) as exc_info:
            s3_client.client.copy_object(
                CopySource=copy_source,
                Bucket=bucket_name,
                Key=dest_key,
                CopySourceIfUnmodifiedSince=past_time
            )

        error_code = exc_info.value.response['Error']['Code']
        assert error_code == 'PreconditionFailed' or error_code == '412', \
            f"Expected PreconditionFailed, got {error_code}"

    finally:
        fixture.cleanup()
