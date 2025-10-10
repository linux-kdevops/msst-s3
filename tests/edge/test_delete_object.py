#!/usr/bin/env python3
"""
S3 DeleteObject Edge Case Tests

Tests DeleteObject API edge cases:
- Non-existing objects (should succeed)
- Directory-like objects
- Delete marker behavior
- Success status verification

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


def test_delete_object_success(s3_client, config):
    """
    Test basic DeleteObject operation

    Verifies object is deleted and no longer accessible
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('delete-success')
        s3_client.create_bucket(bucket_name)

        key = 'test-object'
        data = fixture.generate_random_data(100)
        s3_client.put_object(bucket_name, key, data)

        # Verify object exists
        head_response = s3_client.head_object(bucket_name, key)
        assert head_response['ContentLength'] == 100

        # Delete object
        delete_response = s3_client.delete_object(bucket_name, key)

        # Verify it was deleted
        with pytest.raises(ClientError) as exc_info:
            s3_client.head_object(bucket_name, key)

        error_code = exc_info.value.response['Error']['Code']
        assert error_code in ['404', 'NotFound'], \
            f"Expected NotFound after delete, got {error_code}"

    finally:
        fixture.cleanup()


def test_delete_object_non_existing(s3_client, config):
    """
    Test DeleteObject on non-existing object

    Should succeed (idempotent operation)
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('delete-nonexist')
        s3_client.create_bucket(bucket_name)

        # Delete non-existing object should succeed
        delete_response = s3_client.delete_object(bucket_name, 'non-existing-object')

        # Should not raise error
        assert delete_response is not None

    finally:
        fixture.cleanup()


def test_delete_object_twice(s3_client, config):
    """
    Test deleting same object twice

    Both deletes should succeed (idempotent)
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('delete-twice')
        s3_client.create_bucket(bucket_name)

        key = 'test-object'
        data = fixture.generate_random_data(50)
        s3_client.put_object(bucket_name, key, data)

        # First delete
        s3_client.delete_object(bucket_name, key)

        # Second delete (object already gone)
        delete_response = s3_client.delete_object(bucket_name, key)

        # Should succeed without error
        assert delete_response is not None

    finally:
        fixture.cleanup()


def test_delete_object_directory_object_noslash(s3_client, config):
    """
    Test deleting directory object without trailing slash

    Deleting 'dir' should not delete 'dir/' (different objects)
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('delete-dir-noslash')
        s3_client.create_bucket(bucket_name)

        # Create directory-like object with trailing slash
        dir_obj = 'my-obj/'
        s3_client.put_object(bucket_name, dir_obj, b'')

        # Try to delete without trailing slash
        s3_client.delete_object(bucket_name, 'my-obj')

        # Directory object with slash should still exist
        head_response = s3_client.head_object(bucket_name, dir_obj)
        assert head_response['ContentLength'] == 0

    finally:
        fixture.cleanup()


def test_delete_object_non_empty_directory(s3_client, config):
    """
    Test deleting directory object when nested objects exist

    Deleting 'dir/' should not delete 'dir/file'
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('delete-nonempty-dir')
        s3_client.create_bucket(bucket_name)

        # Create directory object and nested object
        dir_obj = 'foo/'
        nested_obj = 'foo/bar'

        s3_client.put_object(bucket_name, dir_obj, b'')
        s3_client.put_object(bucket_name, nested_obj, b'nested')

        # Delete directory object
        s3_client.delete_object(bucket_name, dir_obj)

        # List objects - nested object should still exist
        list_response = s3_client.client.list_objects_v2(Bucket=bucket_name)

        assert 'Contents' in list_response
        assert len(list_response['Contents']) == 1
        assert list_response['Contents'][0]['Key'] == nested_obj

    finally:
        fixture.cleanup()


def test_delete_object_with_special_characters(s3_client, config):
    """
    Test DeleteObject with special characters in key

    Verifies deletion works with various special characters
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('delete-special')
        s3_client.create_bucket(bucket_name)

        # Test keys with special characters
        special_keys = [
            'test-obj-with-dash',
            'test_obj_with_underscore',
            'test.obj.with.dots',
            'test obj with spaces',
            'test(obj)with(parens)',
        ]

        for key in special_keys:
            # Create object
            s3_client.put_object(bucket_name, key, b'test')

            # Delete object
            s3_client.delete_object(bucket_name, key)

            # Verify deleted
            with pytest.raises(ClientError) as exc_info:
                s3_client.head_object(bucket_name, key)

            error_code = exc_info.value.response['Error']['Code']
            assert error_code in ['404', 'NotFound'], \
                f"Key '{key}': Expected NotFound after delete"

    finally:
        fixture.cleanup()


def test_delete_object_returns_delete_marker(s3_client, config):
    """
    Test DeleteObject response includes DeleteMarker field

    For non-versioned buckets, DeleteMarker should be false or absent
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('delete-marker')
        s3_client.create_bucket(bucket_name)

        key = 'test-object'
        data = fixture.generate_random_data(100)
        s3_client.put_object(bucket_name, key, data)

        # Delete object
        delete_response = s3_client.client.delete_object(
            Bucket=bucket_name,
            Key=key
        )

        # For non-versioned bucket, DeleteMarker should be False or absent
        delete_marker = delete_response.get('DeleteMarker', False)
        assert delete_marker == False or delete_marker is None, \
            f"Expected DeleteMarker to be False for non-versioned bucket"

    finally:
        fixture.cleanup()


def test_delete_object_response_status(s3_client, config):
    """
    Test DeleteObject HTTP response metadata

    Verifies successful deletion returns proper HTTP status
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('delete-status')
        s3_client.create_bucket(bucket_name)

        key = 'test-object'
        data = fixture.generate_random_data(50)
        s3_client.put_object(bucket_name, key, data)

        # Delete and check response
        delete_response = s3_client.client.delete_object(
            Bucket=bucket_name,
            Key=key
        )

        # Response should have HTTPStatusCode
        status_code = delete_response['ResponseMetadata']['HTTPStatusCode']
        assert status_code in [200, 204], \
            f"Expected 200 or 204 status code, got {status_code}"

    finally:
        fixture.cleanup()
