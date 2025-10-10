#!/usr/bin/env python3
"""
S3 Object Naming and Path Tests

Tests object naming edge cases and path handling:
- Directory/file object conflicts
- Object overwrites
- Path validation
- Slash handling
- Invalid object names

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


def test_put_object_overwrite_dir_obj(s3_client, config):
    """
    Test PutObject trying to overwrite directory object with file

    Creating 'foo/' then 'foo' may fail on some implementations
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('overwrite-dir')
        s3_client.create_bucket(bucket_name)

        # Create directory object
        s3_client.put_object(bucket_name, 'foo/', b'')

        # Try to create file object with same name (no slash)
        # This may succeed (AWS S3) or fail (some implementations)
        try:
            s3_client.put_object(bucket_name, 'foo', b'data')
            # If it succeeds, verify both exist as separate objects
            assert s3_client.object_exists(bucket_name, 'foo/')
            assert s3_client.object_exists(bucket_name, 'foo')
        except ClientError as e:
            # Some implementations reject this
            error_code = e.response['Error']['Code']
            # Accept various error codes for this edge case
            assert error_code in ['ExistingObjectIsDirectory', 'InvalidRequest']

    finally:
        fixture.cleanup()


def test_put_object_overwrite_file_obj(s3_client, config):
    """
    Test PutObject trying to create directory when file exists

    Creating 'foo' then 'foo/' may fail on some implementations
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('overwrite-file')
        s3_client.create_bucket(bucket_name)

        # Create file object
        s3_client.put_object(bucket_name, 'foo', b'data')

        # Try to create directory object with same name
        # This may succeed (AWS S3) or fail (some implementations)
        try:
            s3_client.put_object(bucket_name, 'foo/', b'')
            # If it succeeds, verify both exist as separate objects
            assert s3_client.object_exists(bucket_name, 'foo')
            assert s3_client.object_exists(bucket_name, 'foo/')
        except ClientError as e:
            # Some implementations reject this
            error_code = e.response['Error']['Code']
            assert error_code in ['ObjectParentIsFile', 'InvalidRequest']

    finally:
        fixture.cleanup()


def test_put_object_overwrite_file_obj_with_nested_obj(s3_client, config):
    """
    Test PutObject with nested path when parent is a file

    Creating 'foo' then 'foo/bar' should fail on filesystem-based backends
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('nested-conflict')
        s3_client.create_bucket(bucket_name)

        # Create file object
        s3_client.put_object(bucket_name, 'foo', b'data')

        # Try to create nested object under file
        # AWS S3 allows this (objects are independent)
        # Filesystem-based backends may reject it
        try:
            s3_client.put_object(bucket_name, 'foo/bar', b'nested data')
            # If it succeeds, verify both exist
            assert s3_client.object_exists(bucket_name, 'foo')
            assert s3_client.object_exists(bucket_name, 'foo/bar')
        except ClientError as e:
            # Filesystem-based backends may reject
            error_code = e.response['Error']['Code']
            assert error_code in ['ObjectParentIsFile', 'InvalidRequest']

    finally:
        fixture.cleanup()


def test_put_object_dir_obj_with_data(s3_client, config):
    """
    Test PutObject directory object with non-empty data

    Directory objects typically have empty body
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('dir-data')
        s3_client.create_bucket(bucket_name)

        # Create directory object with data
        # This should succeed - directory objects can have data
        s3_client.put_object(bucket_name, 'dir/', b'directory data')

        # Verify it exists and has data
        get_response = s3_client.get_object(bucket_name, 'dir/')
        data = get_response['Body'].read()
        assert data == b'directory data'

    finally:
        fixture.cleanup()


def test_put_object_with_slashes(s3_client, config):
    """
    Test PutObject with various slash patterns

    MinIO rejects leading/double slashes - this is implementation-specific
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('slashes')
        s3_client.create_bucket(bucket_name)

        # Keys that should work
        valid_keys = [
            'path/to/file.txt',      # Normal path
            'trailing/slash/'        # Trailing slash
        ]

        for key in valid_keys:
            s3_client.put_object(bucket_name, key, b'data')
            assert s3_client.object_exists(bucket_name, key)

        # Keys that MinIO rejects (AWS S3 may accept)
        invalid_keys = [
            'path//double//slash',    # Double slashes
            '///leading/slashes',     # Leading slashes
            'mixed///slashes///'      # Mixed patterns
        ]

        for key in invalid_keys:
            try:
                s3_client.put_object(bucket_name, key, b'data')
                # If it succeeds, that's also valid
            except ClientError as e:
                # MinIO rejects with various error codes
                error_code = e.response['Error']['Code']
                assert error_code in ['XMinioInvalidResourceName', 'XMinioInvalidObjectName', 'InvalidObjectName']

    finally:
        fixture.cleanup()


def test_put_object_leading_slash(s3_client, config):
    """
    Test PutObject with leading slash

    MinIO rejects leading slashes - this is implementation-specific
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('leading-slash')
        s3_client.create_bucket(bucket_name)

        key = '/file-with-leading-slash.txt'

        try:
            s3_client.put_object(bucket_name, key, b'data')
            # If it succeeds (AWS S3), verify retrieval
            get_response = s3_client.get_object(bucket_name, key)
            assert get_response['Body'].read() == b'data'
        except ClientError as e:
            # MinIO rejects leading slashes
            error_code = e.response['Error']['Code']
            assert error_code in ['XMinioInvalidResourceName', 'InvalidObjectName']

    finally:
        fixture.cleanup()


def test_put_object_consecutive_slashes(s3_client, config):
    """
    Test PutObject with consecutive slashes in key

    MinIO rejects consecutive slashes - this is implementation-specific
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('consec-slash')
        s3_client.create_bucket(bucket_name)

        # Keys with consecutive slashes
        keys = [
            'path//file.txt',
            'path///file.txt',
            'path////file.txt'
        ]

        created_keys = []
        for key in keys:
            try:
                s3_client.put_object(bucket_name, key, b'data')
                created_keys.append(key)
            except ClientError as e:
                # MinIO rejects consecutive slashes
                error_code = e.response['Error']['Code']
                assert error_code in ['XMinioInvalidResourceName', 'XMinioInvalidObjectName', 'InvalidObjectName']

        # If any were created, verify they're distinct
        if created_keys:
            objects = s3_client.list_objects(bucket_name)
            retrieved_keys = {obj['Key'] for obj in objects}
            assert retrieved_keys == set(created_keys)

    finally:
        fixture.cleanup()


def test_put_object_empty_key_segments(s3_client, config):
    """
    Test PutObject with empty key segments (slashes)

    MinIO rejects empty segments - this is implementation-specific
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('empty-seg')
        s3_client.create_bucket(bucket_name)

        key = 'a//b//c'

        try:
            s3_client.put_object(bucket_name, key, b'data')
            # If it succeeds, verify retrieval
            assert s3_client.object_exists(bucket_name, key)
            get_response = s3_client.get_object(bucket_name, key)
            assert get_response['Body'].read() == b'data'
        except ClientError as e:
            # MinIO rejects consecutive slashes
            error_code = e.response['Error']['Code']
            assert error_code in ['XMinioInvalidResourceName', 'XMinioInvalidObjectName', 'InvalidObjectName']

    finally:
        fixture.cleanup()


def test_put_object_dot_segments(s3_client, config):
    """
    Test PutObject with dot segments in key

    MinIO rejects dot segments - this is implementation-specific
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('dots')
        s3_client.create_bucket(bucket_name)

        # Keys with dots (AWS S3 treats as literal, MinIO rejects)
        keys = [
            './file.txt',
            '../file.txt',
            'path/./file.txt',
            'path/../file.txt'
        ]

        created_keys = []
        for key in keys:
            try:
                s3_client.put_object(bucket_name, key, b'data')
                created_keys.append(key)
            except ClientError as e:
                # MinIO rejects dot segments
                error_code = e.response['Error']['Code']
                assert error_code in ['XMinioInvalidResourceName', 'XMinioInvalidObjectName', 'InvalidObjectName']

        # If any were created, verify they exist
        if created_keys:
            objects = s3_client.list_objects(bucket_name)
            retrieved_keys = {obj['Key'] for obj in objects}
            assert retrieved_keys == set(created_keys)

    finally:
        fixture.cleanup()


def test_put_object_overwrite_same_key(s3_client, config):
    """
    Test PutObject overwrites existing object with same key

    Should update the object content and metadata
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('overwrite')
        s3_client.create_bucket(bucket_name)

        key = 'overwrite-test.txt'

        # Create initial object
        put1 = s3_client.put_object(bucket_name, key, b'version 1')
        etag1 = put1['ETag']

        # Overwrite with new content
        put2 = s3_client.put_object(bucket_name, key, b'version 2')
        etag2 = put2['ETag']

        # ETags should be different
        assert etag1 != etag2

        # Content should be updated
        get_response = s3_client.get_object(bucket_name, key)
        assert get_response['Body'].read() == b'version 2'

    finally:
        fixture.cleanup()


def test_put_object_case_sensitive_keys(s3_client, config):
    """
    Test PutObject with different case variations

    Object keys are case-sensitive
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('case-sens')
        s3_client.create_bucket(bucket_name)

        # Different case variations
        keys = [
            'File.txt',
            'file.txt',
            'FILE.TXT',
            'FiLe.TxT'
        ]

        for key in keys:
            s3_client.put_object(bucket_name, key, b'data')

        # All should be distinct objects
        objects = s3_client.list_objects(bucket_name)
        assert len(objects) == len(keys)

        retrieved_keys = {obj['Key'] for obj in objects}
        assert retrieved_keys == set(keys)

    finally:
        fixture.cleanup()


def test_put_object_unicode_in_key(s3_client, config):
    """
    Test PutObject with Unicode characters in key

    Unicode should be supported in object keys
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('unicode-key')
        s3_client.create_bucket(bucket_name)

        # Unicode key (already tested in special_characters, but verify)
        key = 'файл-文件-ファイル.txt'
        s3_client.put_object(bucket_name, key, b'unicode data')

        # Verify retrieval
        get_response = s3_client.get_object(bucket_name, key)
        assert get_response['Body'].read() == b'unicode data'

    finally:
        fixture.cleanup()
