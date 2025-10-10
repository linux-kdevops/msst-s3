#!/usr/bin/env python3
"""
S3 ListObjects v1 Tests

Tests ListObjects (v1) API edge cases:
- Prefix and delimiter filtering
- Pagination with marker
- MaxKeys parameter variations
- Common prefixes
- Non-existing buckets
- Response validation

Note: ListObjects is the v1 API (older than ListObjectsV2).
Many applications still use it for compatibility.

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


def test_list_objects_non_existing_bucket(s3_client, config):
    """
    Test ListObjects on non-existing bucket

    Should return NoSuchBucket error
    """
    fixture = TestFixture(s3_client, config)

    try:
        with pytest.raises(ClientError) as exc_info:
            s3_client.client.list_objects(
                Bucket='non-existing-bucket-12345'
            )

        error_code = exc_info.value.response['Error']['Code']
        assert error_code == 'NoSuchBucket', \
            f"Expected NoSuchBucket, got {error_code}"

    finally:
        fixture.cleanup()


def test_list_objects_with_prefix(s3_client, config):
    """
    Test ListObjects with Prefix parameter

    Should return only objects matching prefix
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('list-prefix')
        s3_client.create_bucket(bucket_name)

        prefix = "obj"
        # Objects with prefix
        with_prefix = [f'{prefix}/bar', f'{prefix}/baz/bla', f'{prefix}/foo']
        # Objects without prefix
        without_prefix = ['azy/csf', 'hell']

        # Create all objects
        for key in with_prefix + without_prefix:
            s3_client.put_object(bucket_name, key, b'data')

        # List with prefix
        list_response = s3_client.client.list_objects(
            Bucket=bucket_name,
            Prefix=prefix
        )

        # Verify prefix in response
        assert list_response.get('Prefix') == prefix

        # Verify only prefixed objects returned
        returned_keys = [obj['Key'] for obj in list_response.get('Contents', [])]
        assert len(returned_keys) == len(with_prefix)
        assert set(returned_keys) == set(with_prefix)

    finally:
        fixture.cleanup()


def test_list_objects_paginated(s3_client, config):
    """
    Test ListObjects pagination with Marker

    Should support pagination through results
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('list-page')
        s3_client.create_bucket(bucket_name)

        # Create objects
        keys = ['dir1/subdir/file.txt', 'dir1/subdir.ext',
                'dir1/subdir1.ext', 'dir1/subdir2.ext']
        for key in keys:
            s3_client.put_object(bucket_name, key, b'data')

        # First page with MaxKeys=2
        page1 = s3_client.client.list_objects(
            Bucket=bucket_name,
            MaxKeys=2
        )

        assert page1['IsTruncated'] is True
        assert len(page1['Contents']) == 2
        assert 'NextMarker' in page1

        # Second page using Marker
        page2 = s3_client.client.list_objects(
            Bucket=bucket_name,
            Marker=page1['NextMarker'],
            MaxKeys=2
        )

        assert len(page2['Contents']) == 2

        # Verify all objects retrieved across pages
        all_keys = [obj['Key'] for obj in page1['Contents']] + \
                   [obj['Key'] for obj in page2['Contents']]
        assert set(all_keys) == set(keys)

    finally:
        fixture.cleanup()


def test_list_objects_truncated(s3_client, config):
    """
    Test ListObjects IsTruncated flag

    Should indicate when results are truncated
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('list-trunc')
        s3_client.create_bucket(bucket_name)

        # Create 5 objects
        for i in range(5):
            s3_client.put_object(bucket_name, f'object-{i}', b'data')

        # List with MaxKeys=3 (truncated)
        truncated_response = s3_client.client.list_objects(
            Bucket=bucket_name,
            MaxKeys=3
        )

        assert truncated_response['IsTruncated'] is True
        assert len(truncated_response['Contents']) == 3

        # List all (not truncated)
        full_response = s3_client.client.list_objects(
            Bucket=bucket_name
        )

        assert full_response.get('IsTruncated', False) is False
        assert len(full_response['Contents']) == 5

    finally:
        fixture.cleanup()


def test_list_objects_invalid_max_keys(s3_client, config):
    """
    Test ListObjects with invalid MaxKeys

    Negative MaxKeys should be rejected
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('list-invalid')
        s3_client.create_bucket(bucket_name)

        with pytest.raises(Exception) as exc_info:
            s3_client.client.list_objects(
                Bucket=bucket_name,
                MaxKeys=-1
            )

        # May be validation error or parameter error
        assert exc_info.value is not None

    finally:
        fixture.cleanup()


def test_list_objects_max_keys_zero(s3_client, config):
    """
    Test ListObjects with MaxKeys=0

    Should return no objects but valid response
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('list-zero')
        s3_client.create_bucket(bucket_name)

        # Create some objects
        for i in range(3):
            s3_client.put_object(bucket_name, f'obj-{i}', b'data')

        # List with MaxKeys=0
        list_response = s3_client.client.list_objects(
            Bucket=bucket_name,
            MaxKeys=0
        )

        # Should return no contents
        assert len(list_response.get('Contents', [])) == 0
        assert list_response.get('IsTruncated', False) is False

    finally:
        fixture.cleanup()


def test_list_objects_delimiter(s3_client, config):
    """
    Test ListObjects with Delimiter for directory-like listing

    Should return CommonPrefixes for directory-like structures
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('list-delim')
        s3_client.create_bucket(bucket_name)

        # Create directory-like structure
        keys = [
            'dir1/file1.txt',
            'dir1/file2.txt',
            'dir2/file1.txt',
            'file-at-root.txt'
        ]
        for key in keys:
            s3_client.put_object(bucket_name, key, b'data')

        # List with delimiter '/'
        list_response = s3_client.client.list_objects(
            Bucket=bucket_name,
            Delimiter='/'
        )

        # Should have CommonPrefixes for directories
        common_prefixes = list_response.get('CommonPrefixes', [])
        prefix_list = [cp['Prefix'] for cp in common_prefixes]
        assert 'dir1/' in prefix_list
        assert 'dir2/' in prefix_list

        # Should have root-level file in Contents
        contents = list_response.get('Contents', [])
        content_keys = [obj['Key'] for obj in contents]
        assert 'file-at-root.txt' in content_keys

    finally:
        fixture.cleanup()


def test_list_objects_marker_not_from_obj_list(s3_client, config):
    """
    Test ListObjects with Marker not in object list

    Marker doesn't have to be an existing key
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('list-marker')
        s3_client.create_bucket(bucket_name)

        # Create objects: obj-a, obj-c, obj-e
        keys = ['obj-a', 'obj-c', 'obj-e']
        for key in keys:
            s3_client.put_object(bucket_name, key, b'data')

        # Use marker 'obj-b' which doesn't exist
        list_response = s3_client.client.list_objects(
            Bucket=bucket_name,
            Marker='obj-b'
        )

        # Should return objects after 'obj-b' lexicographically
        returned_keys = [obj['Key'] for obj in list_response.get('Contents', [])]
        assert 'obj-c' in returned_keys
        assert 'obj-e' in returned_keys
        assert 'obj-a' not in returned_keys

    finally:
        fixture.cleanup()


def test_list_objects_list_all_objs(s3_client, config):
    """
    Test ListObjects lists all objects

    Should return all objects when no filters applied
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('list-all')
        s3_client.create_bucket(bucket_name)

        # Create various objects
        keys = [
            'file1.txt',
            'dir/file2.txt',
            'dir/subdir/file3.txt',
            'another-file.txt'
        ]
        for key in keys:
            s3_client.put_object(bucket_name, key, b'data')

        # List all objects
        list_response = s3_client.client.list_objects(
            Bucket=bucket_name
        )

        returned_keys = [obj['Key'] for obj in list_response.get('Contents', [])]
        assert len(returned_keys) == len(keys)
        assert set(returned_keys) == set(keys)

    finally:
        fixture.cleanup()


def test_list_objects_nested_dir_file_objs(s3_client, config):
    """
    Test ListObjects with nested directory structure

    Should handle deep hierarchies correctly
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('list-nested')
        s3_client.create_bucket(bucket_name)

        # Create nested structure
        keys = [
            'a/b/c/file1.txt',
            'a/b/file2.txt',
            'a/file3.txt',
            'file4.txt'
        ]
        for key in keys:
            s3_client.put_object(bucket_name, key, b'data')

        # List with prefix 'a/b/'
        list_response = s3_client.client.list_objects(
            Bucket=bucket_name,
            Prefix='a/b/'
        )

        returned_keys = [obj['Key'] for obj in list_response.get('Contents', [])]
        assert 'a/b/c/file1.txt' in returned_keys
        assert 'a/b/file2.txt' in returned_keys
        assert 'a/file3.txt' not in returned_keys
        assert 'file4.txt' not in returned_keys

    finally:
        fixture.cleanup()


def test_list_objects_empty_bucket(s3_client, config):
    """
    Test ListObjects on empty bucket

    Should return empty Contents list
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('list-empty')
        s3_client.create_bucket(bucket_name)

        # List empty bucket
        list_response = s3_client.client.list_objects(
            Bucket=bucket_name
        )

        assert len(list_response.get('Contents', [])) == 0
        assert list_response.get('IsTruncated', False) is False

    finally:
        fixture.cleanup()


def test_list_objects_prefix_and_delimiter(s3_client, config):
    """
    Test ListObjects with both Prefix and Delimiter

    Should combine prefix filtering with delimiter grouping
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('list-both')
        s3_client.create_bucket(bucket_name)

        # Create structure
        keys = [
            'logs/2024/01/file1.log',
            'logs/2024/02/file2.log',
            'logs/2024/file3.log',
            'data/file4.txt'
        ]
        for key in keys:
            s3_client.put_object(bucket_name, key, b'data')

        # List with prefix='logs/2024/' and delimiter='/'
        list_response = s3_client.client.list_objects(
            Bucket=bucket_name,
            Prefix='logs/2024/',
            Delimiter='/'
        )

        # Should show month directories as CommonPrefixes
        common_prefixes = [cp['Prefix'] for cp in list_response.get('CommonPrefixes', [])]
        assert 'logs/2024/01/' in common_prefixes
        assert 'logs/2024/02/' in common_prefixes

        # Should show file at prefix level in Contents
        contents = [obj['Key'] for obj in list_response.get('Contents', [])]
        assert 'logs/2024/file3.log' in contents

    finally:
        fixture.cleanup()
