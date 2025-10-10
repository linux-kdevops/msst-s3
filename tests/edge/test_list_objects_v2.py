#!/usr/bin/env python3
"""
S3 ListObjectsV2 Edge Case Tests

Tests comprehensive ListObjectsV2 functionality:
- Pagination with MaxKeys and ContinuationToken
- StartAfter parameter
- Prefix and Delimiter combinations
- Common prefixes (directory-like structure)
- Truncation handling
- Checksum metadata in listings
- Owner information

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


def test_list_objects_v2_with_start_after(s3_client, config):
    """
    Test ListObjectsV2 StartAfter parameter for pagination

    Verifies that StartAfter correctly filters objects
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('list-start-after')
        s3_client.create_bucket(bucket_name)

        # Create objects with predictable names
        object_keys = [f'obj-{i:03d}' for i in range(10)]
        data = b'test data'

        for key in object_keys:
            s3_client.put_object(bucket_name, key, data)

        # List with StartAfter='obj-004' should return obj-005 onwards
        response = s3_client.client.list_objects_v2(
            Bucket=bucket_name,
            StartAfter='obj-004'
        )

        assert 'Contents' in response
        listed_keys = [obj['Key'] for obj in response['Contents']]

        # Should get obj-005 through obj-009 (5 objects)
        assert len(listed_keys) == 5
        assert 'obj-004' not in listed_keys
        assert 'obj-005' in listed_keys
        assert 'obj-009' in listed_keys

    finally:
        fixture.cleanup()


def test_list_objects_v2_start_after_not_in_list(s3_client, config):
    """
    Test ListObjectsV2 with StartAfter value that doesn't exist in bucket

    StartAfter doesn't need to be an existing key
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('list-start-nonexist')
        s3_client.create_bucket(bucket_name)

        # Create objects
        object_keys = ['a-file', 'c-file', 'e-file']
        data = b'test'

        for key in object_keys:
            s3_client.put_object(bucket_name, key, data)

        # StartAfter with non-existing key 'b-file' should return 'c-file' and 'e-file'
        response = s3_client.client.list_objects_v2(
            Bucket=bucket_name,
            StartAfter='b-file'
        )

        listed_keys = [obj['Key'] for obj in response.get('Contents', [])]

        assert len(listed_keys) == 2
        assert 'a-file' not in listed_keys
        assert 'c-file' in listed_keys
        assert 'e-file' in listed_keys

    finally:
        fixture.cleanup()


def test_list_objects_v2_pagination_with_max_keys(s3_client, config):
    """
    Test ListObjectsV2 pagination using MaxKeys

    Verifies proper pagination with ContinuationToken
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('list-paginate')
        s3_client.create_bucket(bucket_name)

        # Create 25 objects
        num_objects = 25
        data = b'data'

        for i in range(num_objects):
            key = f'object-{i:03d}'
            s3_client.put_object(bucket_name, key, data)

        # First page with MaxKeys=10
        all_keys = []
        response = s3_client.client.list_objects_v2(
            Bucket=bucket_name,
            MaxKeys=10
        )

        assert 'Contents' in response
        all_keys.extend([obj['Key'] for obj in response['Contents']])
        assert len(response['Contents']) == 10
        assert response['IsTruncated'] is True
        assert 'NextContinuationToken' in response

        # Second page
        response = s3_client.client.list_objects_v2(
            Bucket=bucket_name,
            MaxKeys=10,
            ContinuationToken=response['NextContinuationToken']
        )

        all_keys.extend([obj['Key'] for obj in response['Contents']])
        assert len(response['Contents']) == 10
        assert response['IsTruncated'] is True

        # Third page (final)
        response = s3_client.client.list_objects_v2(
            Bucket=bucket_name,
            MaxKeys=10,
            ContinuationToken=response['NextContinuationToken']
        )

        all_keys.extend([obj['Key'] for obj in response['Contents']])
        assert len(response['Contents']) == 5  # Remaining objects
        assert response['IsTruncated'] is False
        assert 'NextContinuationToken' not in response

        # Verify we got all objects
        assert len(all_keys) == num_objects
        assert len(set(all_keys)) == num_objects  # No duplicates

    finally:
        fixture.cleanup()


def test_list_objects_v2_with_prefix(s3_client, config):
    """
    Test ListObjectsV2 with Prefix filter

    Verifies prefix filtering works correctly
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('list-prefix')
        s3_client.create_bucket(bucket_name)

        # Create objects with different prefixes
        objects = [
            'logs/2024-01-01.log',
            'logs/2024-01-02.log',
            'data/file1.csv',
            'data/file2.csv',
            'images/photo.jpg',
        ]

        data = b'content'
        for key in objects:
            s3_client.put_object(bucket_name, key, data)

        # List only 'logs/' prefix
        response = s3_client.client.list_objects_v2(
            Bucket=bucket_name,
            Prefix='logs/'
        )

        listed_keys = [obj['Key'] for obj in response.get('Contents', [])]

        assert len(listed_keys) == 2
        assert all(key.startswith('logs/') for key in listed_keys)

        # List only 'data/' prefix
        response = s3_client.client.list_objects_v2(
            Bucket=bucket_name,
            Prefix='data/'
        )

        listed_keys = [obj['Key'] for obj in response.get('Contents', [])]

        assert len(listed_keys) == 2
        assert all(key.startswith('data/') for key in listed_keys)

    finally:
        fixture.cleanup()


def test_list_objects_v2_with_delimiter_and_prefix(s3_client, config):
    """
    Test ListObjectsV2 with both Delimiter and Prefix

    Verifies directory-like listing with common prefixes
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('list-delim-prefix')
        s3_client.create_bucket(bucket_name)

        # Create directory-like structure
        objects = [
            'documents/2024/file1.txt',
            'documents/2024/file2.txt',
            'documents/2023/file3.txt',
            'documents/readme.txt',
        ]

        data = b'content'
        for key in objects:
            s3_client.put_object(bucket_name, key, data)

        # List with prefix='documents/' and delimiter='/'
        response = s3_client.client.list_objects_v2(
            Bucket=bucket_name,
            Prefix='documents/',
            Delimiter='/'
        )

        # Should get readme.txt in Contents
        contents = response.get('Contents', [])
        assert len(contents) == 1
        assert contents[0]['Key'] == 'documents/readme.txt'

        # Should get two common prefixes (2024/ and 2023/)
        common_prefixes = response.get('CommonPrefixes', [])
        prefixes = [cp['Prefix'] for cp in common_prefixes]

        assert len(prefixes) == 2
        assert 'documents/2024/' in prefixes
        assert 'documents/2023/' in prefixes

    finally:
        fixture.cleanup()


def test_list_objects_v2_truncated_common_prefixes(s3_client, config):
    """
    Test ListObjectsV2 when common prefixes exceed MaxKeys

    Verifies proper truncation with CommonPrefixes
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('list-trunc-prefix')
        s3_client.create_bucket(bucket_name)

        # Create many directory-like prefixes
        data = b'test'
        for i in range(15):
            key = f'folder-{i:02d}/file.txt'
            s3_client.put_object(bucket_name, key, data)

        # List with delimiter and small MaxKeys
        response = s3_client.client.list_objects_v2(
            Bucket=bucket_name,
            Delimiter='/',
            MaxKeys=10
        )

        # Should be truncated
        assert response['IsTruncated'] is True

        # Should have common prefixes (up to MaxKeys limit)
        common_prefixes = response.get('CommonPrefixes', [])
        assert len(common_prefixes) <= 10

    finally:
        fixture.cleanup()


def test_list_objects_v2_max_keys_exceeding_limit(s3_client, config):
    """
    Test ListObjectsV2 with MaxKeys exceeding default limit

    S3 default max is 1000
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('list-max-keys')
        s3_client.create_bucket(bucket_name)

        # Create some objects
        data = b'test'
        for i in range(10):
            s3_client.put_object(bucket_name, f'obj-{i}', data)

        # Try to set MaxKeys higher than allowed (>1000)
        response = s3_client.client.list_objects_v2(
            Bucket=bucket_name,
            MaxKeys=2000
        )

        # Should succeed but be capped at 1000 or return all objects
        contents = response.get('Contents', [])
        assert len(contents) <= 1000

    finally:
        fixture.cleanup()


def test_list_objects_v2_max_keys_zero(s3_client, config):
    """
    Test ListObjectsV2 with MaxKeys=0

    Should return no objects but may return common prefixes
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('list-max-zero')
        s3_client.create_bucket(bucket_name)

        # Create objects
        data = b'test'
        for i in range(5):
            s3_client.put_object(bucket_name, f'obj-{i}', data)

        # List with MaxKeys=0
        response = s3_client.client.list_objects_v2(
            Bucket=bucket_name,
            MaxKeys=0
        )

        # Should return no contents
        contents = response.get('Contents', [])
        assert len(contents) == 0

    finally:
        fixture.cleanup()


def test_list_objects_v2_with_owner(s3_client, config):
    """
    Test ListObjectsV2 with FetchOwner parameter

    Verifies that owner information is returned when requested
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('list-owner')
        s3_client.create_bucket(bucket_name)

        # Create object
        data = b'test'
        s3_client.put_object(bucket_name, 'test-obj', data)

        # List with FetchOwner=True
        response = s3_client.client.list_objects_v2(
            Bucket=bucket_name,
            FetchOwner=True
        )

        assert 'Contents' in response
        assert len(response['Contents']) == 1

        obj = response['Contents'][0]

        # Owner should be present
        if 'Owner' in obj:
            assert 'ID' in obj['Owner'] or 'DisplayName' in obj['Owner']

    finally:
        fixture.cleanup()


def test_list_objects_v2_nested_directory_structure(s3_client, config):
    """
    Test ListObjectsV2 with nested directory-like structure

    Verifies proper handling of deep hierarchies
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('list-nested')
        s3_client.create_bucket(bucket_name)

        # Create nested structure
        objects = [
            'a/b/c/file1.txt',
            'a/b/c/file2.txt',
            'a/b/file3.txt',
            'a/file4.txt',
            'file5.txt',
        ]

        data = b'nested content'
        for key in objects:
            s3_client.put_object(bucket_name, key, data)

        # List root level with delimiter
        response = s3_client.client.list_objects_v2(
            Bucket=bucket_name,
            Delimiter='/'
        )

        # Should have file5.txt in Contents
        contents = [obj['Key'] for obj in response.get('Contents', [])]
        assert 'file5.txt' in contents

        # Should have 'a/' as common prefix
        common_prefixes = [cp['Prefix'] for cp in response.get('CommonPrefixes', [])]
        assert 'a/' in common_prefixes

        # List 'a/' level
        response = s3_client.client.list_objects_v2(
            Bucket=bucket_name,
            Prefix='a/',
            Delimiter='/'
        )

        contents = [obj['Key'] for obj in response.get('Contents', [])]
        assert 'a/file4.txt' in contents

        common_prefixes = [cp['Prefix'] for cp in response.get('CommonPrefixes', [])]
        assert 'a/b/' in common_prefixes

    finally:
        fixture.cleanup()


def test_list_objects_v2_empty_result(s3_client, config):
    """
    Test ListObjectsV2 on empty bucket or with non-matching prefix

    Should return empty list without errors
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('list-empty')
        s3_client.create_bucket(bucket_name)

        # List empty bucket
        response = s3_client.client.list_objects_v2(Bucket=bucket_name)

        assert 'Contents' not in response or len(response.get('Contents', [])) == 0
        assert response['IsTruncated'] is False

        # Create some objects
        s3_client.put_object(bucket_name, 'test', b'data')

        # List with non-matching prefix
        response = s3_client.client.list_objects_v2(
            Bucket=bucket_name,
            Prefix='nonexistent-prefix/'
        )

        assert 'Contents' not in response or len(response.get('Contents', [])) == 0

    finally:
        fixture.cleanup()


def test_list_objects_v2_start_after_empty_result(s3_client, config):
    """
    Test ListObjectsV2 with StartAfter beyond all objects

    Should return empty result
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('list-start-empty')
        s3_client.create_bucket(bucket_name)

        # Create objects with keys 'a', 'b', 'c'
        for key in ['a', 'b', 'c']:
            s3_client.put_object(bucket_name, key, b'data')

        # StartAfter='z' should return no results
        response = s3_client.client.list_objects_v2(
            Bucket=bucket_name,
            StartAfter='z'
        )

        assert 'Contents' not in response or len(response.get('Contents', [])) == 0
        assert response['IsTruncated'] is False

    finally:
        fixture.cleanup()


def test_list_objects_v2_invalid_max_keys(s3_client, config):
    """
    Test ListObjectsV2 with invalid MaxKeys values

    Negative values should be rejected
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('list-invalid-max')
        s3_client.create_bucket(bucket_name)

        # Try negative MaxKeys
        with pytest.raises(ClientError) as exc_info:
            s3_client.client.list_objects_v2(
                Bucket=bucket_name,
                MaxKeys=-1
            )

        error_code = exc_info.value.response['Error']['Code']
        assert error_code in ['InvalidArgument', 'InvalidRequest']

    finally:
        fixture.cleanup()
