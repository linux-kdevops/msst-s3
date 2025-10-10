#!/usr/bin/env python3
"""
S3 DeleteObjects (Batch Delete) Tests

Tests DeleteObjects API for batch deletion:
- Basic batch deletion
- Empty input handling
- Non-existing objects
- Mixed existing/non-existing
- Error handling
- Response validation

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


def test_delete_objects_success(s3_client, config):
    """
    Test DeleteObjects with multiple objects

    Should delete specified objects and leave others intact
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('delete-batch')
        s3_client.create_bucket(bucket_name)

        # Create 6 objects
        to_delete = ['foo', 'bar', 'baz']
        to_keep = ['obj1', 'obj2', 'obj3']
        all_objects = to_delete + to_keep

        for key in all_objects:
            s3_client.put_object(bucket_name, key, b'test data')

        # Delete 3 objects
        delete_response = s3_client.client.delete_objects(
            Bucket=bucket_name,
            Delete={
                'Objects': [{'Key': key} for key in to_delete]
            }
        )

        # Verify response
        assert 'Deleted' in delete_response
        assert len(delete_response['Deleted']) == 3

        deleted_keys = {obj['Key'] for obj in delete_response['Deleted']}
        assert deleted_keys == set(to_delete)

        # Verify errors (should be none)
        assert len(delete_response.get('Errors', [])) == 0

        # List remaining objects
        remaining_objects = s3_client.list_objects(bucket_name)
        remaining_keys = {obj['Key'] for obj in remaining_objects}
        assert remaining_keys == set(to_keep)

    finally:
        fixture.cleanup()


def test_delete_objects_empty_input(s3_client, config):
    """
    Test DeleteObjects with empty object list

    boto3 requires at least 1 object, so this tests with a non-existing key
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('delete-empty')
        s3_client.create_bucket(bucket_name)

        # Create some objects
        for key in ['foo', 'bar', 'baz']:
            s3_client.put_object(bucket_name, key, b'data')

        # Delete non-existing object (empty-like behavior)
        delete_response = s3_client.client.delete_objects(
            Bucket=bucket_name,
            Delete={
                'Objects': [{'Key': 'non-existing-key'}]
            }
        )

        # Should report as deleted (idempotent)
        assert len(delete_response.get('Deleted', [])) == 1
        assert len(delete_response.get('Errors', [])) == 0

        # Verify all original objects still exist
        remaining_objects = s3_client.list_objects(bucket_name)
        assert len(remaining_objects) == 3

    finally:
        fixture.cleanup()


def test_delete_objects_non_existing_objects(s3_client, config):
    """
    Test DeleteObjects with non-existing objects

    Should succeed (idempotent) and report deletions
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('delete-nonexist')
        s3_client.create_bucket(bucket_name)

        # Delete non-existing objects
        delete_response = s3_client.client.delete_objects(
            Bucket=bucket_name,
            Delete={
                'Objects': [
                    {'Key': 'obj1'},
                    {'Key': 'obj2'}
                ]
            }
        )

        # Should report as deleted (idempotent behavior)
        assert len(delete_response['Deleted']) == 2
        assert len(delete_response.get('Errors', [])) == 0

        deleted_keys = {obj['Key'] for obj in delete_response['Deleted']}
        assert deleted_keys == {'obj1', 'obj2'}

    finally:
        fixture.cleanup()


def test_delete_objects_mixed_existing_non_existing(s3_client, config):
    """
    Test DeleteObjects with mix of existing and non-existing

    Should delete all specified keys regardless of existence
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('delete-mixed')
        s3_client.create_bucket(bucket_name)

        # Create only some objects
        s3_client.put_object(bucket_name, 'exists1', b'data')
        s3_client.put_object(bucket_name, 'exists2', b'data')

        # Delete mix of existing and non-existing
        delete_response = s3_client.client.delete_objects(
            Bucket=bucket_name,
            Delete={
                'Objects': [
                    {'Key': 'exists1'},
                    {'Key': 'not-exists1'},
                    {'Key': 'exists2'},
                    {'Key': 'not-exists2'}
                ]
            }
        )

        # All should be reported as deleted
        assert len(delete_response['Deleted']) == 4
        assert len(delete_response.get('Errors', [])) == 0

        deleted_keys = {obj['Key'] for obj in delete_response['Deleted']}
        assert deleted_keys == {'exists1', 'not-exists1', 'exists2', 'not-exists2'}

        # Verify bucket is empty
        remaining_objects = s3_client.list_objects(bucket_name)
        assert len(remaining_objects) == 0

    finally:
        fixture.cleanup()


def test_delete_objects_non_existing_bucket(s3_client, config):
    """
    Test DeleteObjects on non-existing bucket

    Should return NoSuchBucket error
    """
    fixture = TestFixture(s3_client, config)

    try:
        with pytest.raises(ClientError) as exc_info:
            s3_client.client.delete_objects(
                Bucket='non-existing-bucket-12345',
                Delete={
                    'Objects': [{'Key': 'obj1'}]
                }
            )

        error_code = exc_info.value.response['Error']['Code']
        assert error_code == 'NoSuchBucket', \
            f"Expected NoSuchBucket, got {error_code}"

    finally:
        fixture.cleanup()


def test_delete_objects_returns_deleted_list(s3_client, config):
    """
    Test DeleteObjects returns deleted object list

    Response should contain all deleted keys
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('delete-list')
        s3_client.create_bucket(bucket_name)

        keys_to_delete = ['file1.txt', 'file2.txt', 'file3.txt']
        for key in keys_to_delete:
            s3_client.put_object(bucket_name, key, b'data')

        delete_response = s3_client.client.delete_objects(
            Bucket=bucket_name,
            Delete={
                'Objects': [{'Key': key} for key in keys_to_delete]
            }
        )

        # Verify all in Deleted list
        deleted_keys = [obj['Key'] for obj in delete_response['Deleted']]
        assert sorted(deleted_keys) == sorted(keys_to_delete)

        # Each deleted object should have Key field
        for deleted_obj in delete_response['Deleted']:
            assert 'Key' in deleted_obj

    finally:
        fixture.cleanup()


def test_delete_objects_quiet_mode(s3_client, config):
    """
    Test DeleteObjects with Quiet mode

    Quiet mode should not return Deleted list
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('delete-quiet')
        s3_client.create_bucket(bucket_name)

        # Create objects
        for key in ['obj1', 'obj2', 'obj3']:
            s3_client.put_object(bucket_name, key, b'data')

        # Delete with Quiet=True
        delete_response = s3_client.client.delete_objects(
            Bucket=bucket_name,
            Delete={
                'Objects': [
                    {'Key': 'obj1'},
                    {'Key': 'obj2'},
                    {'Key': 'obj3'}
                ],
                'Quiet': True
            }
        )

        # In quiet mode, Deleted list should be empty or absent
        deleted_count = len(delete_response.get('Deleted', []))
        assert deleted_count == 0, \
            f"Expected empty Deleted list in quiet mode, got {deleted_count} items"

        # Verify objects were actually deleted
        remaining_objects = s3_client.list_objects(bucket_name)
        assert len(remaining_objects) == 0

    finally:
        fixture.cleanup()


def test_delete_objects_with_special_characters(s3_client, config):
    """
    Test DeleteObjects with special characters in keys

    Special characters should be handled correctly
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('delete-special')
        s3_client.create_bucket(bucket_name)

        special_keys = [
            'file with spaces.txt',
            'file-with-dashes.txt',
            'file_with_underscores.txt',
            'path/to/file.txt',
            'file@example.com'
        ]

        # Create objects
        for key in special_keys:
            s3_client.put_object(bucket_name, key, b'data')

        # Delete all with special chars
        delete_response = s3_client.client.delete_objects(
            Bucket=bucket_name,
            Delete={
                'Objects': [{'Key': key} for key in special_keys]
            }
        )

        # Verify all deleted
        assert len(delete_response['Deleted']) == len(special_keys)
        deleted_keys = {obj['Key'] for obj in delete_response['Deleted']}
        assert deleted_keys == set(special_keys)

    finally:
        fixture.cleanup()


def test_delete_objects_large_batch(s3_client, config):
    """
    Test DeleteObjects with large batch (100 objects)

    Should handle large batches correctly
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('delete-large')
        s3_client.create_bucket(bucket_name)

        # Create 100 objects
        num_objects = 100
        keys = [f'object-{i:03d}' for i in range(num_objects)]

        for key in keys:
            s3_client.put_object(bucket_name, key, b'data')

        # Delete all 100 objects
        delete_response = s3_client.client.delete_objects(
            Bucket=bucket_name,
            Delete={
                'Objects': [{'Key': key} for key in keys]
            }
        )

        # Verify all deleted
        assert len(delete_response['Deleted']) == num_objects

        # Verify bucket is empty
        remaining_objects = s3_client.list_objects(bucket_name)
        assert len(remaining_objects) == 0

    finally:
        fixture.cleanup()


def test_delete_objects_response_status(s3_client, config):
    """
    Test DeleteObjects HTTP response status

    Should return 200 OK
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('delete-status')
        s3_client.create_bucket(bucket_name)

        s3_client.put_object(bucket_name, 'test-obj', b'data')

        delete_response = s3_client.client.delete_objects(
            Bucket=bucket_name,
            Delete={
                'Objects': [{'Key': 'test-obj'}]
            }
        )

        status_code = delete_response['ResponseMetadata']['HTTPStatusCode']
        assert status_code == 200, f"Expected 200, got {status_code}"

    finally:
        fixture.cleanup()
