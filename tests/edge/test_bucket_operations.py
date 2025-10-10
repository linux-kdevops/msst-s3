#!/usr/bin/env python3
"""
S3 Bucket Operations Tests

Tests bucket-level operations:
- CreateBucket
- DeleteBucket
- HeadBucket
- ListBuckets
- GetBucketLocation

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


def test_create_bucket_success(s3_client, config):
    """
    Test basic CreateBucket operation

    Bucket should be created and accessible
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('create-success')
        s3_client.create_bucket(bucket_name)

        # Verify bucket exists via HeadBucket
        head_response = s3_client.client.head_bucket(Bucket=bucket_name)
        assert head_response['ResponseMetadata']['HTTPStatusCode'] in [200, 204]

    finally:
        fixture.cleanup()


def test_create_bucket_already_exists(s3_client, config):
    """
    Test CreateBucket on existing bucket

    Should return BucketAlreadyExists or BucketAlreadyOwnedByYou
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('create-exists')
        s3_client.create_bucket(bucket_name)

        # Try to create same bucket again
        with pytest.raises(ClientError) as exc_info:
            s3_client.create_bucket(bucket_name)

        error_code = exc_info.value.response['Error']['Code']
        assert error_code in ['BucketAlreadyExists', 'BucketAlreadyOwnedByYou'], \
            f"Expected BucketAlreadyExists or BucketAlreadyOwnedByYou, got {error_code}"

    finally:
        fixture.cleanup()


def test_head_bucket_success(s3_client, config):
    """
    Test HeadBucket on existing bucket

    Should return success
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('head-success')
        s3_client.create_bucket(bucket_name)

        # HeadBucket should succeed
        head_response = s3_client.client.head_bucket(Bucket=bucket_name)

        assert 'ResponseMetadata' in head_response
        assert head_response['ResponseMetadata']['HTTPStatusCode'] in [200, 204]

    finally:
        fixture.cleanup()


def test_head_bucket_non_existing(s3_client, config):
    """
    Test HeadBucket on non-existing bucket

    Should return NotFound (404)
    """
    fixture = TestFixture(s3_client, config)

    try:
        with pytest.raises(ClientError) as exc_info:
            s3_client.client.head_bucket(Bucket='non-existing-bucket-12345')

        error_code = exc_info.value.response['Error']['Code']
        assert error_code in ['404', 'NotFound'], \
            f"Expected NotFound, got {error_code}"

    finally:
        fixture.cleanup()


def test_delete_bucket_success(s3_client, config):
    """
    Test DeleteBucket operation

    Bucket should be deleted and no longer accessible
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('delete-success')
        s3_client.create_bucket(bucket_name)

        # Delete bucket
        s3_client.delete_bucket(bucket_name)

        # Verify bucket is gone
        with pytest.raises(ClientError) as exc_info:
            s3_client.client.head_bucket(Bucket=bucket_name)

        error_code = exc_info.value.response['Error']['Code']
        assert error_code in ['404', 'NotFound']

    finally:
        # Bucket already deleted, cleanup should handle gracefully
        try:
            fixture.cleanup()
        except:
            pass


def test_delete_bucket_non_existing(s3_client, config):
    """
    Test DeleteBucket on non-existing bucket

    Should return NoSuchBucket error
    """
    fixture = TestFixture(s3_client, config)

    try:
        with pytest.raises(ClientError) as exc_info:
            s3_client.client.delete_bucket(Bucket='non-existing-bucket-12345')

        error_code = exc_info.value.response['Error']['Code']
        assert error_code == 'NoSuchBucket', \
            f"Expected NoSuchBucket, got {error_code}"

    finally:
        fixture.cleanup()


def test_delete_bucket_not_empty(s3_client, config):
    """
    Test DeleteBucket on bucket with objects

    Should return BucketNotEmpty error
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('delete-notempty')
        s3_client.create_bucket(bucket_name)

        # Put an object in bucket
        s3_client.put_object(bucket_name, 'test-object', b'data')

        # Try to delete non-empty bucket
        with pytest.raises(ClientError) as exc_info:
            s3_client.delete_bucket(bucket_name)

        error_code = exc_info.value.response['Error']['Code']
        assert error_code == 'BucketNotEmpty', \
            f"Expected BucketNotEmpty, got {error_code}"

    finally:
        fixture.cleanup()


def test_list_buckets_success(s3_client, config):
    """
    Test ListBuckets operation

    Should return list of buckets
    """
    fixture = TestFixture(s3_client, config)

    try:
        # Create a few buckets
        bucket1 = fixture.generate_bucket_name('list-1')
        bucket2 = fixture.generate_bucket_name('list-2')
        s3_client.create_bucket(bucket1)
        s3_client.create_bucket(bucket2)

        # List buckets
        list_response = s3_client.client.list_buckets()

        assert 'Buckets' in list_response
        bucket_names = [b['Name'] for b in list_response['Buckets']]

        assert bucket1 in bucket_names
        assert bucket2 in bucket_names

    finally:
        fixture.cleanup()


def test_list_buckets_empty(s3_client, config):
    """
    Test ListBuckets when user has no buckets

    Should return empty list (or buckets from other tests)
    """
    fixture = TestFixture(s3_client, config)

    try:
        # Just list buckets - may or may not be empty depending on environment
        list_response = s3_client.client.list_buckets()

        assert 'Buckets' in list_response
        assert isinstance(list_response['Buckets'], list)

    finally:
        fixture.cleanup()


def test_get_bucket_location_success(s3_client, config):
    """
    Test GetBucketLocation operation

    Should return bucket region
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('location')
        s3_client.create_bucket(bucket_name)

        # Get bucket location
        location_response = s3_client.client.get_bucket_location(
            Bucket=bucket_name
        )

        # LocationConstraint may be None for us-east-1
        assert 'LocationConstraint' in location_response

    finally:
        fixture.cleanup()


def test_get_bucket_location_non_existing(s3_client, config):
    """
    Test GetBucketLocation on non-existing bucket

    Should return NoSuchBucket error
    """
    fixture = TestFixture(s3_client, config)

    try:
        with pytest.raises(ClientError) as exc_info:
            s3_client.client.get_bucket_location(
                Bucket='non-existing-bucket-12345'
            )

        error_code = exc_info.value.response['Error']['Code']
        assert error_code == 'NoSuchBucket', \
            f"Expected NoSuchBucket, got {error_code}"

    finally:
        fixture.cleanup()


def test_create_delete_bucket_lifecycle(s3_client, config):
    """
    Test complete bucket lifecycle (create, use, delete)

    Verifies bucket can be created, used, and deleted
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('lifecycle')

        # Create bucket
        s3_client.create_bucket(bucket_name)

        # Use bucket
        s3_client.put_object(bucket_name, 'test-object', b'test data')
        get_response = s3_client.get_object(bucket_name, 'test-object')
        assert get_response['Body'].read() == b'test data'

        # Delete object
        s3_client.delete_object(bucket_name, 'test-object')

        # Delete bucket
        s3_client.delete_bucket(bucket_name)

        # Verify bucket is gone
        with pytest.raises(ClientError) as exc_info:
            s3_client.client.head_bucket(Bucket=bucket_name)

        error_code = exc_info.value.response['Error']['Code']
        assert error_code in ['404', 'NotFound']

    finally:
        # Bucket already deleted
        try:
            fixture.cleanup()
        except:
            pass


def test_bucket_operations_case_sensitivity(s3_client, config):
    """
    Test bucket name case sensitivity

    Bucket names are case-sensitive
    """
    fixture = TestFixture(s3_client, config)

    try:
        # Create bucket with specific name
        bucket_name = fixture.generate_bucket_name('case-test')
        s3_client.create_bucket(bucket_name)

        # Try to access with different case should fail
        wrong_case = bucket_name.upper()
        if wrong_case != bucket_name:  # Only test if actually different
            with pytest.raises(ClientError) as exc_info:
                s3_client.client.head_bucket(Bucket=wrong_case)

            error_code = exc_info.value.response['Error']['Code']
            # MinIO returns 400 for invalid bucket name format
            assert error_code in ['404', 'NotFound', '400', 'InvalidBucketName']

    finally:
        fixture.cleanup()
