#!/usr/bin/env python3
"""
S3 Object Tagging Tests

Tests object tagging operations:
- PutObjectTagging
- GetObjectTagging
- DeleteObjectTagging
- Tag validation and limits

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


def test_put_object_tagging_success(s3_client, config):
    """
    Test PutObjectTagging basic operation

    Tags should be set and retrievable
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('tag-put-success')
        s3_client.create_bucket(bucket_name)

        key = 'tagged-object'
        data = fixture.generate_random_data(100)
        s3_client.put_object(bucket_name, key, data)

        # Put tags
        tagging = {
            'TagSet': [
                {'Key': 'key1', 'Value': 'value1'},
                {'Key': 'key2', 'Value': 'value2'},
            ]
        }

        s3_client.client.put_object_tagging(
            Bucket=bucket_name,
            Key=key,
            Tagging=tagging
        )

        # Verify tags were set
        tag_response = s3_client.client.get_object_tagging(
            Bucket=bucket_name,
            Key=key
        )

        tags = {tag['Key']: tag['Value'] for tag in tag_response['TagSet']}
        assert 'key1' in tags
        assert tags['key1'] == 'value1'
        assert 'key2' in tags
        assert tags['key2'] == 'value2'

    finally:
        fixture.cleanup()


def test_put_object_tagging_non_existing_object(s3_client, config):
    """
    Test PutObjectTagging on non-existing object

    Should return NoSuchKey error
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('tag-put-noobj')
        s3_client.create_bucket(bucket_name)

        tagging = {
            'TagSet': [
                {'Key': 'key1', 'Value': 'value1'},
            ]
        }

        with pytest.raises(ClientError) as exc_info:
            s3_client.client.put_object_tagging(
                Bucket=bucket_name,
                Key='non-existing-object',
                Tagging=tagging
            )

        error_code = exc_info.value.response['Error']['Code']
        assert error_code == 'NoSuchKey', \
            f"Expected NoSuchKey, got {error_code}"

    finally:
        fixture.cleanup()


def test_put_object_tagging_replaces_existing(s3_client, config):
    """
    Test PutObjectTagging replaces existing tags

    New tags should completely replace old tags
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('tag-replace')
        s3_client.create_bucket(bucket_name)

        key = 'tagged-object'
        data = fixture.generate_random_data(100)
        s3_client.put_object(bucket_name, key, data)

        # Set initial tags
        tagging1 = {
            'TagSet': [
                {'Key': 'old-key', 'Value': 'old-value'},
            ]
        }

        s3_client.client.put_object_tagging(
            Bucket=bucket_name,
            Key=key,
            Tagging=tagging1
        )

        # Replace with new tags
        tagging2 = {
            'TagSet': [
                {'Key': 'new-key', 'Value': 'new-value'},
            ]
        }

        s3_client.client.put_object_tagging(
            Bucket=bucket_name,
            Key=key,
            Tagging=tagging2
        )

        # Verify old tags are gone
        tag_response = s3_client.client.get_object_tagging(
            Bucket=bucket_name,
            Key=key
        )

        tags = {tag['Key']: tag['Value'] for tag in tag_response['TagSet']}
        assert 'old-key' not in tags
        assert 'new-key' in tags
        assert tags['new-key'] == 'new-value'

    finally:
        fixture.cleanup()


def test_get_object_tagging_non_existing_object(s3_client, config):
    """
    Test GetObjectTagging on non-existing object

    Should return NoSuchKey error
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('tag-get-noobj')
        s3_client.create_bucket(bucket_name)

        with pytest.raises(ClientError) as exc_info:
            s3_client.client.get_object_tagging(
                Bucket=bucket_name,
                Key='non-existing-object'
            )

        error_code = exc_info.value.response['Error']['Code']
        assert error_code == 'NoSuchKey', \
            f"Expected NoSuchKey, got {error_code}"

    finally:
        fixture.cleanup()


def test_get_object_tagging_unset_tags(s3_client, config):
    """
    Test GetObjectTagging on object without tags

    Should return empty TagSet or NotFound
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('tag-get-unset')
        s3_client.create_bucket(bucket_name)

        key = 'untagged-object'
        data = fixture.generate_random_data(100)
        s3_client.put_object(bucket_name, key, data)

        # Get tags on object without tags
        try:
            tag_response = s3_client.client.get_object_tagging(
                Bucket=bucket_name,
                Key=key
            )

            # Should return empty TagSet
            assert 'TagSet' in tag_response
            assert len(tag_response['TagSet']) == 0

        except ClientError as e:
            # Some implementations may return error for unset tags
            error_code = e.response['Error']['Code']
            assert error_code in ['NoSuchTagSet', 'NoSuchTagSetError'], \
                f"Expected NoSuchTagSet, got {error_code}"

    finally:
        fixture.cleanup()


def test_get_object_tagging_success(s3_client, config):
    """
    Test GetObjectTagging retrieves tags correctly

    Tags should match what was set
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('tag-get-success')
        s3_client.create_bucket(bucket_name)

        key = 'tagged-object'
        data = fixture.generate_random_data(100)
        s3_client.put_object(bucket_name, key, data)

        # Set tags
        expected_tags = {
            'environment': 'testing',
            'application': 's3-tests',
            'version': '1.0',
        }

        tagging = {
            'TagSet': [
                {'Key': k, 'Value': v} for k, v in expected_tags.items()
            ]
        }

        s3_client.client.put_object_tagging(
            Bucket=bucket_name,
            Key=key,
            Tagging=tagging
        )

        # Get and verify tags
        tag_response = s3_client.client.get_object_tagging(
            Bucket=bucket_name,
            Key=key
        )

        retrieved_tags = {tag['Key']: tag['Value'] for tag in tag_response['TagSet']}
        assert retrieved_tags == expected_tags

    finally:
        fixture.cleanup()


def test_delete_object_tagging_success(s3_client, config):
    """
    Test DeleteObjectTagging removes all tags

    After deletion, GetObjectTagging should return empty
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('tag-delete')
        s3_client.create_bucket(bucket_name)

        key = 'tagged-object'
        data = fixture.generate_random_data(100)
        s3_client.put_object(bucket_name, key, data)

        # Set tags
        tagging = {
            'TagSet': [
                {'Key': 'key1', 'Value': 'value1'},
                {'Key': 'key2', 'Value': 'value2'},
            ]
        }

        s3_client.client.put_object_tagging(
            Bucket=bucket_name,
            Key=key,
            Tagging=tagging
        )

        # Delete tags
        s3_client.client.delete_object_tagging(
            Bucket=bucket_name,
            Key=key
        )

        # Verify tags are gone
        try:
            tag_response = s3_client.client.get_object_tagging(
                Bucket=bucket_name,
                Key=key
            )

            # Should return empty TagSet
            assert len(tag_response.get('TagSet', [])) == 0

        except ClientError as e:
            # Some implementations may return error for deleted tags
            error_code = e.response['Error']['Code']
            assert error_code in ['NoSuchTagSet', 'NoSuchTagSetError']

    finally:
        fixture.cleanup()


def test_delete_object_tagging_non_existing_object(s3_client, config):
    """
    Test DeleteObjectTagging on non-existing object

    Should return NoSuchKey error
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('tag-delete-noobj')
        s3_client.create_bucket(bucket_name)

        with pytest.raises(ClientError) as exc_info:
            s3_client.client.delete_object_tagging(
                Bucket=bucket_name,
                Key='non-existing-object'
            )

        error_code = exc_info.value.response['Error']['Code']
        assert error_code == 'NoSuchKey', \
            f"Expected NoSuchKey, got {error_code}"

    finally:
        fixture.cleanup()


def test_object_tagging_with_special_characters(s3_client, config):
    """
    Test object tagging with special characters in tag values

    Special characters should be handled correctly
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('tag-special')
        s3_client.create_bucket(bucket_name)

        key = 'tagged-object'
        data = fixture.generate_random_data(100)
        s3_client.put_object(bucket_name, key, data)

        # Tags with special characters (S3 restricts certain chars)
        tagging = {
            'TagSet': [
                {'Key': 'path', 'Value': '/usr/local/bin'},
                {'Key': 'email', 'Value': 'test@example.com'},
                {'Key': 'description', 'Value': 'Test object with spaces and symbols'},
            ]
        }

        s3_client.client.put_object_tagging(
            Bucket=bucket_name,
            Key=key,
            Tagging=tagging
        )

        # Verify tags
        tag_response = s3_client.client.get_object_tagging(
            Bucket=bucket_name,
            Key=key
        )

        tags = {tag['Key']: tag['Value'] for tag in tag_response['TagSet']}
        assert tags['path'] == '/usr/local/bin'
        assert tags['email'] == 'test@example.com'
        assert tags['description'] == 'Test object with spaces and symbols'

    finally:
        fixture.cleanup()


def test_object_tagging_multiple_operations(s3_client, config):
    """
    Test multiple tagging operations on same object

    Verify tags can be added, updated, and deleted multiple times
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('tag-multiple')
        s3_client.create_bucket(bucket_name)

        key = 'tagged-object'
        data = fixture.generate_random_data(100)
        s3_client.put_object(bucket_name, key, data)

        # Operation 1: Set initial tags
        tagging1 = {
            'TagSet': [
                {'Key': 'stage', 'Value': 'dev'},
            ]
        }
        s3_client.client.put_object_tagging(
            Bucket=bucket_name,
            Key=key,
            Tagging=tagging1
        )

        # Operation 2: Update tags
        tagging2 = {
            'TagSet': [
                {'Key': 'stage', 'Value': 'prod'},
                {'Key': 'region', 'Value': 'us-east-1'},
            ]
        }
        s3_client.client.put_object_tagging(
            Bucket=bucket_name,
            Key=key,
            Tagging=tagging2
        )

        # Verify current state
        tag_response = s3_client.client.get_object_tagging(
            Bucket=bucket_name,
            Key=key
        )

        tags = {tag['Key']: tag['Value'] for tag in tag_response['TagSet']}
        assert tags['stage'] == 'prod'
        assert tags['region'] == 'us-east-1'

        # Operation 3: Delete all tags
        s3_client.client.delete_object_tagging(
            Bucket=bucket_name,
            Key=key
        )

        # Verify tags are gone
        try:
            tag_response = s3_client.client.get_object_tagging(
                Bucket=bucket_name,
                Key=key
            )
            assert len(tag_response.get('TagSet', [])) == 0
        except ClientError as e:
            assert e.response['Error']['Code'] in ['NoSuchTagSet', 'NoSuchTagSetError']

    finally:
        fixture.cleanup()
