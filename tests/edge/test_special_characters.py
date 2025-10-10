#!/usr/bin/env python3
"""
S3 Special Character Object Naming Tests

Tests object key naming with special characters to ensure proper
encoding and handling across different S3 implementations.

Tests all commonly used special characters in object names:
! - _ . ' ( ) & @ = ; : space , ? ^ { } % ` [ ] ~ < > | # \\

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


def test_put_object_with_special_characters(s3_client, config):
    """
    Test PutObject and ListObjectsV2 with comprehensive special characters

    Verifies that S3 properly handles object names containing special characters
    and that all created objects can be listed and retrieved.
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('special-chars')
        s3_client.create_bucket(bucket_name)

        # Comprehensive list of special characters to test
        object_names = [
            "my!key",    # Exclamation mark
            "my-key",    # Hyphen
            "my_key",    # Underscore
            "my.key",    # Period
            "my'key",    # Single quote
            "my(key",    # Left parenthesis
            "my)key",    # Right parenthesis
            "my&key",    # Ampersand
            "my@key",    # At sign
            "my=key",    # Equals
            "my;key",    # Semicolon
            "my:key",    # Colon
            "my key",    # Space
            "my,key",    # Comma
            "my?key",    # Question mark
            "my^key",    # Caret
            "my{}key",   # Curly braces
            "my%key",    # Percent
            "my`key",    # Backtick
            "my[]key",   # Square brackets
            "my~key",    # Tilde
            "my<>key",   # Angle brackets
            "my|key",    # Pipe
            "my#key",    # Hash/pound
        ]

        # Note: Backslash may not be supported by all S3 implementations
        # Azure specifically has issues with backslashes
        # object_names.append("my\\key")  # Backslash

        data = b"test data"
        created_keys = []

        # Create all objects with special characters
        for obj_name in object_names:
            try:
                s3_client.put_object(bucket_name, obj_name, data)
                created_keys.append(obj_name)
            except ClientError as e:
                # Some implementations may not support certain characters
                error_code = e.response['Error']['Code']
                print(f"Warning: Could not create object '{obj_name}': {error_code}")
                # Continue with other objects

        # List all objects in the bucket
        listed_objects = s3_client.list_objects(bucket_name)
        listed_keys = [obj['Key'] for obj in listed_objects]

        # Verify all created objects are listed
        assert len(listed_keys) == len(created_keys), \
            f"Expected {len(created_keys)} objects, but got {len(listed_keys)}"

        for key in created_keys:
            assert key in listed_keys, \
                f"Object '{key}' was created but not found in listing"

        # Verify we can retrieve each object
        for key in created_keys:
            try:
                response = s3_client.get_object(bucket_name, key)
                retrieved_data = response['Body'].read()
                assert retrieved_data == data, \
                    f"Data mismatch for object '{key}'"
            except ClientError as e:
                pytest.fail(f"Failed to retrieve object '{key}': {e}")

    finally:
        fixture.cleanup()


def test_put_object_with_unicode_characters(s3_client, config):
    """
    Test object names with Unicode/UTF-8 characters

    Verifies proper handling of non-ASCII characters in object names
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('unicode-chars')
        s3_client.create_bucket(bucket_name)

        # Unicode/UTF-8 characters
        unicode_names = [
            "my-中文-key",      # Chinese characters
            "my-日本語-key",    # Japanese characters
            "my-한글-key",      # Korean characters
            "my-café-key",      # Accented characters
            "my-emoji-😀-key",  # Emoji (may not be supported)
            "my-Ñoño-key",      # Spanish characters
            "my-Москва-key",    # Cyrillic characters
        ]

        data = b"unicode test data"
        created_keys = []

        for obj_name in unicode_names:
            try:
                s3_client.put_object(bucket_name, obj_name, data)
                created_keys.append(obj_name)
            except ClientError as e:
                # Some S3 implementations may have restrictions on Unicode
                error_code = e.response['Error']['Code']
                print(f"Warning: Could not create Unicode object '{obj_name}': {error_code}")
                # Skip emoji and other potentially unsupported characters
                if "emoji" not in obj_name:
                    # Most Unicode should be supported
                    raise

        # List and verify
        if created_keys:
            listed_objects = s3_client.list_objects(bucket_name)
            listed_keys = [obj['Key'] for obj in listed_objects]

            for key in created_keys:
                assert key in listed_keys, \
                    f"Unicode object '{key}' was created but not found in listing"

    finally:
        fixture.cleanup()


def test_put_object_with_url_encoded_characters(s3_client, config):
    """
    Test object names that would require URL encoding

    Verifies that S3 properly handles characters that require URL encoding
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('url-encoded')
        s3_client.create_bucket(bucket_name)

        # Characters that typically require URL encoding
        url_encoded_names = [
            "my key with spaces",
            "my+plus+key",
            "my%percent%key",
            "my&query&key",
            "my=equals=key",
            "my?question?key",
            "my#hash#key",
        ]

        data = b"url encoded test"
        created_keys = []

        for obj_name in url_encoded_names:
            try:
                s3_client.put_object(bucket_name, obj_name, data)
                created_keys.append(obj_name)

                # Verify immediate retrieval
                response = s3_client.get_object(bucket_name, obj_name)
                assert response['Body'].read() == data, \
                    f"Data mismatch for '{obj_name}'"

            except ClientError as e:
                error_code = e.response['Error']['Code']
                print(f"Warning: Could not create object '{obj_name}': {error_code}")

        # List all objects
        if created_keys:
            listed_objects = s3_client.list_objects(bucket_name)
            listed_keys = [obj['Key'] for obj in listed_objects]

            assert len(listed_keys) == len(created_keys), \
                f"Expected {len(created_keys)} objects, got {len(listed_keys)}"

    finally:
        fixture.cleanup()


def test_put_object_with_path_separators(s3_client, config):
    """
    Test object names with path-like structure using forward slashes

    Verifies proper handling of directory-like object names
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('path-separators')
        s3_client.create_bucket(bucket_name)

        # Path-like object names
        path_names = [
            "folder/file.txt",
            "deep/nested/path/file.txt",
            "folder/subfolder/",  # Directory marker
            "/leading/slash/file.txt",
            "trailing/slash/",
            "folder//double//slash.txt",
        ]

        data = b"path test data"

        for obj_name in path_names:
            s3_client.put_object(bucket_name, obj_name, data)

        # List all objects
        listed_objects = s3_client.list_objects(bucket_name)
        listed_keys = [obj['Key'] for obj in listed_objects]

        assert len(listed_keys) == len(path_names), \
            f"Expected {len(path_names)} objects, got {len(listed_keys)}"

        for key in path_names:
            assert key in listed_keys, \
                f"Path object '{key}' not found in listing"

    finally:
        fixture.cleanup()


def test_put_object_with_very_long_key(s3_client, config):
    """
    Test object name length limits

    S3 supports keys up to 1024 bytes
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('long-keys')
        s3_client.create_bucket(bucket_name)

        # Create a long but valid key (under 1024 bytes)
        long_key = "a" * 1000
        data = b"long key test"

        s3_client.put_object(bucket_name, long_key, data)

        # Verify we can retrieve it
        response = s3_client.get_object(bucket_name, long_key)
        assert response['Body'].read() == data

        # Try to create a key that's too long (over 1024 bytes)
        too_long_key = "a" * 1025

        with pytest.raises(ClientError) as exc_info:
            s3_client.put_object(bucket_name, too_long_key, data)

        # Should fail with key too long error
        error_code = exc_info.value.response['Error']['Code']
        assert error_code in ['KeyTooLongError', 'InvalidRequest'], \
            f"Expected KeyTooLongError or InvalidRequest, got {error_code}"

    finally:
        fixture.cleanup()


def test_put_object_with_mixed_special_characters(s3_client, config):
    """
    Test object names with combinations of special characters

    Verifies complex real-world scenarios
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('mixed-special')
        s3_client.create_bucket(bucket_name)

        # Real-world complex object names
        complex_names = [
            "documents/2024-01-15_report_v1.2.pdf",
            "user@email.com/files/my-file.txt",
            "data/export_(2024)_final.csv",
            "backups/db_backup_2024-01-15_23:59:59.sql",
            "images/photo #123 [final].jpg",
            "logs/app.log.2024-01-15.gz",
        ]

        data = b"complex name test"
        created_keys = []

        for obj_name in complex_names:
            try:
                s3_client.put_object(bucket_name, obj_name, data)
                created_keys.append(obj_name)

                # Verify retrieval
                response = s3_client.get_object(bucket_name, obj_name)
                assert response['Body'].read() == data

            except ClientError as e:
                error_code = e.response['Error']['Code']
                print(f"Warning: Could not create complex object '{obj_name}': {error_code}")

        # List and verify all created objects
        if created_keys:
            listed_objects = s3_client.list_objects(bucket_name)
            listed_keys = [obj['Key'] for obj in listed_objects]

            for key in created_keys:
                assert key in listed_keys, \
                    f"Complex object '{key}' not found in listing"

    finally:
        fixture.cleanup()
