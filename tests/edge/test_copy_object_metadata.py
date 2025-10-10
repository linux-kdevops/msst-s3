#!/usr/bin/env python3
"""
S3 CopyObject Metadata Tests

Tests metadata handling during object copy operations:
- Metadata preservation (default COPY behavior)
- Metadata replacement (REPLACE directive)
- Content headers (ContentType, ContentEncoding, ContentDisposition, etc.)
- Cache control headers
- Custom metadata
- Tagging during copy
- Checksum handling

Ported from versitygw integration tests.

Generated-by: Claude AI
Signed-off-by: Luis Chamberlain <mcgrof@kernel.org>
"""

import pytest
from datetime import datetime, timedelta, timezone
import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from tests.common.fixtures import TestFixture
from botocore.exceptions import ClientError


def test_copy_object_preserves_metadata(s3_client, config):
    """
    Test that CopyObject preserves all metadata by default (COPY directive)

    Verifies that all HTTP headers and custom metadata are copied
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('copy-metadata')
        s3_client.create_bucket(bucket_name)

        source_key = 'source-object'
        dest_key = 'dest-object'

        # Create source object with comprehensive metadata
        content_type = 'application/json'
        content_encoding = 'base64'
        content_disposition = 'attachment; filename="test.json"'
        content_language = 'en-US'
        cache_control = 'no-cache, no-store'
        expires = datetime.now(timezone.utc) + timedelta(hours=10)

        custom_metadata = {
            'foo': 'bar',
            'baz': 'quxx',
            'test-key': 'test-value'
        }

        data = fixture.generate_random_data(100)

        # Put source object with all metadata
        s3_client.client.put_object(
            Bucket=bucket_name,
            Key=source_key,
            Body=data,
            ContentType=content_type,
            ContentEncoding=content_encoding,
            ContentDisposition=content_disposition,
            ContentLanguage=content_language,
            CacheControl=cache_control,
            Expires=expires,
            Metadata=custom_metadata
        )

        # Copy object (default behavior should preserve metadata)
        copy_source = {'Bucket': bucket_name, 'Key': source_key}
        s3_client.client.copy_object(
            CopySource=copy_source,
            Bucket=bucket_name,
            Key=dest_key
        )

        # Verify destination object has all metadata
        head_response = s3_client.head_object(bucket_name, dest_key)

        assert head_response['ContentLength'] == 100, \
            f"Expected ContentLength 100, got {head_response['ContentLength']}"
        assert head_response['ContentType'] == content_type, \
            f"Expected ContentType {content_type}, got {head_response.get('ContentType')}"
        assert head_response.get('ContentEncoding') == content_encoding, \
            f"Expected ContentEncoding {content_encoding}, got {head_response.get('ContentEncoding')}"
        assert head_response.get('ContentDisposition') == content_disposition, \
            f"Expected ContentDisposition {content_disposition}, got {head_response.get('ContentDisposition')}"
        assert head_response.get('ContentLanguage') == content_language, \
            f"Expected ContentLanguage {content_language}, got {head_response.get('ContentLanguage')}"
        assert head_response.get('CacheControl') == cache_control, \
            f"Expected CacheControl {cache_control}, got {head_response.get('CacheControl')}"

        # Verify custom metadata
        dest_metadata = head_response.get('Metadata', {})
        assert dest_metadata == custom_metadata, \
            f"Expected metadata {custom_metadata}, got {dest_metadata}"

    finally:
        fixture.cleanup()


def test_copy_object_replace_metadata(s3_client, config):
    """
    Test that CopyObject can replace metadata with REPLACE directive

    Verifies that new metadata completely replaces old metadata
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('copy-replace')
        s3_client.create_bucket(bucket_name)

        source_key = 'source-object'
        dest_key = 'dest-object'

        # Create source object with metadata
        old_metadata = {'old-key': 'old-value'}
        old_content_type = 'application/json'

        data = fixture.generate_random_data(100)

        s3_client.client.put_object(
            Bucket=bucket_name,
            Key=source_key,
            Body=data,
            ContentType=old_content_type,
            Metadata=old_metadata
        )

        # Copy object with REPLACE directive and new metadata
        new_metadata = {'new-key': 'new-value'}
        new_content_type = 'text/plain'

        copy_source = {'Bucket': bucket_name, 'Key': source_key}
        s3_client.client.copy_object(
            CopySource=copy_source,
            Bucket=bucket_name,
            Key=dest_key,
            MetadataDirective='REPLACE',
            ContentType=new_content_type,
            Metadata=new_metadata
        )

        # Verify destination has new metadata
        head_response = s3_client.head_object(bucket_name, dest_key)

        assert head_response['ContentType'] == new_content_type, \
            f"Expected ContentType {new_content_type}, got {head_response.get('ContentType')}"

        dest_metadata = head_response.get('Metadata', {})
        assert dest_metadata == new_metadata, \
            f"Expected metadata {new_metadata}, got {dest_metadata}"
        assert 'old-key' not in dest_metadata, \
            "Old metadata should not be present after REPLACE"

    finally:
        fixture.cleanup()


def test_copy_object_to_itself_with_new_metadata(s3_client, config):
    """
    Test copying an object to itself with metadata replacement

    This is a valid operation for updating metadata without changing the object
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('copy-self')
        s3_client.create_bucket(bucket_name)

        key = 'test-object'

        # Create object with initial metadata
        old_metadata = {'version': '1'}
        data = fixture.generate_random_data(100)

        s3_client.client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=data,
            Metadata=old_metadata
        )

        # Copy to itself with new metadata
        new_metadata = {'version': '2', 'updated': 'true'}

        copy_source = {'Bucket': bucket_name, 'Key': key}
        s3_client.client.copy_object(
            CopySource=copy_source,
            Bucket=bucket_name,
            Key=key,
            MetadataDirective='REPLACE',
            Metadata=new_metadata
        )

        # Verify metadata was updated
        head_response = s3_client.head_object(bucket_name, key)
        dest_metadata = head_response.get('Metadata', {})

        assert dest_metadata == new_metadata, \
            f"Expected metadata {new_metadata}, got {dest_metadata}"

    finally:
        fixture.cleanup()


def test_copy_object_preserves_content_headers(s3_client, config):
    """
    Test that all HTTP content headers are preserved during copy

    Tests: ContentType, ContentEncoding, ContentLanguage, ContentDisposition
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('copy-headers')
        s3_client.create_bucket(bucket_name)

        source_key = 'source-with-headers'
        dest_key = 'dest-with-headers'

        # Create object with all content headers
        data = fixture.generate_random_data(256)

        s3_client.client.put_object(
            Bucket=bucket_name,
            Key=source_key,
            Body=data,
            ContentType='application/pdf',
            ContentEncoding='gzip',
            ContentLanguage='en',
            ContentDisposition='inline; filename="document.pdf"'
        )

        # Copy object
        copy_source = {'Bucket': bucket_name, 'Key': source_key}
        s3_client.client.copy_object(
            CopySource=copy_source,
            Bucket=bucket_name,
            Key=dest_key
        )

        # Verify all headers preserved
        head_response = s3_client.head_object(bucket_name, dest_key)

        assert head_response['ContentType'] == 'application/pdf'
        assert head_response.get('ContentEncoding') == 'gzip'
        assert head_response.get('ContentLanguage') == 'en'
        assert head_response.get('ContentDisposition') == 'inline; filename="document.pdf"'

    finally:
        fixture.cleanup()


def test_copy_object_with_tagging(s3_client, config):
    """
    Test tagging behavior during object copy

    Tests both COPY and REPLACE tagging directives
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('copy-tagging')
        s3_client.create_bucket(bucket_name)

        source_key = 'source-tagged'
        dest_key_copy = 'dest-copy-tags'
        dest_key_replace = 'dest-replace-tags'

        data = fixture.generate_random_data(100)

        # Create source object with tags
        s3_client.client.put_object(
            Bucket=bucket_name,
            Key=source_key,
            Body=data,
            Tagging='source=true&version=1'
        )

        # Test 1: Copy with default tagging behavior (should copy tags)
        copy_source = {'Bucket': bucket_name, 'Key': source_key}
        s3_client.client.copy_object(
            CopySource=copy_source,
            Bucket=bucket_name,
            Key=dest_key_copy
        )

        # Verify tags were copied
        try:
            tag_response = s3_client.client.get_object_tagging(
                Bucket=bucket_name,
                Key=dest_key_copy
            )
            tags = {tag['Key']: tag['Value'] for tag in tag_response.get('TagSet', [])}
            assert 'source' in tags
            assert tags['source'] == 'true'
        except ClientError as e:
            if e.response['Error']['Code'] != 'NotImplemented':
                raise

        # Test 2: Copy with REPLACE tagging directive
        s3_client.client.copy_object(
            CopySource=copy_source,
            Bucket=bucket_name,
            Key=dest_key_replace,
            TaggingDirective='REPLACE',
            Tagging='copied=true&version=2'
        )

        # Verify new tags
        try:
            tag_response = s3_client.client.get_object_tagging(
                Bucket=bucket_name,
                Key=dest_key_replace
            )
            tags = {tag['Key']: tag['Value'] for tag in tag_response.get('TagSet', [])}
            assert 'copied' in tags
            assert tags['copied'] == 'true'
            assert 'source' not in tags  # Old tags should be replaced
        except ClientError as e:
            if e.response['Error']['Code'] != 'NotImplemented':
                raise

    finally:
        fixture.cleanup()


def test_copy_object_with_checksum(s3_client, config):
    """
    Test checksum handling during object copy

    Verifies that checksums are preserved when copying objects
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('copy-checksum')
        s3_client.create_bucket(bucket_name)

        source_key = 'source-checksummed'
        dest_key = 'dest-checksummed'

        data = fixture.generate_random_data(512)

        # Create source with checksum
        try:
            put_response = s3_client.client.put_object(
                Bucket=bucket_name,
                Key=source_key,
                Body=data,
                ChecksumAlgorithm='CRC32'
            )

            source_checksum = put_response.get('ChecksumCRC32')

            # Copy object
            copy_source = {'Bucket': bucket_name, 'Key': source_key}
            s3_client.client.copy_object(
                CopySource=copy_source,
                Bucket=bucket_name,
                Key=dest_key
            )

            # Verify checksum is preserved (use GetObjectAttributes if available)
            try:
                attrs_response = s3_client.client.get_object_attributes(
                    Bucket=bucket_name,
                    Key=dest_key,
                    ObjectAttributes=['Checksum']
                )

                if 'Checksum' in attrs_response:
                    dest_checksum = attrs_response['Checksum'].get('ChecksumCRC32')
                    if source_checksum and dest_checksum:
                        assert dest_checksum == source_checksum, \
                            "Checksum should be preserved during copy"

            except ClientError as e:
                if e.response['Error']['Code'] != 'NotImplemented':
                    raise

        except ClientError as e:
            if e.response['Error']['Code'] in ['NotImplemented', 'InvalidArgument']:
                pytest.skip("Checksum functionality not supported")
            raise

    finally:
        fixture.cleanup()


def test_copy_object_cross_bucket(s3_client, config):
    """
    Test copying object between different buckets

    Verifies that metadata is preserved in cross-bucket copies
    """
    fixture = TestFixture(s3_client, config)

    try:
        source_bucket = fixture.generate_bucket_name('copy-src')
        dest_bucket = fixture.generate_bucket_name('copy-dst')

        s3_client.create_bucket(source_bucket)
        s3_client.create_bucket(dest_bucket)

        key = 'test-object'
        metadata = {'origin': 'source-bucket'}

        data = fixture.generate_random_data(100)

        # Create object in source bucket
        s3_client.client.put_object(
            Bucket=source_bucket,
            Key=key,
            Body=data,
            ContentType='text/plain',
            Metadata=metadata
        )

        # Copy to destination bucket
        copy_source = {'Bucket': source_bucket, 'Key': key}
        s3_client.client.copy_object(
            CopySource=copy_source,
            Bucket=dest_bucket,
            Key=key
        )

        # Verify metadata preserved in destination bucket
        head_response = s3_client.head_object(dest_bucket, key)

        assert head_response['ContentType'] == 'text/plain'
        dest_metadata = head_response.get('Metadata', {})
        assert dest_metadata == metadata

    finally:
        fixture.cleanup()


def test_copy_object_with_cache_control(s3_client, config):
    """
    Test Cache-Control and Expires headers during copy

    Verifies time-based cache headers are preserved
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('copy-cache')
        s3_client.create_bucket(bucket_name)

        source_key = 'source-cached'
        dest_key = 'dest-cached'

        data = fixture.generate_random_data(100)
        cache_control = 'max-age=3600, public'
        expires = datetime.now(timezone.utc) + timedelta(hours=24)

        # Create source with cache headers
        s3_client.client.put_object(
            Bucket=bucket_name,
            Key=source_key,
            Body=data,
            CacheControl=cache_control,
            Expires=expires
        )

        # Copy object
        copy_source = {'Bucket': bucket_name, 'Key': source_key}
        s3_client.client.copy_object(
            CopySource=copy_source,
            Bucket=bucket_name,
            Key=dest_key
        )

        # Verify cache headers preserved
        head_response = s3_client.head_object(bucket_name, dest_key)

        assert head_response.get('CacheControl') == cache_control, \
            f"Expected CacheControl {cache_control}, got {head_response.get('CacheControl')}"

        # Expires header may be formatted differently, so just check it exists
        assert 'Expires' in head_response or 'expires' in head_response

    finally:
        fixture.cleanup()
