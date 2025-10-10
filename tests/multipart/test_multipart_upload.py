#!/usr/bin/env python3
"""
S3 Multipart Upload Tests

Comprehensive multipart upload testing including:
- Complete multipart upload workflow
- Part size validation
- Upload initiation and completion
- Part listing
- Upload abortion

Ported from versitygw integration tests.

Generated-by: Claude AI
Signed-off-by: Luis Chamberlain <mcgrof@kernel.org>
"""

import pytest
import hashlib
import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from tests.common.fixtures import TestFixture
from botocore.exceptions import ClientError


def calculate_sha256(data: bytes) -> str:
    """Calculate SHA256 hash of data"""
    return hashlib.sha256(data).hexdigest()


def test_create_multipart_upload_success(s3_client, config):
    """
    Test basic multipart upload initiation

    Verifies that CreateMultipartUpload returns valid UploadId
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('mpu-create')
        s3_client.create_bucket(bucket_name)

        key = 'test-multipart-object'

        # Create multipart upload
        response = s3_client.client.create_multipart_upload(
            Bucket=bucket_name,
            Key=key
        )

        # Verify response
        assert 'UploadId' in response, "Expected UploadId in response"
        assert response['UploadId'], "Expected non-empty UploadId"
        assert response['Bucket'] == bucket_name, \
            f"Expected bucket {bucket_name}, got {response.get('Bucket')}"
        assert response['Key'] == key, \
            f"Expected key {key}, got {response.get('Key')}"

        # Cleanup: abort the upload
        s3_client.client.abort_multipart_upload(
            Bucket=bucket_name,
            Key=key,
            UploadId=response['UploadId']
        )

    finally:
        fixture.cleanup()


def test_upload_part_success(s3_client, config):
    """
    Test basic part upload

    Verifies that UploadPart returns ETag
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('mpu-part')
        s3_client.create_bucket(bucket_name)

        key = 'test-part-object'

        # Create multipart upload
        create_response = s3_client.client.create_multipart_upload(
            Bucket=bucket_name,
            Key=key
        )
        upload_id = create_response['UploadId']

        # Upload a part (must be at least 5MB except for last part)
        part_data = fixture.generate_random_data(5 * 1024 * 1024)
        part_response = s3_client.client.upload_part(
            Bucket=bucket_name,
            Key=key,
            UploadId=upload_id,
            PartNumber=1,
            Body=part_data
        )

        # Verify ETag is returned
        assert 'ETag' in part_response, "Expected ETag in response"
        assert part_response['ETag'], "Expected non-empty ETag"

        # Cleanup
        s3_client.client.abort_multipart_upload(
            Bucket=bucket_name,
            Key=key,
            UploadId=upload_id
        )

    finally:
        fixture.cleanup()


def test_complete_multipart_upload_success(s3_client, config):
    """
    Test complete multipart upload workflow

    Creates 25MB object split into 5 parts of 5MB each,
    validates data integrity using SHA256 checksum
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('mpu-complete')
        s3_client.create_bucket(bucket_name)

        key = 'test-complete-object'
        part_size = 5 * 1024 * 1024  # 5MB
        num_parts = 5
        total_size = part_size * num_parts

        # Generate complete data
        complete_data = fixture.generate_random_data(total_size)
        expected_checksum = calculate_sha256(complete_data)

        # Create multipart upload
        create_response = s3_client.client.create_multipart_upload(
            Bucket=bucket_name,
            Key=key
        )
        upload_id = create_response['UploadId']

        # Upload parts
        parts = []
        for part_num in range(1, num_parts + 1):
            start = (part_num - 1) * part_size
            end = start + part_size
            part_data = complete_data[start:end]

            part_response = s3_client.client.upload_part(
                Bucket=bucket_name,
                Key=key,
                UploadId=upload_id,
                PartNumber=part_num,
                Body=part_data
            )

            parts.append({
                'PartNumber': part_num,
                'ETag': part_response['ETag']
            })

        # Complete multipart upload
        complete_response = s3_client.client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={'Parts': parts}
        )

        # Verify completion response
        assert complete_response['Key'] == key, \
            f"Expected key {key}, got {complete_response.get('Key')}"
        assert 'ETag' in complete_response, "Expected ETag in response"

        # Verify object exists and has correct size
        head_response = s3_client.head_object(bucket_name, key)
        assert head_response['ContentLength'] == total_size, \
            f"Expected size {total_size}, got {head_response['ContentLength']}"
        assert head_response['ETag'] == complete_response['ETag'], \
            "ETags should match"

        # Verify data integrity
        get_response = s3_client.get_object(bucket_name, key)
        retrieved_data = get_response['Body'].read()

        assert len(retrieved_data) == total_size, \
            f"Expected {total_size} bytes, got {len(retrieved_data)}"

        actual_checksum = calculate_sha256(retrieved_data)
        assert actual_checksum == expected_checksum, \
            f"Data integrity check failed: {actual_checksum} != {expected_checksum}"

    finally:
        fixture.cleanup()


def test_complete_multipart_upload_small_parts_fails(s3_client, config):
    """
    Test that parts smaller than 5MB minimum fail

    AWS S3 requires minimum 5MB per part (except last part)
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('mpu-small')
        s3_client.create_bucket(bucket_name)

        key = 'test-small-parts'

        # Create multipart upload
        create_response = s3_client.client.create_multipart_upload(
            Bucket=bucket_name,
            Key=key
        )
        upload_id = create_response['UploadId']

        # Upload parts that are too small (256 bytes each)
        parts = []
        for part_num in range(1, 5):
            part_data = fixture.generate_random_data(256)

            part_response = s3_client.client.upload_part(
                Bucket=bucket_name,
                Key=key,
                UploadId=upload_id,
                PartNumber=part_num,
                Body=part_data
            )

            parts.append({
                'PartNumber': part_num,
                'ETag': part_response['ETag']
            })

        # Complete should fail with EntityTooSmall
        with pytest.raises(ClientError) as exc_info:
            s3_client.client.complete_multipart_upload(
                Bucket=bucket_name,
                Key=key,
                UploadId=upload_id,
                MultipartUpload={'Parts': parts}
            )

        error_code = exc_info.value.response['Error']['Code']
        assert error_code in ['EntityTooSmall', 'InvalidPart'], \
            f"Expected EntityTooSmall or InvalidPart, got {error_code}"

    finally:
        fixture.cleanup()


def test_abort_multipart_upload_success(s3_client, config):
    """
    Test multipart upload abortion

    Verifies upload is removed from listings after abort
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('mpu-abort')
        s3_client.create_bucket(bucket_name)

        key = 'test-abort-object'

        # Create multipart upload
        create_response = s3_client.client.create_multipart_upload(
            Bucket=bucket_name,
            Key=key
        )
        upload_id = create_response['UploadId']

        # Abort the upload
        s3_client.client.abort_multipart_upload(
            Bucket=bucket_name,
            Key=key,
            UploadId=upload_id
        )

        # Verify upload is no longer listed
        list_response = s3_client.client.list_multipart_uploads(
            Bucket=bucket_name
        )

        uploads = list_response.get('Uploads', [])
        assert len(uploads) == 0, \
            f"Expected 0 uploads after abort, got {len(uploads)}"

    finally:
        fixture.cleanup()


def test_list_parts_success(s3_client, config):
    """
    Test listing uploaded parts

    Verifies part metadata and storage class
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('mpu-list')
        s3_client.create_bucket(bucket_name)

        key = 'test-list-parts'
        part_size = 5 * 1024 * 1024  # 5MB
        num_parts = 5

        # Create multipart upload
        create_response = s3_client.client.create_multipart_upload(
            Bucket=bucket_name,
            Key=key
        )
        upload_id = create_response['UploadId']

        # Upload parts
        uploaded_parts = []
        for part_num in range(1, num_parts + 1):
            part_data = fixture.generate_random_data(part_size)

            part_response = s3_client.client.upload_part(
                Bucket=bucket_name,
                Key=key,
                UploadId=upload_id,
                PartNumber=part_num,
                Body=part_data
            )

            uploaded_parts.append({
                'PartNumber': part_num,
                'ETag': part_response['ETag'],
                'Size': part_size
            })

        # List parts
        list_response = s3_client.client.list_parts(
            Bucket=bucket_name,
            Key=key,
            UploadId=upload_id
        )

        # Verify storage class
        assert list_response['StorageClass'] == 'STANDARD', \
            f"Expected STANDARD storage class, got {list_response.get('StorageClass')}"

        # Verify parts
        listed_parts = list_response.get('Parts', [])
        assert len(listed_parts) == num_parts, \
            f"Expected {num_parts} parts, got {len(listed_parts)}"

        for i, listed_part in enumerate(listed_parts):
            expected = uploaded_parts[i]
            assert listed_part['PartNumber'] == expected['PartNumber'], \
                f"Part number mismatch at index {i}"
            assert listed_part['ETag'] == expected['ETag'], \
                f"ETag mismatch at index {i}"
            assert listed_part['Size'] == expected['Size'], \
                f"Size mismatch at index {i}"

        # Cleanup
        s3_client.client.abort_multipart_upload(
            Bucket=bucket_name,
            Key=key,
            UploadId=upload_id
        )

    finally:
        fixture.cleanup()


def test_list_multipart_uploads(s3_client, config):
    """
    Test listing in-progress multipart uploads

    Verifies multiple uploads can be listed
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('mpu-list-uploads')
        s3_client.create_bucket(bucket_name)

        # Create multiple uploads
        upload_ids = []
        for i in range(3):
            key = f'test-upload-{i}'
            create_response = s3_client.client.create_multipart_upload(
                Bucket=bucket_name,
                Key=key
            )
            upload_ids.append((key, create_response['UploadId']))

        # List uploads
        list_response = s3_client.client.list_multipart_uploads(
            Bucket=bucket_name
        )

        uploads = list_response.get('Uploads', [])
        assert len(uploads) == 3, \
            f"Expected 3 uploads, got {len(uploads)}"

        # Cleanup: abort all uploads
        for key, upload_id in upload_ids:
            s3_client.client.abort_multipart_upload(
                Bucket=bucket_name,
                Key=key,
                UploadId=upload_id
            )

    finally:
        fixture.cleanup()


def test_multipart_upload_with_metadata(s3_client, config):
    """
    Test multipart upload with object metadata

    Verifies metadata is preserved after completion
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('mpu-metadata')
        s3_client.create_bucket(bucket_name)

        key = 'test-metadata-object'
        metadata = {'test-key': 'test-value', 'author': 'test-suite'}

        # Create multipart upload with metadata
        create_response = s3_client.client.create_multipart_upload(
            Bucket=bucket_name,
            Key=key,
            Metadata=metadata,
            ContentType='application/octet-stream'
        )
        upload_id = create_response['UploadId']

        # Upload single part (can be smaller as it's the last part)
        part_data = fixture.generate_random_data(1024)
        part_response = s3_client.client.upload_part(
            Bucket=bucket_name,
            Key=key,
            UploadId=upload_id,
            PartNumber=1,
            Body=part_data
        )

        # Complete upload
        s3_client.client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={'Parts': [{'PartNumber': 1, 'ETag': part_response['ETag']}]}
        )

        # Verify metadata preserved
        head_response = s3_client.head_object(bucket_name, key)
        assert head_response.get('Metadata') == metadata, \
            f"Expected metadata {metadata}, got {head_response.get('Metadata')}"
        assert head_response.get('ContentType') == 'application/octet-stream'

    finally:
        fixture.cleanup()
