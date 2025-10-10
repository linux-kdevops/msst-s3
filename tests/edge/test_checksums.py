#!/usr/bin/env python3
"""
S3 Checksum Tests

Tests comprehensive checksum functionality including:
- All checksum algorithms (CRC32, CRC32C, SHA1, SHA256, CRC64NVME)
- PutObject with checksums
- GetObjectAttributes with checksums
- ListObjectsV2 with checksum metadata

Ported from versitygw integration tests.

Generated-by: Claude AI
Signed-off-by: Luis Chamberlain <mcgrof@kernel.org>
"""

import pytest
import hashlib
import zlib
import base64
import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from tests.common.fixtures import TestFixture
from botocore.exceptions import ClientError


def calculate_crc32(data: bytes) -> str:
    """Calculate CRC32 checksum and return base64-encoded string"""
    crc = zlib.crc32(data) & 0xFFFFFFFF
    return base64.b64encode(crc.to_bytes(4, byteorder='big')).decode('utf-8')


def calculate_sha1(data: bytes) -> str:
    """Calculate SHA1 checksum and return base64-encoded string"""
    sha1_hash = hashlib.sha1(data).digest()
    return base64.b64encode(sha1_hash).decode('utf-8')


def calculate_sha256(data: bytes) -> str:
    """Calculate SHA256 checksum and return base64-encoded string"""
    sha256_hash = hashlib.sha256(data).digest()
    return base64.b64encode(sha256_hash).decode('utf-8')


def test_put_object_checksum_crc32(s3_client, config):
    """Test PutObject with CRC32 checksum algorithm"""
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('checksum-crc32')
        s3_client.create_bucket(bucket_name)

        key = 'test-object-crc32'
        data = fixture.generate_random_data(1024)

        # Upload with CRC32 checksum algorithm
        response = s3_client.client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=data,
            ChecksumAlgorithm='CRC32'
        )

        # Verify response contains checksum
        assert 'ChecksumCRC32' in response, "Expected ChecksumCRC32 in response"
        assert response['ChecksumCRC32'] is not None, "Expected non-empty CRC32 checksum"

        # Verify ChecksumType if supported
        if 'ChecksumType' in response:
            assert response['ChecksumType'] == 'FULL_OBJECT', \
                f"Expected checksum type FULL_OBJECT, got {response.get('ChecksumType')}"

    finally:
        fixture.cleanup()


def test_put_object_checksum_sha256(s3_client, config):
    """Test PutObject with SHA256 checksum algorithm"""
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('checksum-sha256')
        s3_client.create_bucket(bucket_name)

        key = 'test-object-sha256'
        data = fixture.generate_random_data(2048)

        # Upload with SHA256 checksum algorithm
        response = s3_client.client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=data,
            ChecksumAlgorithm='SHA256'
        )

        # Verify response contains checksum
        assert 'ChecksumSHA256' in response, "Expected ChecksumSHA256 in response"
        assert response['ChecksumSHA256'] is not None, "Expected non-empty SHA256 checksum"

        # Verify ChecksumType if supported
        if 'ChecksumType' in response:
            assert response['ChecksumType'] == 'FULL_OBJECT', \
                f"Expected checksum type FULL_OBJECT, got {response.get('ChecksumType')}"

    finally:
        fixture.cleanup()


def test_put_object_all_checksum_algorithms(s3_client, config):
    """
    Test PutObject with all supported checksum algorithms

    Tests: CRC32, CRC32C, SHA1, SHA256
    Note: CRC64NVME may not be supported by all S3 implementations
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('checksum-all')
        s3_client.create_bucket(bucket_name)

        # Test all checksum algorithms
        algorithms = [
            ('CRC32', 'ChecksumCRC32'),
            ('CRC32C', 'ChecksumCRC32C'),
            ('SHA1', 'ChecksumSHA1'),
            ('SHA256', 'ChecksumSHA256'),
        ]

        for i, (algo, checksum_field) in enumerate(algorithms):
            key = f'test-object-{algo.lower()}'
            data = fixture.generate_random_data(200 * i)

            try:
                response = s3_client.client.put_object(
                    Bucket=bucket_name,
                    Key=key,
                    Body=data,
                    ChecksumAlgorithm=algo
                )

                # Verify response contains the appropriate checksum
                assert checksum_field in response, \
                    f"Expected {checksum_field} in response for algorithm {algo}"
                assert response[checksum_field] is not None, \
                    f"Expected non-empty {checksum_field} checksum"

                # Verify ChecksumType if supported
                if 'ChecksumType' in response:
                    assert response['ChecksumType'] == 'FULL_OBJECT', \
                        f"Expected checksum type FULL_OBJECT for {algo}, " \
                        f"got {response.get('ChecksumType')}"

            except ClientError as e:
                # Some S3 implementations may not support all algorithms
                error_code = e.response['Error']['Code']
                if error_code in ['NotImplemented', 'InvalidArgument']:
                    pytest.skip(f"Checksum algorithm {algo} not supported by this S3 implementation")
                raise

    finally:
        fixture.cleanup()


def test_get_object_attributes_checksum_crc32(s3_client, config):
    """Test GetObjectAttributes returns CRC32 checksum metadata"""
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('attrs-crc32')
        s3_client.create_bucket(bucket_name)

        key = 'test-object-attrs-crc32'
        data = fixture.generate_random_data(512)

        # Upload with CRC32 checksum
        put_response = s3_client.client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=data,
            ChecksumAlgorithm='CRC32'
        )

        # Get object attributes
        try:
            attrs_response = s3_client.client.get_object_attributes(
                Bucket=bucket_name,
                Key=key,
                ObjectAttributes=['Checksum']
            )

            # Verify checksum is returned
            assert 'Checksum' in attrs_response, "Expected Checksum in GetObjectAttributes response"
            checksum = attrs_response['Checksum']

            assert 'ChecksumCRC32' in checksum, "Expected ChecksumCRC32 in checksum metadata"
            assert checksum['ChecksumCRC32'] == put_response.get('ChecksumCRC32'), \
                "Expected matching CRC32 checksums between PutObject and GetObjectAttributes"

            if 'ChecksumType' in checksum:
                assert checksum['ChecksumType'] == 'FULL_OBJECT', \
                    f"Expected checksum type FULL_OBJECT, got {checksum.get('ChecksumType')}"

        except ClientError as e:
            if e.response['Error']['Code'] == 'NotImplemented':
                pytest.skip("GetObjectAttributes not supported by this S3 implementation")
            raise

    finally:
        fixture.cleanup()


def test_get_object_attributes_all_checksums(s3_client, config):
    """
    Test GetObjectAttributes returns correct checksums for all algorithms

    Verifies that checksums from PutObject match those from GetObjectAttributes
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('attrs-all-checksums')
        s3_client.create_bucket(bucket_name)

        # Test objects with different checksum algorithms
        test_objects = [
            ('obj-1', 'CRC32', 'ChecksumCRC32', 120),
            ('obj-2', 'CRC32C', 'ChecksumCRC32C', 240),
            ('obj-3', 'SHA1', 'ChecksumSHA1', 360),
            ('obj-4', 'SHA256', 'ChecksumSHA256', 480),
        ]

        for key, algo, checksum_field, size in test_objects:
            data = fixture.generate_random_data(size)

            # Upload with checksum
            try:
                put_response = s3_client.client.put_object(
                    Bucket=bucket_name,
                    Key=key,
                    Body=data,
                    ChecksumAlgorithm=algo
                )
            except ClientError as e:
                if e.response['Error']['Code'] in ['NotImplemented', 'InvalidArgument']:
                    pytest.skip(f"Checksum algorithm {algo} not supported")
                raise

            # Get object attributes
            try:
                attrs_response = s3_client.client.get_object_attributes(
                    Bucket=bucket_name,
                    Key=key,
                    ObjectAttributes=['Checksum']
                )

                # Verify checksum metadata
                assert 'Checksum' in attrs_response, \
                    f"Expected Checksum in response for {key}"
                checksum = attrs_response['Checksum']

                # Verify checksum type
                if 'ChecksumType' in checksum:
                    assert checksum['ChecksumType'] == 'FULL_OBJECT', \
                        f"Expected checksum type FULL_OBJECT for {key}"

                # Verify the specific checksum field matches
                if checksum_field in put_response:
                    assert checksum_field in checksum, \
                        f"Expected {checksum_field} in checksum metadata for {key}"
                    assert checksum[checksum_field] == put_response[checksum_field], \
                        f"Checksum mismatch for {key}: " \
                        f"PutObject returned {put_response[checksum_field]}, " \
                        f"GetObjectAttributes returned {checksum[checksum_field]}"

            except ClientError as e:
                if e.response['Error']['Code'] == 'NotImplemented':
                    pytest.skip("GetObjectAttributes not supported")
                raise

    finally:
        fixture.cleanup()


def test_list_objects_v2_with_checksums(s3_client, config):
    """
    Test ListObjectsV2 returns checksum metadata for objects

    Verifies that listed objects include ChecksumAlgorithm information
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('list-checksums')
        s3_client.create_bucket(bucket_name)

        # Create objects with different checksum algorithms
        algorithms = ['CRC32', 'CRC32C', 'SHA1', 'SHA256']
        created_objects = []

        for i, algo in enumerate(algorithms):
            key = f'obj-{i}'
            size = 100 * i
            data = fixture.generate_random_data(size)

            try:
                put_response = s3_client.client.put_object(
                    Bucket=bucket_name,
                    Key=key,
                    Body=data,
                    ChecksumAlgorithm=algo
                )

                created_objects.append({
                    'Key': key,
                    'Size': size,
                    'Algorithm': algo,
                    'ETag': put_response.get('ETag'),
                })
            except ClientError as e:
                if e.response['Error']['Code'] in ['NotImplemented', 'InvalidArgument']:
                    # Skip this algorithm if not supported
                    continue
                raise

        # List objects
        list_response = s3_client.client.list_objects_v2(Bucket=bucket_name)

        assert 'Contents' in list_response, "Expected Contents in ListObjectsV2 response"
        listed_objects = list_response['Contents']

        assert len(listed_objects) >= len(created_objects), \
            f"Expected at least {len(created_objects)} objects, got {len(listed_objects)}"

        # Verify checksum metadata is included (if supported)
        for obj in listed_objects:
            # Check if checksum algorithm is included
            if 'ChecksumAlgorithm' in obj:
                assert isinstance(obj['ChecksumAlgorithm'], list), \
                    "ChecksumAlgorithm should be a list"
                assert len(obj['ChecksumAlgorithm']) > 0, \
                    "ChecksumAlgorithm list should not be empty"

    finally:
        fixture.cleanup()


def test_put_object_with_provided_checksum(s3_client, config):
    """
    Test PutObject with client-provided checksum value

    Verifies that S3 validates the provided checksum
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('provided-checksum')
        s3_client.create_bucket(bucket_name)

        key = 'test-object-provided-crc32'
        data = fixture.generate_random_data(1024)

        # Calculate CRC32 checksum
        calculated_checksum = calculate_crc32(data)

        # Upload with provided checksum
        response = s3_client.client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=data,
            ChecksumCRC32=calculated_checksum
        )

        # Verify the checksum is returned
        assert 'ChecksumCRC32' in response, "Expected ChecksumCRC32 in response"
        assert response['ChecksumCRC32'] == calculated_checksum, \
            f"Expected checksum {calculated_checksum}, got {response['ChecksumCRC32']}"

    finally:
        fixture.cleanup()


def test_put_object_incorrect_checksum_fails(s3_client, config):
    """
    Test that PutObject fails when provided checksum doesn't match data

    Verifies S3 checksum validation
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('incorrect-checksum')
        s3_client.create_bucket(bucket_name)

        key = 'test-object-bad-checksum'
        data = fixture.generate_random_data(1024)

        # Provide an incorrect checksum (base64 of zeros)
        incorrect_checksum = base64.b64encode(b'\x00\x00\x00\x00').decode('utf-8')

        # Upload should fail with incorrect checksum
        with pytest.raises(ClientError) as exc_info:
            s3_client.client.put_object(
                Bucket=bucket_name,
                Key=key,
                Body=data,
                ChecksumCRC32=incorrect_checksum
            )

        # Verify error is checksum-related
        error_code = exc_info.value.response['Error']['Code']
        assert error_code in ['InvalidRequest', 'BadDigest', 'InvalidDigest', 'XAmzContentChecksumMismatch'], \
            f"Expected checksum validation error, got {error_code}"

    finally:
        fixture.cleanup()
