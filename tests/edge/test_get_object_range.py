#!/usr/bin/env python3
"""
S3 GetObject Range Request Tests

Tests GetObject API with Range header:
- Valid byte ranges
- Suffix ranges
- Invalid ranges
- Content-Range headers
- Partial content (206) responses

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


def test_get_object_with_range_basic(s3_client, config):
    """
    Test GetObject with basic byte range

    Verifies partial content retrieval with correct data
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('get-range-basic')
        s3_client.create_bucket(bucket_name)

        key = 'test-object'
        obj_length = 100
        data = fixture.generate_random_data(obj_length)
        s3_client.put_object(bucket_name, key, data)

        # Get first 10 bytes
        response = s3_client.client.get_object(
            Bucket=bucket_name,
            Key=key,
            Range='bytes=0-9'
        )

        retrieved_data = response['Body'].read()
        assert len(retrieved_data) == 10, f"Expected 10 bytes, got {len(retrieved_data)}"
        assert retrieved_data == data[0:10], "Retrieved data doesn't match expected range"
        assert response['ContentLength'] == 10
        assert response.get('ContentRange') == 'bytes 0-9/100'

    finally:
        fixture.cleanup()


def test_get_object_with_range_middle(s3_client, config):
    """
    Test GetObject with range in middle of object

    Verifies retrieval from arbitrary offset
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('get-range-middle')
        s3_client.create_bucket(bucket_name)

        key = 'test-object'
        obj_length = 100
        data = fixture.generate_random_data(obj_length)
        s3_client.put_object(bucket_name, key, data)

        # Get bytes 50-59
        response = s3_client.client.get_object(
            Bucket=bucket_name,
            Key=key,
            Range='bytes=50-59'
        )

        retrieved_data = response['Body'].read()
        assert len(retrieved_data) == 10
        assert retrieved_data == data[50:60]
        assert response.get('ContentRange') == 'bytes 50-59/100'

    finally:
        fixture.cleanup()


def test_get_object_with_range_suffix(s3_client, config):
    """
    Test GetObject with suffix range (last N bytes)

    Verifies negative range syntax for getting last bytes
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('get-range-suffix')
        s3_client.create_bucket(bucket_name)

        key = 'test-object'
        obj_length = 100
        data = fixture.generate_random_data(obj_length)
        s3_client.put_object(bucket_name, key, data)

        # Get last 10 bytes
        response = s3_client.client.get_object(
            Bucket=bucket_name,
            Key=key,
            Range='bytes=-10'
        )

        retrieved_data = response['Body'].read()
        assert len(retrieved_data) == 10
        assert retrieved_data == data[-10:]
        assert response.get('ContentRange') == 'bytes 90-99/100'

    finally:
        fixture.cleanup()


def test_get_object_with_range_open_ended(s3_client, config):
    """
    Test GetObject with open-ended range (from offset to end)

    Verifies 'bytes=N-' syntax
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('get-range-open')
        s3_client.create_bucket(bucket_name)

        key = 'test-object'
        obj_length = 100
        data = fixture.generate_random_data(obj_length)
        s3_client.put_object(bucket_name, key, data)

        # Get from byte 50 to end
        response = s3_client.client.get_object(
            Bucket=bucket_name,
            Key=key,
            Range='bytes=50-'
        )

        retrieved_data = response['Body'].read()
        assert len(retrieved_data) == 50
        assert retrieved_data == data[50:]
        assert response.get('ContentRange') == 'bytes 50-99/100'

    finally:
        fixture.cleanup()


def test_get_object_with_range_entire_object(s3_client, config):
    """
    Test GetObject with range covering entire object

    Verifies 'bytes=0-' returns full object
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('get-range-full')
        s3_client.create_bucket(bucket_name)

        key = 'test-object'
        obj_length = 100
        data = fixture.generate_random_data(obj_length)
        s3_client.put_object(bucket_name, key, data)

        # Get entire object with range
        response = s3_client.client.get_object(
            Bucket=bucket_name,
            Key=key,
            Range='bytes=0-'
        )

        retrieved_data = response['Body'].read()
        assert len(retrieved_data) == obj_length
        assert retrieved_data == data

    finally:
        fixture.cleanup()


def test_get_object_with_range_beyond_object(s3_client, config):
    """
    Test GetObject with range extending beyond object size

    Range should be trimmed to object boundaries
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('get-range-beyond')
        s3_client.create_bucket(bucket_name)

        key = 'test-object'
        obj_length = 100
        data = fixture.generate_random_data(obj_length)
        s3_client.put_object(bucket_name, key, data)

        # Request beyond object size
        response = s3_client.client.get_object(
            Bucket=bucket_name,
            Key=key,
            Range='bytes=0-999'
        )

        retrieved_data = response['Body'].read()
        assert len(retrieved_data) == obj_length
        assert retrieved_data == data
        assert response.get('ContentRange') == 'bytes 0-99/100'

    finally:
        fixture.cleanup()


def test_get_object_with_range_not_satisfiable(s3_client, config):
    """
    Test GetObject with unsatisfiable range

    Range completely beyond object should return error
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('get-range-unsatisfy')
        s3_client.create_bucket(bucket_name)

        key = 'test-object'
        obj_length = 100
        data = fixture.generate_random_data(obj_length)
        s3_client.put_object(bucket_name, key, data)

        # Request range beyond object
        with pytest.raises(ClientError) as exc_info:
            s3_client.client.get_object(
                Bucket=bucket_name,
                Key=key,
                Range='bytes=100-110'
            )

        error_code = exc_info.value.response['Error']['Code']
        assert error_code in ['InvalidRange', 'RequestedRangeNotSatisfiable', '416'], \
            f"Expected InvalidRange or RequestedRangeNotSatisfiable, got {error_code}"

    finally:
        fixture.cleanup()


def test_get_object_with_range_single_byte(s3_client, config):
    """
    Test GetObject with single byte range

    Verifies retrieving exactly one byte
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('get-range-single')
        s3_client.create_bucket(bucket_name)

        key = 'test-object'
        obj_length = 100
        data = fixture.generate_random_data(obj_length)
        s3_client.put_object(bucket_name, key, data)

        # Get first byte
        response = s3_client.client.get_object(
            Bucket=bucket_name,
            Key=key,
            Range='bytes=0-0'
        )

        retrieved_data = response['Body'].read()
        assert len(retrieved_data) == 1
        assert retrieved_data == data[0:1]
        assert response.get('ContentRange') == 'bytes 0-0/100'

        # Get last byte
        response = s3_client.client.get_object(
            Bucket=bucket_name,
            Key=key,
            Range='bytes=99-99'
        )

        retrieved_data = response['Body'].read()
        assert len(retrieved_data) == 1
        assert retrieved_data == data[99:100]
        assert response.get('ContentRange') == 'bytes 99-99/100'

    finally:
        fixture.cleanup()


def test_get_object_with_range_last_byte(s3_client, config):
    """
    Test GetObject with suffix-1 (last single byte)

    Verifies 'bytes=-1' returns last byte
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('get-range-last')
        s3_client.create_bucket(bucket_name)

        key = 'test-object'
        obj_length = 100
        data = fixture.generate_random_data(obj_length)
        s3_client.put_object(bucket_name, key, data)

        # Get last byte with suffix range
        response = s3_client.client.get_object(
            Bucket=bucket_name,
            Key=key,
            Range='bytes=-1'
        )

        retrieved_data = response['Body'].read()
        assert len(retrieved_data) == 1
        assert retrieved_data == data[-1:]

    finally:
        fixture.cleanup()


def test_get_object_range_data_integrity(s3_client, config):
    """
    Test GetObject range data integrity with known pattern

    Verifies exact byte match for range requests
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name('get-range-integrity')
        s3_client.create_bucket(bucket_name)

        key = 'test-object'
        # Create data with known pattern (0-255 repeated)
        data = bytes(range(256)) * 4  # 1024 bytes
        s3_client.put_object(bucket_name, key, data)

        # Test various ranges
        test_cases = [
            (0, 9),    # First 10 bytes
            (100, 199),  # Middle 100 bytes
            (1000, 1023),  # Last 24 bytes
            (256, 511),  # Second block
        ]

        for start, end in test_cases:
            response = s3_client.client.get_object(
                Bucket=bucket_name,
                Key=key,
                Range=f'bytes={start}-{end}'
            )

            retrieved_data = response['Body'].read()
            expected_data = data[start:end+1]

            assert len(retrieved_data) == len(expected_data), \
                f"Length mismatch for range {start}-{end}"
            assert retrieved_data == expected_data, \
                f"Data mismatch for range {start}-{end}"

    finally:
        fixture.cleanup()
