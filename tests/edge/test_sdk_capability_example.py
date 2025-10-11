#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Example test demonstrating SDK capability-aware testing

This test shows how to adapt test expectations based on SDK capabilities.
Different AWS SDKs have different behaviors, and tests should account for these
differences rather than failing on legitimate SDK-specific behavior.

Generated-by: Claude AI
Signed-off-by: Luis Chamberlain <mcgrof@kernel.org>
"""

import pytest
from tests.common.test_utils import random_string


def test_list_objects_url_plus_encoding(s3_client, sdk_capabilities, config):
    """
    Test how SDK handles '+' characters in object keys during listing.

    Different SDKs treat '+' differently in URL encoding:
    - Some treat '+' as '+' (correct)
    - Some treat '+' as a space (legacy URL encoding)

    This test adapts its expectations based on the SDK capability profile.
    """
    bucket_name = f"{config['s3_bucket_prefix']}-sdk-caps-{random_string()}"

    try:
        # Create bucket
        s3_client.create_bucket(bucket_name)

        # Upload an object with a '+' in the key
        key_with_plus = "test+object.txt"
        s3_client.put_object(bucket_name, key_with_plus, b"test data")

        # Check SDK capability for how it handles '+' in listing
        caps = sdk_capabilities.get("profile", {})
        plus_treated_as_space = caps.get(
            "list_objects_url_plus_treated_as_space", False
        )

        if plus_treated_as_space:
            # SDK treats '+' as space in URL encoding
            # We need to search for "test object.txt" (with space)
            prefix = "test "
        else:
            # SDK correctly treats '+' as '+'
            prefix = "test+"

        # List objects with the appropriate prefix
        objects = s3_client.list_objects(bucket_name, prefix=prefix)

        # Verify we found our object
        found = any(obj.get("Key") == key_with_plus for obj in objects)
        assert found, (
            f"Object with key '{key_with_plus}' not found using prefix '{prefix}'. "
            f"SDK capability 'list_objects_url_plus_treated_as_space' = {plus_treated_as_space}"
        )

        # Also verify the object exists with head request
        response = s3_client.head_object(bucket_name, key_with_plus)
        assert response is not None

    finally:
        # Cleanup
        try:
            s3_client.delete_object(bucket_name, key_with_plus)
        except Exception:
            pass
        try:
            s3_client.delete_bucket(bucket_name)
        except Exception:
            pass


def test_retry_behavior(s3_client, sdk_capabilities):
    """
    Test that demonstrates awareness of SDK retry mode.

    Different SDKs have different retry modes:
    - standard: Fixed retry intervals
    - adaptive: Adaptive retry based on service load
    - legacy: Old retry behavior

    This test doesn't change behavior but demonstrates how to access capabilities.
    """
    caps = sdk_capabilities.get("profile", {})
    retry_mode = caps.get("retry_mode", "standard")

    # Log the retry mode (in a real test, this might affect timeout expectations)
    print(f"SDK retry mode: {retry_mode}")

    # Tests that expect specific timing behavior should account for retry_mode
    # For example, adaptive retry might take longer on loaded systems
    if retry_mode == "adaptive":
        # Might use longer timeouts or be more forgiving
        pass
    elif retry_mode == "legacy":
        # Might need to account for different retry counts
        pass


def test_checksum_defaults(s3_client, sdk_capabilities, config):
    """
    Test SDK default checksum behavior.

    Some modern SDKs default to using CRC32C checksums, while others use MD5.
    Tests should be aware of which checksum algorithm is expected.
    """
    bucket_name = f"{config['s3_bucket_prefix']}-checksum-{random_string()}"

    try:
        # Create bucket
        s3_client.create_bucket(bucket_name)

        # Upload an object
        key = "checksum-test.txt"
        body = b"test data for checksum"
        response = s3_client.put_object(bucket_name, key, body)

        # Check SDK capability for CRC32C default
        caps = sdk_capabilities.get("profile", {})
        uses_crc32c = caps.get("crc32c_default", False)

        if uses_crc32c:
            # Modern SDKs might include CRC32C in response
            # Tests can verify this if needed
            print("SDK defaults to CRC32C checksums")
        else:
            # Traditional MD5 ETag behavior
            assert "ETag" in response
            print("SDK uses traditional MD5 ETag")

        # Cleanup
        s3_client.delete_object(bucket_name, key)

    finally:
        try:
            s3_client.delete_bucket(bucket_name)
        except Exception:
            pass


def test_virtual_hosted_style_default(s3_client, sdk_capabilities):
    """
    Test demonstrating awareness of SDK addressing style.

    SDKs can use:
    - Virtual-hosted style: bucket.s3.region.amazonaws.com
    - Path style: s3.region.amazonaws.com/bucket

    This affects how URLs are constructed and validated.
    """
    caps = sdk_capabilities.get("profile", {})
    virtual_hosted = caps.get("virtual_hosted_default", True)

    # Tests that validate URLs or host headers should account for this
    if virtual_hosted:
        print("SDK uses virtual-hosted style by default")
        # Expect bucket.endpoint format
    else:
        print("SDK uses path style by default")
        # Expect endpoint/bucket format


def test_capability_access_from_client(s3_client):
    """
    Demonstrate accessing capabilities directly from the S3Client.

    The S3Client has convenience methods for checking capabilities.
    """
    # Check if a specific capability is enabled
    has_sigv4_chunked = s3_client.has_capability("sigv4_chunked")
    print(f"SigV4 chunked encoding supported: {has_sigv4_chunked}")

    # Get capability value with default
    retry_mode = s3_client.get_capability("retry_mode", "standard")
    print(f"Retry mode: {retry_mode}")

    # Access all capabilities
    all_caps = s3_client.capabilities
    print(f"All capabilities: {all_caps}")


if __name__ == "__main__":
    # Run with pytest
    pytest.main([__file__, "-v"])
