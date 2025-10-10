#!/usr/bin/env python3
"""
S3 ListObjectVersions Additional Tests

Tests ListObjectVersions with versioning disabled:
- Null version IDs when versioning not enabled
- Multiple objects with null versions
- Version listing without versioning enabled

Ported from versitygw integration tests.

Generated-by: Claude AI
Signed-off-by: Luis Chamberlain <mcgrof@kernel.org>
"""

import pytest
import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from tests.common.fixtures import TestFixture
from botocore.exceptions import ClientError


def test_list_object_versions_versioning_disabled(s3_client, config):
    """
    Test ListObjectVersions when versioning is not enabled

    Objects created before versioning is enabled have null version IDs
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("list-versions-disabled")

        s3_client.create_bucket(bucket_name)

        # NOTE: Do NOT enable versioning - test versioning disabled behavior

        # Put multiple objects without versioning enabled
        objects = []
        for i in range(5):
            object_key = f"my-obj-{i}"
            object_data = b"x" * (i * 100)  # Varying sizes

            s3_client.client.put_object(
                Bucket=bucket_name, Key=object_key, Body=object_data
            )

            objects.append(
                {
                    "Key": object_key,
                    "Size": len(object_data),
                }
            )

        # List object versions
        list_response = s3_client.client.list_object_versions(Bucket=bucket_name)

        # Should have versions list
        assert "Versions" in list_response
        versions = list_response["Versions"]

        # Should have 5 versions
        assert len(versions) == 5, f"Expected 5 versions, got {len(versions)}"

        # Each version should have null version ID (or "null" string)
        # When versioning is not enabled, objects have null versions
        for version in versions:
            assert "VersionId" in version
            version_id = version["VersionId"]
            # MinIO may return "null" string or actual null-like value
            # Both are acceptable for versioning-disabled buckets
            assert (
                version_id == "null" or version_id is None or version_id == ""
            ), f"Expected null version ID, got {version_id}"

            assert "IsLatest" in version
            assert version["IsLatest"] is True

            assert "Key" in version
            assert "Size" in version
            assert "ETag" in version
            assert "StorageClass" in version

        # Verify all keys are present
        version_keys = {v["Key"] for v in versions}
        expected_keys = {obj["Key"] for obj in objects}
        assert version_keys == expected_keys

        # Verify sizes match
        for obj in objects:
            matching_versions = [v for v in versions if v["Key"] == obj["Key"]]
            assert len(matching_versions) == 1
            assert matching_versions[0]["Size"] == obj["Size"]

    finally:
        fixture.cleanup()
