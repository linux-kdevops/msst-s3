#!/usr/bin/env python3
"""
S3 Versioning with GetObjectAttributes Tests

Tests GetObjectAttributes API with versioning:
- GetObjectAttributes with VersionId parameter
- GetObjectAttributes on delete markers
- GetObjectAttributes on latest version
- Versioning edge cases (special characters, concurrent operations)

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


def enable_versioning(s3_client, bucket_name):
    """Helper to enable versioning on a bucket"""
    s3_client.put_bucket_versioning(bucket_name, {"Status": "Enabled"})


def test_versioning_get_object_attributes_object_version(s3_client, config):
    """
    Test GetObjectAttributes with VersionId parameter

    Should return attributes for specific version and latest version
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("ver-goa-version")
        s3_client.create_bucket(bucket_name)
        enable_versioning(s3_client, bucket_name)

        key = "my-obj"

        # Create first version
        data1 = b"version 1 data"
        response1 = s3_client.put_object(bucket_name, key, data1)
        version_id1 = response1["VersionId"]

        # Create second version
        data2 = b"version 2 data is longer"
        response2 = s3_client.put_object(bucket_name, key, data2)
        version_id2 = response2["VersionId"]

        # GetObjectAttributes for first version
        attrs1 = s3_client.client.get_object_attributes(
            Bucket=bucket_name,
            Key=key,
            VersionId=version_id1,
            ObjectAttributes=["ETag", "ObjectSize", "StorageClass"],
        )

        assert attrs1["VersionId"] == version_id1
        assert "ETag" in attrs1
        assert attrs1["ObjectSize"] == len(data1)
        assert "StorageClass" in attrs1

        # GetObjectAttributes for second version
        attrs2 = s3_client.client.get_object_attributes(
            Bucket=bucket_name,
            Key=key,
            VersionId=version_id2,
            ObjectAttributes=["ETag", "ObjectSize", "StorageClass"],
        )

        assert attrs2["VersionId"] == version_id2
        assert attrs2["ObjectSize"] == len(data2)

        # GetObjectAttributes without VersionId (should return latest)
        attrs_latest = s3_client.client.get_object_attributes(
            Bucket=bucket_name,
            Key=key,
            ObjectAttributes=["ETag", "ObjectSize", "StorageClass"],
        )

        assert attrs_latest["VersionId"] == version_id2
        assert attrs_latest["ObjectSize"] == len(data2)

    finally:
        fixture.cleanup()


def test_versioning_get_object_attributes_delete_marker(s3_client, config):
    """
    Test GetObjectAttributes on delete marker

    Should return NoSuchKey error
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("ver-goa-dm")
        s3_client.create_bucket(bucket_name)
        enable_versioning(s3_client, bucket_name)

        key = "my-obj"

        # Create object version
        s3_client.put_object(bucket_name, key, b"test data")

        # Delete object (creates delete marker)
        delete_response = s3_client.client.delete_object(Bucket=bucket_name, Key=key)
        delete_marker_version = delete_response.get("VersionId")

        # GetObjectAttributes on delete marker version should fail
        if delete_marker_version:
            with pytest.raises(ClientError) as exc_info:
                s3_client.client.get_object_attributes(
                    Bucket=bucket_name,
                    Key=key,
                    VersionId=delete_marker_version,
                    ObjectAttributes=["ETag", "ObjectSize"],
                )

            error_code = exc_info.value.response["Error"]["Code"]
            # MinIO may return NoSuchKey or MethodNotAllowed
            assert error_code in [
                "NoSuchKey",
                "MethodNotAllowed",
                "404",
                "405",
            ], f"Expected NoSuchKey/MethodNotAllowed, got {error_code}"

        # GetObjectAttributes without version ID should also fail
        with pytest.raises(ClientError) as exc_info:
            s3_client.client.get_object_attributes(
                Bucket=bucket_name,
                Key=key,
                ObjectAttributes=["ETag", "ObjectSize"],
            )

        error_code = exc_info.value.response["Error"]["Code"]
        assert error_code in [
            "NoSuchKey",
            "404",
        ], f"Expected NoSuchKey/404, got {error_code}"

    finally:
        fixture.cleanup()


def test_versioning_copy_object_special_chars(s3_client, config):
    """
    Test CopyObject with special characters in keys and versionId

    Should handle URL encoding and versionId in CopySource
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("ver-copy-special")
        s3_client.create_bucket(bucket_name)
        enable_versioning(s3_client, bucket_name)

        # Create source object with special characters in key
        src_key = "my-obj?test&data"
        src_data = b"source data with special chars"
        response = s3_client.put_object(bucket_name, src_key, src_data)
        version_id = response["VersionId"]

        # Copy with versionId in CopySource (boto3 handles URL encoding)
        dest_key = "dest-obj"
        copy_response = s3_client.client.copy_object(
            Bucket=bucket_name,
            Key=dest_key,
            CopySource={"Bucket": bucket_name, "Key": src_key, "VersionId": version_id},
        )

        # Should have CopySourceVersionId in response
        # MinIO may not return this field
        if "CopySourceVersionId" in copy_response:
            assert copy_response["CopySourceVersionId"] == version_id

        # Verify destination
        get_response = s3_client.get_object(bucket_name, dest_key)
        assert get_response["Body"].read() == src_data

    finally:
        fixture.cleanup()


def test_versioning_concurrent_upload_object(s3_client, config):
    """
    Test concurrent uploads of same object key with versioning

    All versions should be created successfully
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("ver-concurrent")
        s3_client.create_bucket(bucket_name)
        enable_versioning(s3_client, bucket_name)

        key = "my-obj"

        # Create 5 versions rapidly (simulating concurrent uploads)
        version_ids = []
        for i in range(5):
            response = s3_client.put_object(bucket_name, key, f"version-{i}".encode())
            version_ids.append(response["VersionId"])

        # All version IDs should be unique
        assert len(set(version_ids)) == 5, "All version IDs should be unique"

        # List versions - should have all 5
        list_response = s3_client.client.list_object_versions(Bucket=bucket_name)
        assert "Versions" in list_response
        assert len(list_response["Versions"]) == 5

        # Verify all version IDs are in the list
        listed_version_ids = [v["VersionId"] for v in list_response["Versions"]]
        for version_id in version_ids:
            assert version_id in listed_version_ids

        # All versions should be accessible
        for i, version_id in enumerate(version_ids):
            get_response = s3_client.client.get_object(
                Bucket=bucket_name, Key=key, VersionId=version_id
            )
            assert get_response["Body"].read() == f"version-{i}".encode()

    finally:
        fixture.cleanup()
