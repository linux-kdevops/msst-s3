#!/usr/bin/env python3
"""
S3 Versioning Delete Markers and CopyObject Tests

Tests versioning with delete markers and copy operations:
- Delete markers creation and behavior
- HeadObject and GetObject with delete markers
- CopyObject with versioning
- Copying specific object versions
- DeleteObjects with versioning

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


def suspend_versioning(s3_client, bucket_name):
    """Helper to suspend versioning on a bucket"""
    s3_client.put_bucket_versioning(bucket_name, {"Status": "Suspended"})


def test_versioning_head_object_delete_marker(s3_client, config):
    """
    Test HeadObject on delete marker

    Should return MethodNotAllowed for delete marker version
    Should return NotFound without version ID
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("ver-head-dm")
        s3_client.create_bucket(bucket_name)
        enable_versioning(s3_client, bucket_name)

        # Put object
        key = "my-obj"
        s3_client.put_object(bucket_name, key, b"x" * 2000)

        # Delete object (creates delete marker)
        delete_response = s3_client.client.delete_object(Bucket=bucket_name, Key=key)
        delete_marker_version = delete_response.get("VersionId")

        # HeadObject on delete marker version should fail
        if delete_marker_version:
            with pytest.raises(ClientError) as exc_info:
                s3_client.client.head_object(
                    Bucket=bucket_name, Key=key, VersionId=delete_marker_version
                )
            error_code = exc_info.value.response["Error"]["Code"]
            assert error_code in [
                "MethodNotAllowed",
                "405",
            ], f"Expected MethodNotAllowed/405, got {error_code}"

        # HeadObject without version ID should return NotFound
        with pytest.raises(ClientError) as exc_info:
            s3_client.client.head_object(Bucket=bucket_name, Key=key)
        error_code = exc_info.value.response["Error"]["Code"]
        assert error_code in [
            "NotFound",
            "404",
        ], f"Expected NotFound/404, got {error_code}"

    finally:
        fixture.cleanup()


def test_versioning_get_object_delete_marker_without_version_id(s3_client, config):
    """
    Test GetObject on key with delete marker (without version ID)

    Should return NoSuchKey error
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("ver-get-dm-no-id")
        s3_client.create_bucket(bucket_name)
        enable_versioning(s3_client, bucket_name)

        # Put object
        key = "my-obj"
        s3_client.put_object(bucket_name, key, b"test data")

        # Delete object (creates delete marker)
        s3_client.client.delete_object(Bucket=bucket_name, Key=key)

        # GetObject without version ID should fail with NoSuchKey
        with pytest.raises(ClientError) as exc_info:
            s3_client.get_object(bucket_name, key)

        error_code = exc_info.value.response["Error"]["Code"]
        assert error_code == "NoSuchKey", f"Expected NoSuchKey, got {error_code}"

    finally:
        fixture.cleanup()


def test_versioning_get_object_delete_marker(s3_client, config):
    """
    Test GetObject on delete marker with version ID

    Should return MethodNotAllowed error
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("ver-get-dm")
        s3_client.create_bucket(bucket_name)
        enable_versioning(s3_client, bucket_name)

        # Put object
        key = "my-obj"
        s3_client.put_object(bucket_name, key, b"test data")

        # Delete object (creates delete marker)
        delete_response = s3_client.client.delete_object(Bucket=bucket_name, Key=key)
        delete_marker_version = delete_response.get("VersionId")

        # GetObject on delete marker version should fail
        if delete_marker_version:
            with pytest.raises(ClientError) as exc_info:
                s3_client.client.get_object(
                    Bucket=bucket_name, Key=key, VersionId=delete_marker_version
                )

            error_code = exc_info.value.response["Error"]["Code"]
            assert error_code in [
                "MethodNotAllowed",
                "405",
            ], f"Expected MethodNotAllowed/405, got {error_code}"

    finally:
        fixture.cleanup()


def test_versioning_delete_object_delete_a_delete_marker(s3_client, config):
    """
    Test deleting a delete marker

    Should remove the delete marker and restore object visibility
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("ver-del-dm")
        s3_client.create_bucket(bucket_name)
        enable_versioning(s3_client, bucket_name)

        # Put object
        key = "my-obj"
        data = b"original data"
        put_response = s3_client.put_object(bucket_name, key, data)
        object_version = put_response["VersionId"]

        # Delete object (creates delete marker)
        delete_response = s3_client.client.delete_object(Bucket=bucket_name, Key=key)
        delete_marker_version = delete_response.get("VersionId")
        assert delete_response.get("DeleteMarker") is True

        # Object should not be accessible without version ID
        with pytest.raises(ClientError) as exc_info:
            s3_client.get_object(bucket_name, key)
        assert exc_info.value.response["Error"]["Code"] == "NoSuchKey"

        # Delete the delete marker
        if delete_marker_version:
            delete_dm_response = s3_client.client.delete_object(
                Bucket=bucket_name, Key=key, VersionId=delete_marker_version
            )
            # When deleting a delete marker, DeleteMarker should be True
            assert delete_dm_response.get("DeleteMarker") is True

            # Object should now be accessible again (shows previous version)
            get_response = s3_client.get_object(bucket_name, key)
            assert get_response["Body"].read() == data
            assert get_response["VersionId"] == object_version

    finally:
        fixture.cleanup()


def test_versioning_delete_null_version_id_object(s3_client, config):
    """
    Test deleting object with null version ID

    Should delete null version permanently
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("ver-del-null")
        s3_client.create_bucket(bucket_name)

        # Put object before enabling versioning (null version)
        key = "my-obj"
        s3_client.put_object(bucket_name, key, b"null version data")

        # Enable versioning
        enable_versioning(s3_client, bucket_name)

        # Put new version
        s3_client.put_object(bucket_name, key, b"versioned data")

        # Delete null version specifically
        delete_response = s3_client.client.delete_object(
            Bucket=bucket_name, Key=key, VersionId="null"
        )
        # MinIO may not return VersionId in response
        if "VersionId" in delete_response:
            assert delete_response["VersionId"] == "null"

        # Null version should be gone
        with pytest.raises(ClientError) as exc_info:
            s3_client.client.get_object(Bucket=bucket_name, Key=key, VersionId="null")
        error_code = exc_info.value.response["Error"]["Code"]
        assert error_code in ["NoSuchVersion", "NoSuchKey", "404"]

    finally:
        fixture.cleanup()


def test_versioning_delete_object_suspended(s3_client, config):
    """
    Test DeleteObject when versioning is suspended

    Should create or update null version
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("ver-del-suspended")
        s3_client.create_bucket(bucket_name)
        enable_versioning(s3_client, bucket_name)

        # Put versioned object
        key = "my-obj"
        response1 = s3_client.put_object(bucket_name, key, b"v1")
        version_id1 = response1["VersionId"]

        # Suspend versioning
        suspend_versioning(s3_client, bucket_name)

        # Delete object while suspended
        delete_response = s3_client.client.delete_object(Bucket=bucket_name, Key=key)

        # When suspended, delete may create null version delete marker
        # or just return success without version info
        assert delete_response["ResponseMetadata"]["HTTPStatusCode"] == 204

        # Original version should still exist
        get_response = s3_client.client.get_object(
            Bucket=bucket_name, Key=key, VersionId=version_id1
        )
        assert get_response["Body"].read() == b"v1"

    finally:
        fixture.cleanup()


def test_versioning_copy_object_success(s3_client, config):
    """
    Test CopyObject with versioning

    Should create new version in destination
    """
    fixture = TestFixture(s3_client, config)

    try:
        dest_bucket = fixture.generate_bucket_name("ver-copy-dest")
        src_bucket = fixture.generate_bucket_name("ver-copy-src")
        s3_client.create_bucket(dest_bucket)
        s3_client.create_bucket(src_bucket)
        enable_versioning(s3_client, dest_bucket)
        enable_versioning(s3_client, src_bucket)

        # Create source object
        src_key = "src-obj"
        src_data = b"z" * 2345
        s3_client.put_object(src_bucket, src_key, src_data)

        # Create destination object (first version)
        dest_key = "dest-obj"
        s3_client.put_object(dest_bucket, dest_key, b"old data")

        # Copy source to destination
        copy_response = s3_client.client.copy_object(
            Bucket=dest_bucket,
            Key=dest_key,
            CopySource=f"{src_bucket}/{src_key}",
        )

        # Should create new version
        assert "VersionId" in copy_response
        assert copy_response["VersionId"] is not None
        new_version_id = copy_response["VersionId"]

        # Verify destination has new content
        get_response = s3_client.get_object(dest_bucket, dest_key)
        assert get_response["Body"].read() == src_data
        assert get_response["VersionId"] == new_version_id

    finally:
        fixture.cleanup()


def test_versioning_copy_object_non_existing_version_id(s3_client, config):
    """
    Test CopyObject with non-existing source version ID

    Should return NoSuchKey error
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("ver-copy-no-ver")
        s3_client.create_bucket(bucket_name)
        enable_versioning(s3_client, bucket_name)

        # Create source object
        src_key = "src-obj"
        s3_client.put_object(bucket_name, src_key, b"source data")

        # Try to copy with invalid version ID
        with pytest.raises(ClientError) as exc_info:
            s3_client.client.copy_object(
                Bucket=bucket_name,
                Key="dest-obj",
                CopySource=f"{bucket_name}/{src_key}?versionId=invalid-version-id",
            )

        error_code = exc_info.value.response["Error"]["Code"]
        assert error_code in [
            "NoSuchKey",
            "NoSuchVersion",
        ], f"Expected NoSuchKey/NoSuchVersion, got {error_code}"

    finally:
        fixture.cleanup()


def test_versioning_copy_object_from_an_object_version(s3_client, config):
    """
    Test CopyObject from specific source version

    Should copy data from specified version
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("ver-copy-from-ver")
        s3_client.create_bucket(bucket_name)
        enable_versioning(s3_client, bucket_name)

        # Create multiple versions of source object
        src_key = "src-obj"
        data1 = b"version 1 data"
        response1 = s3_client.put_object(bucket_name, src_key, data1)
        version_id1 = response1["VersionId"]

        data2 = b"version 2 data"
        s3_client.put_object(bucket_name, src_key, data2)

        # Copy from first version specifically
        dest_key = "dest-obj"
        copy_response = s3_client.client.copy_object(
            Bucket=bucket_name,
            Key=dest_key,
            CopySource=f"{bucket_name}/{src_key}?versionId={version_id1}",
        )

        assert "VersionId" in copy_response

        # Verify destination has data from version 1
        get_response = s3_client.get_object(bucket_name, dest_key)
        assert get_response["Body"].read() == data1

    finally:
        fixture.cleanup()


def test_versioning_delete_objects_success(s3_client, config):
    """
    Test DeleteObjects (batch delete) with versioning

    Should create delete markers for objects
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("ver-del-objs")
        s3_client.create_bucket(bucket_name)
        enable_versioning(s3_client, bucket_name)

        # Create multiple objects
        keys = ["obj1", "obj2", "obj3"]
        for key in keys:
            s3_client.put_object(bucket_name, key, b"test data")

        # Batch delete
        delete_response = s3_client.client.delete_objects(
            Bucket=bucket_name,
            Delete={"Objects": [{"Key": key} for key in keys]},
        )

        # Should have deleted all objects
        assert "Deleted" in delete_response
        assert len(delete_response["Deleted"]) == 3

        # All deletes should have delete markers
        for deleted in delete_response["Deleted"]:
            assert deleted["Key"] in keys
            # MinIO may or may not return DeleteMarker/VersionId
            if "DeleteMarker" in deleted:
                assert deleted["DeleteMarker"] is True

        # Objects should not be accessible without version ID
        for key in keys:
            with pytest.raises(ClientError) as exc_info:
                s3_client.get_object(bucket_name, key)
            assert exc_info.value.response["Error"]["Code"] == "NoSuchKey"

    finally:
        fixture.cleanup()


def test_versioning_delete_objects_delete_delete_markers(s3_client, config):
    """
    Test DeleteObjects deleting specific versions including delete markers

    Should permanently delete specified versions
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("ver-del-dms")
        s3_client.create_bucket(bucket_name)
        enable_versioning(s3_client, bucket_name)

        # Create object
        key = "my-obj"
        put_response = s3_client.put_object(bucket_name, key, b"data")
        object_version = put_response["VersionId"]

        # Delete object (create delete marker)
        delete_response = s3_client.client.delete_object(Bucket=bucket_name, Key=key)
        delete_marker_version = delete_response.get("VersionId")

        # Batch delete both delete marker and object version
        objects_to_delete = []
        if delete_marker_version:
            objects_to_delete.append({"Key": key, "VersionId": delete_marker_version})
        objects_to_delete.append({"Key": key, "VersionId": object_version})

        batch_delete_response = s3_client.client.delete_objects(
            Bucket=bucket_name,
            Delete={"Objects": objects_to_delete},
        )

        # Should have deleted all versions
        assert len(batch_delete_response["Deleted"]) == len(objects_to_delete)

        # Object should be completely gone
        with pytest.raises(ClientError) as exc_info:
            s3_client.get_object(bucket_name, key)
        assert exc_info.value.response["Error"]["Code"] == "NoSuchKey"

    finally:
        fixture.cleanup()
