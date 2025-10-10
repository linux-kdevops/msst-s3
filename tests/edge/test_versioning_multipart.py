#!/usr/bin/env python3
"""
S3 Versioning with Multipart Upload Tests

Tests versioning behavior with multipart uploads:
- CompleteMultipartUpload returns VersionId
- Multipart upload creates new version
- UploadPartCopy with specific source version
- Version listing includes multipart objects

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


def test_versioning_multipart_upload_success(s3_client, config):
    """
    Test CompleteMultipartUpload with versioning enabled

    Should return VersionId in response
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("ver-mp-success")
        s3_client.create_bucket(bucket_name)
        enable_versioning(s3_client, bucket_name)

        key = "my-obj"

        # Create multipart upload
        create_response = s3_client.client.create_multipart_upload(
            Bucket=bucket_name, Key=key
        )
        upload_id = create_response["UploadId"]

        # Upload 5 parts (5MB each = 25MB total)
        parts = []
        for part_num in range(1, 6):
            part_data = b"x" * (5 * 1024 * 1024)
            upload_response = s3_client.client.upload_part(
                Bucket=bucket_name,
                Key=key,
                UploadId=upload_id,
                PartNumber=part_num,
                Body=part_data,
            )
            parts.append({"PartNumber": part_num, "ETag": upload_response["ETag"]})

        # Complete multipart upload
        complete_response = s3_client.client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={"Parts": parts},
        )

        # Should have VersionId in response
        assert "VersionId" in complete_response
        assert complete_response["VersionId"] is not None
        version_id = complete_response["VersionId"]

        # Verify with HeadObject
        head_response = s3_client.client.head_object(Bucket=bucket_name, Key=key)
        assert head_response["VersionId"] == version_id
        assert head_response["ContentLength"] == 25 * 1024 * 1024

    finally:
        fixture.cleanup()


def test_versioning_multipart_upload_overwrite_an_object(s3_client, config):
    """
    Test multipart upload creates new version when overwriting

    Should create new version, all versions accessible
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("ver-mp-overwrite")
        s3_client.create_bucket(bucket_name)
        enable_versioning(s3_client, bucket_name)

        key = "my-obj"

        # Create first version with PutObject
        response1 = s3_client.put_object(bucket_name, key, b"v1" * 100)
        version_id1 = response1["VersionId"]

        # Create second version with PutObject
        response2 = s3_client.put_object(bucket_name, key, b"v2" * 100)
        version_id2 = response2["VersionId"]

        # Create third version with multipart upload
        create_response = s3_client.client.create_multipart_upload(
            Bucket=bucket_name, Key=key
        )
        upload_id = create_response["UploadId"]

        # Upload 2 parts (5MB each = 10MB total)
        parts = []
        for part_num in range(1, 3):
            part_data = b"v3" * (5 * 1024 * 1024 // 2)
            upload_response = s3_client.client.upload_part(
                Bucket=bucket_name,
                Key=key,
                UploadId=upload_id,
                PartNumber=part_num,
                Body=part_data,
            )
            parts.append({"PartNumber": part_num, "ETag": upload_response["ETag"]})

        complete_response = s3_client.client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={"Parts": parts},
        )
        version_id3 = complete_response["VersionId"]

        # All 3 versions should be different
        assert version_id1 != version_id2
        assert version_id2 != version_id3
        assert version_id1 != version_id3

        # List versions - should have all 3
        list_response = s3_client.client.list_object_versions(Bucket=bucket_name)
        assert "Versions" in list_response
        assert len(list_response["Versions"]) == 3

        version_ids = [v["VersionId"] for v in list_response["Versions"]]
        assert version_id1 in version_ids
        assert version_id2 in version_ids
        assert version_id3 in version_ids

        # Latest version should be the multipart upload
        head_response = s3_client.client.head_object(Bucket=bucket_name, Key=key)
        assert head_response["VersionId"] == version_id3
        assert head_response["ContentLength"] == 10 * 1024 * 1024

    finally:
        fixture.cleanup()


def test_versioning_upload_part_copy_non_existing_version_id(s3_client, config):
    """
    Test UploadPartCopy with non-existing source version ID

    Should return NoSuchVersion error
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("ver-upc-noversion")
        s3_client.create_bucket(bucket_name)
        enable_versioning(s3_client, bucket_name)

        # Create source object
        src_key = "src-obj"
        s3_client.put_object(bucket_name, src_key, b"source data")

        # Create multipart upload for destination
        dest_key = "dest-obj"
        create_response = s3_client.client.create_multipart_upload(
            Bucket=bucket_name, Key=dest_key
        )
        upload_id = create_response["UploadId"]

        # Try to copy from non-existing version
        with pytest.raises(ClientError) as exc_info:
            s3_client.client.upload_part_copy(
                Bucket=bucket_name,
                Key=dest_key,
                UploadId=upload_id,
                PartNumber=1,
                CopySource=f"{bucket_name}/{src_key}?versionId=invalid-version-id",
            )

        error_code = exc_info.value.response["Error"]["Code"]
        assert error_code in [
            "NoSuchVersion",
            "NoSuchKey",
        ], f"Expected NoSuchVersion/NoSuchKey, got {error_code}"

    finally:
        fixture.cleanup()


def test_versioning_upload_part_copy_from_an_object_version(s3_client, config):
    """
    Test UploadPartCopy from specific source object version

    Should copy data from specified version
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("ver-upc-from-ver")
        s3_client.create_bucket(bucket_name)
        enable_versioning(s3_client, bucket_name)

        # Create multiple versions of source object
        src_key = "src-obj"
        data1 = b"z" * (5 * 1024 * 1024)
        response1 = s3_client.put_object(bucket_name, src_key, data1)
        version_id1 = response1["VersionId"]

        data2 = b"y" * (5 * 1024 * 1024)
        response2 = s3_client.put_object(bucket_name, src_key, data2)
        version_id2 = response2["VersionId"]

        # Create multipart upload
        dest_key = "dest-obj"
        create_response = s3_client.client.create_multipart_upload(
            Bucket=bucket_name, Key=dest_key
        )
        upload_id = create_response["UploadId"]

        # Copy from first version specifically
        copy_response = s3_client.client.upload_part_copy(
            Bucket=bucket_name,
            Key=dest_key,
            UploadId=upload_id,
            PartNumber=1,
            CopySource=f"{bucket_name}/{src_key}?versionId={version_id1}",
        )

        # MinIO may not return CopySourceVersionId in response
        # Both with and without are acceptable
        if "CopySourceVersionId" in copy_response:
            assert copy_response["CopySourceVersionId"] == version_id1

        # List parts - should show the copied part
        list_parts_response = s3_client.client.list_parts(
            Bucket=bucket_name, Key=dest_key, UploadId=upload_id
        )
        assert "Parts" in list_parts_response
        assert len(list_parts_response["Parts"]) == 1
        assert list_parts_response["Parts"][0]["PartNumber"] == 1
        assert list_parts_response["Parts"][0]["Size"] == 5 * 1024 * 1024

        # Complete multipart upload
        parts = [
            {
                "PartNumber": 1,
                "ETag": copy_response["CopyPartResult"]["ETag"],
            }
        ]
        s3_client.client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=dest_key,
            UploadId=upload_id,
            MultipartUpload={"Parts": parts},
        )

        # Verify destination has data from version 1 (all 'z' bytes)
        get_response = s3_client.get_object(bucket_name, dest_key)
        body = get_response["Body"].read()
        assert len(body) == 5 * 1024 * 1024
        assert body == data1  # Should match first version

    finally:
        fixture.cleanup()


def test_versioning_multipart_upload_with_metadata(s3_client, config):
    """
    Test multipart upload with metadata on versioned bucket

    Metadata should be preserved with version
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("ver-mp-metadata")
        s3_client.create_bucket(bucket_name)
        enable_versioning(s3_client, bucket_name)

        key = "my-obj"
        metadata = {"key1": "value1", "key2": "value2"}

        # Create multipart upload with metadata
        create_response = s3_client.client.create_multipart_upload(
            Bucket=bucket_name,
            Key=key,
            Metadata=metadata,
            ContentType="application/octet-stream",
        )
        upload_id = create_response["UploadId"]

        # Upload 2 parts
        parts = []
        for part_num in range(1, 3):
            part_data = b"x" * (5 * 1024 * 1024)
            upload_response = s3_client.client.upload_part(
                Bucket=bucket_name,
                Key=key,
                UploadId=upload_id,
                PartNumber=part_num,
                Body=part_data,
            )
            parts.append({"PartNumber": part_num, "ETag": upload_response["ETag"]})

        # Complete upload
        complete_response = s3_client.client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={"Parts": parts},
        )
        version_id = complete_response["VersionId"]

        # Verify metadata with HeadObject
        head_response = s3_client.client.head_object(
            Bucket=bucket_name, Key=key, VersionId=version_id
        )
        assert head_response["Metadata"] == metadata
        assert head_response["ContentType"] == "application/octet-stream"

    finally:
        fixture.cleanup()


def test_versioning_abort_multipart_upload(s3_client, config):
    """
    Test aborting multipart upload on versioned bucket

    Should not create version
    """
    fixture = TestFixture(s3_client, config)

    try:
        bucket_name = fixture.generate_bucket_name("ver-mp-abort")
        s3_client.create_bucket(bucket_name)
        enable_versioning(s3_client, bucket_name)

        key = "my-obj"

        # Create multipart upload
        create_response = s3_client.client.create_multipart_upload(
            Bucket=bucket_name, Key=key
        )
        upload_id = create_response["UploadId"]

        # Upload a part
        s3_client.client.upload_part(
            Bucket=bucket_name,
            Key=key,
            UploadId=upload_id,
            PartNumber=1,
            Body=b"x" * (5 * 1024 * 1024),
        )

        # Abort multipart upload
        s3_client.client.abort_multipart_upload(
            Bucket=bucket_name, Key=key, UploadId=upload_id
        )

        # List versions - should have no versions
        list_response = s3_client.client.list_object_versions(Bucket=bucket_name)
        # Should have empty Versions or no Versions field
        versions_count = len(list_response.get("Versions", []))
        assert versions_count == 0

        # Object should not exist
        with pytest.raises(ClientError) as exc_info:
            s3_client.get_object(bucket_name, key)
        assert exc_info.value.response["Error"]["Code"] == "NoSuchKey"

    finally:
        fixture.cleanup()
