#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Test 2: Multipart Upload Orphan Management

Production Pattern: Cloud cost optimization - detecting and cleaning up incomplete uploads
Real-world scenarios: Failed uploads, interrupted jobs, cost management

What it tests:
- Creating many multipart uploads in different states
- Orphan detection via ListMultipartUploads
- Cleanup of abandoned parts
- Storage cost implications

Why it matters:
- Incomplete uploads consume storage and cost money
- Production systems need orphan detection and cleanup
- Critical for cloud cost management
- Common source of unexpected storage bills

Generated-by: Claude AI
Signed-off-by: Luis Chamberlain <mcgrof@kernel.org>
"""

import pytest
import time
from concurrent.futures import ThreadPoolExecutor
from tests.common.test_utils import random_string


def test_multipart_orphan_detection(s3_client, config):
    """
    Create 50 multipart uploads in various states:
    - 15 completed (should NOT appear in ListMultipartUploads)
    - 15 aborted (should NOT appear)
    - 20 abandoned (should appear - these are orphans)

    Verify ListMultipartUploads correctly identifies orphans.
    """
    bucket_name = f"{config['s3_bucket_prefix']}-orphan-{random_string()}"
    num_completed = 15
    num_aborted = 15
    num_abandoned = 20
    total_uploads = num_completed + num_aborted + num_abandoned

    try:
        # Create bucket
        s3_client.create_bucket(bucket_name)

        upload_ids = {"completed": [], "aborted": [], "abandoned": []}

        print(f"\nCreating {total_uploads} multipart uploads...")

        # Create completed uploads
        for i in range(num_completed):
            key = f"completed/file-{i}.dat"
            upload_id = s3_client.create_multipart_upload(bucket_name, key)

            # Upload a part
            part_response = s3_client.upload_part(
                bucket_name, key, upload_id, 1, b"test data for completed upload"
            )

            # Complete the upload
            s3_client.complete_multipart_upload(
                bucket_name,
                key,
                upload_id,
                [{"PartNumber": 1, "ETag": part_response["ETag"]}],
            )
            upload_ids["completed"].append(upload_id)

        # Create aborted uploads
        for i in range(num_aborted):
            key = f"aborted/file-{i}.dat"
            upload_id = s3_client.create_multipart_upload(bucket_name, key)

            # Upload a part
            s3_client.upload_part(
                bucket_name, key, upload_id, 1, b"test data for aborted upload"
            )

            # Abort the upload
            s3_client.abort_multipart_upload(bucket_name, key, upload_id)
            upload_ids["aborted"].append(upload_id)

        # Create abandoned uploads (orphans)
        for i in range(num_abandoned):
            key = f"abandoned/file-{i}.dat"
            upload_id = s3_client.create_multipart_upload(bucket_name, key)

            # Some with parts uploaded, some without
            if i % 2 == 0:
                s3_client.upload_part(
                    bucket_name, key, upload_id, 1, b"orphaned data" * 1000  # ~12KB
                )

            upload_ids["abandoned"].append(upload_id)

        print(
            f"  Created: {num_completed} completed, {num_aborted} aborted, {num_abandoned} abandoned"
        )

        # List multipart uploads to find orphans
        orphans = []
        response = s3_client.client.list_multipart_uploads(Bucket=bucket_name)

        while True:
            for upload in response.get("Uploads", []):
                orphans.append(upload)

            if not response.get("IsTruncated"):
                break

            response = s3_client.client.list_multipart_uploads(
                Bucket=bucket_name,
                KeyMarker=response.get("NextKeyMarker"),
                UploadIdMarker=response.get("NextUploadIdMarker"),
            )

        print(f"  ListMultipartUploads found {len(orphans)} orphaned uploads")

        # Verify orphan count matches abandoned uploads
        assert (
            len(orphans) == num_abandoned
        ), f"Expected {num_abandoned} orphans, found {len(orphans)}"

        # Verify completed and aborted uploads are NOT in the list
        orphan_upload_ids = [o["UploadId"] for o in orphans]

        for completed_id in upload_ids["completed"]:
            assert (
                completed_id not in orphan_upload_ids
            ), "Completed upload should not appear in orphan list"

        for aborted_id in upload_ids["aborted"]:
            assert (
                aborted_id not in orphan_upload_ids
            ), "Aborted upload should not appear in orphan list"

        print(f"  ✓ Orphan detection correct: {num_abandoned} orphans identified")

        # Cleanup orphans
        cleanup_count = 0
        for orphan in orphans:
            try:
                s3_client.abort_multipart_upload(
                    bucket_name, orphan["Key"], orphan["UploadId"]
                )
                cleanup_count += 1
            except Exception as e:
                print(f"  Warning: Failed to cleanup orphan: {e}")

        print(f"  ✓ Cleaned up {cleanup_count}/{len(orphans)} orphans")

        # Verify cleanup - list should now be empty
        response = s3_client.client.list_multipart_uploads(Bucket=bucket_name)
        remaining = response.get("Uploads", [])

        assert (
            len(remaining) == 0
        ), f"Expected 0 uploads after cleanup, found {len(remaining)}"

        print(f"  ✓ All orphans successfully cleaned up")

    finally:
        # Cleanup
        try:
            # Delete completed objects
            for i in range(num_completed):
                s3_client.delete_object(bucket_name, f"completed/file-{i}.dat")

            # Abort any remaining uploads
            response = s3_client.client.list_multipart_uploads(Bucket=bucket_name)
            for upload in response.get("Uploads", []):
                s3_client.abort_multipart_upload(
                    bucket_name, upload["Key"], upload["UploadId"]
                )

            s3_client.delete_bucket(bucket_name)
        except Exception:
            pass


def test_large_part_orphan_storage_calculation(s3_client, config):
    """
    Test storage cost implications of orphaned large parts.

    Create orphaned uploads with large parts (100MB+) and verify
    we can detect and quantify wasted storage.
    """
    bucket_name = f"{config['s3_bucket_prefix']}-large-orphan-{random_string()}"
    num_orphans = 5
    part_size_mb = 10  # Use 10MB for testing (would be 100MB+ in production)

    try:
        s3_client.create_bucket(bucket_name)

        orphan_info = []

        print(
            f"\nCreating {num_orphans} orphaned uploads with {part_size_mb}MB parts..."
        )

        for i in range(num_orphans):
            key = f"large-orphan/file-{i}.dat"
            upload_id = s3_client.create_multipart_upload(bucket_name, key)

            # Upload 3 parts of size part_size_mb each
            num_parts = 3
            for part_num in range(1, num_parts + 1):
                part_data = b"X" * (part_size_mb * 1024 * 1024)
                s3_client.upload_part(bucket_name, key, upload_id, part_num, part_data)

            orphan_info.append(
                {
                    "key": key,
                    "upload_id": upload_id,
                    "parts": num_parts,
                    "total_size_mb": num_parts * part_size_mb,
                }
            )

        total_wasted_mb = sum(o["total_size_mb"] for o in orphan_info)
        print(
            f"  Total wasted storage: {total_wasted_mb}MB across {num_orphans} uploads"
        )

        # List parts for each upload to verify they exist
        for orphan in orphan_info:
            parts_response = s3_client.client.list_parts(
                Bucket=bucket_name, Key=orphan["key"], UploadId=orphan["upload_id"]
            )

            parts = parts_response.get("Parts", [])
            assert (
                len(parts) == orphan["parts"]
            ), f"Expected {orphan['parts']} parts, found {len(parts)}"

            # Calculate actual storage from parts
            total_size = sum(p.get("Size", 0) for p in parts)
            total_size_mb = total_size / (1024 * 1024)

            print(
                f"  Upload {orphan['key']}: {len(parts)} parts, {total_size_mb:.1f}MB"
            )

        print(f"  ✓ All orphaned parts verified")

        # Cleanup and measure
        cleanup_start = time.time()
        for orphan in orphan_info:
            s3_client.abort_multipart_upload(
                bucket_name, orphan["key"], orphan["upload_id"]
            )
        cleanup_duration = time.time() - cleanup_start

        print(f"  ✓ Cleanup completed in {cleanup_duration:.2f}s")
        print(f"  ✓ Reclaimed {total_wasted_mb}MB of storage")

    finally:
        try:
            # Cleanup any remaining
            response = s3_client.client.list_multipart_uploads(Bucket=bucket_name)
            for upload in response.get("Uploads", []):
                s3_client.abort_multipart_upload(
                    bucket_name, upload["Key"], upload["UploadId"]
                )
            s3_client.delete_bucket(bucket_name)
        except Exception:
            pass


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
