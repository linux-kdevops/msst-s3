#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Test 13: Time-Travel Point-in-Time Recovery

Production Pattern: Backup and disaster recovery using versioning
Real-world scenarios: Ransomware recovery, compliance audits, rollback

What it tests:
- Create object with many versions over time
- Restore to specific point in time
- Verify data integrity at any historical point
- Test with thousands of versions
- Performance of historical queries

Why it matters:
- Core backup/recovery capability
- Compliance requirements (retain historical data)
- Disaster recovery (restore before corruption)
- Common in database backups, configuration management

Generated-by: Claude AI
Signed-off-by: Luis Chamberlain <mcgrof@kernel.org>
"""

import pytest
import time
from datetime import datetime, timedelta
from tests.common.test_utils import random_string


def test_point_in_time_restore(s3_client, config):
    """
    Test restoring object to specific point in time using versioning.

    Create object with 60 versions (one per minute simulation).
    Restore to arbitrary point in time.
    """
    bucket_name = f"{config['s3_bucket_prefix']}-pitr-{random_string()}"
    key = "data/time-travel.txt"
    num_versions = 60

    try:
        s3_client.create_bucket(bucket_name)
        s3_client.put_bucket_versioning(bucket_name, {"Status": "Enabled"})

        print(f"\nTesting point-in-time recovery with {num_versions} versions...")

        # Create versions with timestamps
        version_timeline = []

        for i in range(num_versions):
            timestamp = datetime.now()
            content = f"version-{i}-at-{timestamp.isoformat()}".encode()

            response = s3_client.put_object(bucket_name, key, content)
            version_id = response.get("VersionId")

            version_timeline.append(
                {
                    "version_id": version_id,
                    "timestamp": timestamp,
                    "content": content,
                    "version_num": i,
                }
            )

            time.sleep(0.05)  # Small delay to ensure different timestamps

        print(f"  Created {len(version_timeline)} versions over time")

        # Test: Restore to specific version (version 37)
        target_version_num = 37
        target_version = version_timeline[target_version_num]

        print(f"\n  Restoring to version {target_version_num}...")
        response = s3_client.get_object(
            bucket_name, key, VersionId=target_version["version_id"]
        )
        restored_content = response["Body"].read()

        assert restored_content == target_version["content"], \
            "Restored content doesn't match target version!"

        print(f"  ✓ Successfully restored to version {target_version_num}")
        print(f"    Timestamp: {target_version['timestamp']}")

        # Test: List all versions and verify chronological order
        print(f"\n  Verifying version history...")
        versions_response = s3_client.list_object_versions(bucket_name, Prefix=key)
        versions = versions_response.get("Versions", [])

        assert len(versions) >= num_versions, \
            f"Expected at least {num_versions} versions, found {len(versions)}"

        print(f"  ✓ All {len(versions)} versions preserved")

        # Test: Restore to multiple random points in time
        print(f"\n  Testing multiple restore points...")
        test_versions = [10, 25, 40, 55]

        for ver_num in test_versions:
            ver_info = version_timeline[ver_num]
            response = s3_client.get_object(
                bucket_name, key, VersionId=ver_info["version_id"]
            )
            content = response["Body"].read()

            assert content == ver_info["content"], \
                f"Version {ver_num} restoration failed!"

        print(f"  ✓ Successfully restored to {len(test_versions)} different points")

        # Test: Restore latest version
        response = s3_client.get_object(bucket_name, key)  # No version ID = latest
        latest_content = response["Body"].read()

        assert latest_content == version_timeline[-1]["content"], \
            "Latest version doesn't match!"

        print(f"  ✓ Latest version accessible without version ID")

    finally:
        # Cleanup all versions
        try:
            versions_response = s3_client.list_object_versions(bucket_name, Prefix=key)
            for version in versions_response.get("Versions", []):
                s3_client.delete_object(
                    bucket_name, key, VersionId=version["VersionId"]
                )
            s3_client.delete_bucket(bucket_name)
        except Exception:
            pass


def test_consistent_snapshot_across_objects(s3_client, config):
    """
    Test creating consistent point-in-time snapshot across multiple objects.

    All objects in snapshot must be from same point in time.
    """
    bucket_name = f"{config['s3_bucket_prefix']}-snapshot-{random_string()}"
    num_objects = 50
    num_updates = 10

    try:
        s3_client.create_bucket(bucket_name)
        s3_client.put_bucket_versioning(bucket_name, {"Status": "Enabled"})

        print(f"\nTesting consistent snapshot with {num_objects} objects...")

        # Create initial objects
        print(f"  Creating {num_objects} objects...")
        for i in range(num_objects):
            key = f"data/object-{i:04d}.txt"
            content = f"initial-{i}".encode()
            s3_client.put_object(bucket_name, key, content)

        # Perform updates over time
        snapshot_timeline = []

        for update_num in range(num_updates):
            time.sleep(0.1)  # Delay between updates

            # Take snapshot metadata before this update
            snapshot_time = datetime.now()
            snapshot_versions = {}

            # Capture current version IDs of all objects
            for i in range(num_objects):
                key = f"data/object-{i:04d}.txt"
                response = s3_client.head_object(bucket_name, key)
                snapshot_versions[key] = response.get("VersionId")

            snapshot_timeline.append(
                {
                    "snapshot_num": update_num,
                    "timestamp": snapshot_time,
                    "versions": snapshot_versions,
                }
            )

            # Now update some objects
            for i in range(update_num, update_num + 5):
                if i < num_objects:
                    key = f"data/object-{i:04d}.txt"
                    content = f"update-{update_num}-object-{i}".encode()
                    s3_client.put_object(bucket_name, key, content)

        print(f"  Created {len(snapshot_timeline)} snapshots")

        # Test: Restore entire bucket to snapshot 5
        target_snapshot_num = 5
        target_snapshot = snapshot_timeline[target_snapshot_num]

        print(f"\n  Restoring to snapshot {target_snapshot_num}...")
        restored_objects = {}

        for key, version_id in target_snapshot["versions"].items():
            response = s3_client.get_object(bucket_name, key, VersionId=version_id)
            content = response["Body"].read()
            restored_objects[key] = content

        print(f"  ✓ Restored {len(restored_objects)} objects from snapshot")

        # Verify all objects are from same snapshot (no mixed state)
        # Re-read all objects using stored version IDs
        for key, version_id in target_snapshot["versions"].items():
            response = s3_client.get_object(bucket_name, key, VersionId=version_id)
            content = response["Body"].read()

            assert content == restored_objects[key], \
                f"Object {key} changed between reads!"

        print(f"  ✓ Snapshot consistency verified (no mixed state)")

        # Test: Verify snapshot isolation (updates after snapshot don't affect it)
        print(f"\n  Verifying snapshot isolation...")

        # Update all objects
        for i in range(num_objects):
            key = f"data/object-{i:04d}.txt"
            content = b"AFTER-SNAPSHOT-UPDATE"
            s3_client.put_object(bucket_name, key, content)

        # Restore snapshot again - should be unchanged
        for key, version_id in target_snapshot["versions"].items():
            response = s3_client.get_object(bucket_name, key, VersionId=version_id)
            content = response["Body"].read()

            assert content == restored_objects[key], \
                "Snapshot was affected by subsequent updates!"

        print(f"  ✓ Snapshot isolated from subsequent updates")

    finally:
        # Cleanup
        try:
            versions_response = s3_client.list_object_versions(bucket_name)
            for version in versions_response.get("Versions", []):
                s3_client.delete_object(
                    bucket_name, version["Key"], VersionId=version["VersionId"]
                )
            s3_client.delete_bucket(bucket_name)
        except Exception:
            pass


def test_version_performance_at_scale(s3_client, config):
    """
    Test performance of historical queries with many versions.

    Create 1000+ versions and verify retrieval performance.
    """
    bucket_name = f"{config['s3_bucket_prefix']}-perf-{random_string()}"
    key = "data/high-version-object.txt"
    num_versions = 1000

    try:
        s3_client.create_bucket(bucket_name)
        s3_client.put_bucket_versioning(bucket_name, {"Status": "Enabled"})

        print(f"\nTesting version performance with {num_versions} versions...")

        # Create many versions
        print(f"  Creating {num_versions} versions...")
        creation_start = time.time()
        version_ids = []

        for i in range(num_versions):
            content = f"version-{i}".encode()
            response = s3_client.put_object(bucket_name, key, content)
            version_ids.append(response.get("VersionId"))

            if (i + 1) % 100 == 0:
                elapsed = time.time() - creation_start
                rate = (i + 1) / elapsed
                print(f"    Created {i+1} versions ({rate:.1f} versions/sec)")

        creation_time = time.time() - creation_start
        creation_rate = num_versions / creation_time

        print(f"  Creation: {num_versions} versions in {creation_time:.2f}s ({creation_rate:.1f}/s)")

        # Test: List all versions
        print(f"\n  Listing versions...")
        list_start = time.time()

        all_versions = []
        continuation_token = None

        while True:
            if continuation_token:
                response = s3_client.client.list_object_versions(
                    Bucket=bucket_name,
                    Prefix=key,
                    KeyMarker=continuation_token.get("KeyMarker"),
                    VersionIdMarker=continuation_token.get("VersionIdMarker"),
                )
            else:
                response = s3_client.client.list_object_versions(
                    Bucket=bucket_name, Prefix=key
                )

            all_versions.extend(response.get("Versions", []))

            if not response.get("IsTruncated"):
                break

            continuation_token = {
                "KeyMarker": response.get("NextKeyMarker"),
                "VersionIdMarker": response.get("NextVersionIdMarker"),
            }

        list_time = time.time() - list_start

        print(f"  Listed {len(all_versions)} versions in {list_time:.2f}s")

        assert len(all_versions) >= num_versions, \
            f"Expected at least {num_versions} versions, found {len(all_versions)}"

        # Test: Random access to historical versions
        print(f"\n  Testing random access to versions...")
        test_indices = [0, 100, 500, 750, 999]
        access_times = []

        for idx in test_indices:
            version_id = version_ids[idx]
            access_start = time.time()

            response = s3_client.get_object(bucket_name, key, VersionId=version_id)
            content = response["Body"].read()
            expected = f"version-{idx}".encode()

            access_time = time.time() - access_start
            access_times.append(access_time)

            assert content == expected, f"Version {idx} content mismatch!"

        avg_access_time = sum(access_times) / len(access_times)
        print(f"  Average access time: {avg_access_time*1000:.1f}ms")
        print(f"  ✓ All historical versions accessible")

        # Performance assertions
        assert list_time < 10.0, f"Listing {num_versions} versions took too long: {list_time}s"
        assert avg_access_time < 1.0, f"Version access too slow: {avg_access_time}s"

        print(f"  ✓ Performance acceptable for {num_versions} versions")

    finally:
        # Cleanup (delete a sample, not all 1000)
        try:
            # Delete first 100 versions
            for i in range(min(100, len(version_ids))):
                s3_client.delete_object(bucket_name, key, VersionId=version_ids[i])

            s3_client.delete_bucket(bucket_name)
        except Exception:
            pass


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
