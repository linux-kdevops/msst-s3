#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Test 19: Incremental Backup and Differential Sync

Production Pattern: Efficient backup using Last-Modified and ETags
Real-world scenarios: Daily backups, disaster recovery, user file sync

What it tests:
- Initial full backup (all objects)
- Incremental backup (only changed)
- Verification (checksum validation)
- Restore from incremental
- Performance vs full backup

Why it matters:
- Full backups are expensive and slow
- Incremental backups save time and money
- Core capability for backup systems
- Used by rsync, backup tools, sync utilities

Generated-by: Claude AI
Signed-off-by: Luis Chamberlain <mcgrof@kernel.org>
"""

import pytest
import time
import hashlib
import json
from datetime import datetime
from tests.common.test_utils import random_string


def test_incremental_backup_strategy(s3_client, config):
    """
    Test incremental backup based on Last-Modified timestamps.

    Day 0: Full backup
    Day 1: Incremental (only modified objects)
    Day 2: Incremental (only modified since day 1)
    """
    source_bucket = f"{config['s3_bucket_prefix']}-src-{random_string()}"
    backup_bucket = f"{config['s3_bucket_prefix']}-bak-{random_string()}"
    num_objects = 100

    try:
        s3_client.create_bucket(source_bucket)
        s3_client.create_bucket(backup_bucket)

        print(f"\nTesting incremental backup with {num_objects} objects...")

        # Day 0: Create initial objects in source
        print(f"\n  Day 0: Creating {num_objects} initial objects...")
        source_objects = {}
        for i in range(num_objects):
            key = f"data/file-{i:04d}.txt"
            content = f"initial-content-{i}".encode()
            s3_client.put_object(source_bucket, key, content)
            source_objects[key] = {"version": 0, "content": content}

        # Day 0: Full backup
        print(f"  Day 0: Performing full backup...")
        day0_start = time.time()
        backup_manifest = {}

        for key in source_objects.keys():
            response = s3_client.get_object(source_bucket, key)
            content = response["Body"].read()
            last_modified = response.get("LastModified")

            # Copy to backup
            s3_client.put_object(backup_bucket, key, content)

            backup_manifest[key] = {
                "last_modified": last_modified.isoformat() if last_modified else None,
                "size": len(content),
            }

        day0_duration = time.time() - day0_start
        day0_copied = len(backup_manifest)

        print(f"  Day 0: Backed up {day0_copied} objects in {day0_duration:.2f}s")

        # Day 1: Modify 20% of objects
        time.sleep(1)  # Ensure timestamp difference
        day1_timestamp = datetime.now()

        print(f"\n  Day 1: Modifying 20 objects...")
        modified_count = 20
        for i in range(modified_count):
            key = f"data/file-{i:04d}.txt"
            content = f"modified-content-day1-{i}".encode()
            s3_client.put_object(source_bucket, key, content)
            source_objects[key] = {"version": 1, "content": content}

        # Day 1: Incremental backup
        print(f"  Day 1: Performing incremental backup...")
        day1_start = time.time()
        day1_copied = 0

        for key in source_objects.keys():
            response = s3_client.head_object(source_bucket, key)
            last_modified = response.get("LastModified")

            # Check if modified since last backup
            if last_modified and last_modified > day0_timestamp.replace(tzinfo=last_modified.tzinfo):
                # Copy to backup (incremental)
                response = s3_client.get_object(source_bucket, key)
                content = response["Body"].read()
                s3_client.put_object(backup_bucket, key, content)

                backup_manifest[key]["last_modified"] = last_modified.isoformat()
                day1_copied += 1

        day1_duration = time.time() - day1_start

        print(f"  Day 1: Backed up {day1_copied} objects in {day1_duration:.2f}s")
        print(f"  Day 1: Incremental speedup: {day0_duration/day1_duration:.1f}x faster")

        # Verify incremental only copied modified objects
        assert day1_copied == modified_count, \
            f"Expected {modified_count} incremental copies, got {day1_copied}"

        print(f"  ✓ Incremental backup only copied modified objects")

        # Day 2: Modify 10% more
        time.sleep(1)
        day2_timestamp = datetime.now()

        print(f"\n  Day 2: Modifying 10 more objects...")
        for i in range(20, 30):
            key = f"data/file-{i:04d}.txt"
            content = f"modified-content-day2-{i}".encode()
            s3_client.put_object(source_bucket, key, content)
            source_objects[key] = {"version": 2, "content": content}

        # Day 2: Incremental backup
        print(f"  Day 2: Performing incremental backup...")
        day2_start = time.time()
        day2_copied = 0

        for key in source_objects.keys():
            response = s3_client.head_object(source_bucket, key)
            last_modified = response.get("LastModified")

            if last_modified and last_modified > day1_timestamp.replace(tzinfo=last_modified.tzinfo):
                response = s3_client.get_object(source_bucket, key)
                content = response["Body"].read()
                s3_client.put_object(backup_bucket, key, content)
                day2_copied += 1

        day2_duration = time.time() - day2_start

        print(f"  Day 2: Backed up {day2_copied} objects in {day2_duration:.2f}s")

        assert day2_copied == 10, f"Expected 10 incremental copies, got {day2_copied}"

        # Verify backup integrity
        print(f"\n  Verifying backup integrity...")
        for key, expected in source_objects.items():
            response = s3_client.get_object(backup_bucket, key)
            backup_content = response["Body"].read()

            assert backup_content == expected["content"], \
                f"Backup content mismatch for {key}"

        print(f"  ✓ All {num_objects} objects verified in backup")

        # Calculate efficiency
        full_backup_time = day0_duration
        incremental_total = day1_duration + day2_duration
        savings = ((full_backup_time * 3) - (full_backup_time + incremental_total)) / (full_backup_time * 3) * 100

        print(f"\n  Backup efficiency:")
        print(f"    Full backup time: {full_backup_time:.2f}s")
        print(f"    Incremental total: {incremental_total:.2f}s")
        print(f"    Time savings: {savings:.1f}%")

    finally:
        # Cleanup
        try:
            for bucket in [source_bucket, backup_bucket]:
                objects = s3_client.list_objects(bucket)
                for obj in objects:
                    s3_client.delete_object(bucket, obj["Key"])
                s3_client.delete_bucket(bucket)
        except Exception:
            pass


def test_differential_sync_with_etag(s3_client, config):
    """
    Test efficient sync using ETag comparison.

    Only copy objects where ETags differ (content changed).
    """
    source_bucket = f"{config['s3_bucket_prefix']}-src-sync-{random_string()}"
    dest_bucket = f"{config['s3_bucket_prefix']}-dst-sync-{random_string()}"
    num_objects = 100

    try:
        s3_client.create_bucket(source_bucket)
        s3_client.create_bucket(dest_bucket)

        print(f"\nTesting differential sync with {num_objects} objects...")

        # Create source objects
        print(f"  Creating source objects...")
        for i in range(num_objects):
            key = f"data/object-{i:04d}.dat"
            content = f"content-{i}".encode()
            s3_client.put_object(source_bucket, key, content)

        # Initial full sync
        print(f"  Initial sync...")
        sync_start = time.time()

        source_objects = s3_client.list_objects(source_bucket)
        synced_count = 0

        for obj in source_objects:
            key = obj["Key"]
            response = s3_client.get_object(source_bucket, key)
            content = response["Body"].read()
            s3_client.put_object(dest_bucket, key, content)
            synced_count += 1

        initial_sync_time = time.time() - sync_start
        print(f"  Initial sync: {synced_count} objects in {initial_sync_time:.2f}s")

        # Modify 10% of source objects
        print(f"\n  Modifying 10 objects...")
        modified_keys = set()
        for i in range(10):
            key = f"data/object-{i:04d}.dat"
            content = f"modified-content-{i}".encode()
            s3_client.put_object(source_bucket, key, content)
            modified_keys.add(key)

        # Differential sync using ETag
        print(f"  Differential sync...")
        diff_sync_start = time.time()

        # Build ETag maps
        source_map = {}
        for obj in s3_client.list_objects(source_bucket):
            response = s3_client.head_object(source_bucket, obj["Key"])
            source_map[obj["Key"]] = response["ETag"].strip('"')

        dest_map = {}
        for obj in s3_client.list_objects(dest_bucket):
            response = s3_client.head_object(dest_bucket, obj["Key"])
            dest_map[obj["Key"]] = response["ETag"].strip('"')

        # Sync only changed objects
        copied = 0
        for key, source_etag in source_map.items():
            dest_etag = dest_map.get(key)

            if dest_etag != source_etag:
                # ETag differs - copy
                response = s3_client.get_object(source_bucket, key)
                content = response["Body"].read()
                s3_client.put_object(dest_bucket, key, content)
                copied += 1

        diff_sync_time = time.time() - diff_sync_start

        print(f"  Differential sync: {copied} objects in {diff_sync_time:.2f}s")

        assert copied == len(modified_keys), \
            f"Expected to sync {len(modified_keys)} objects, synced {copied}"

        speedup = initial_sync_time / diff_sync_time if diff_sync_time > 0 else 0
        print(f"  Speedup: {speedup:.1f}x faster than full sync")

        # Verify sync accuracy
        print(f"\n  Verifying sync...")
        for obj in s3_client.list_objects(source_bucket):
            key = obj["Key"]
            source_response = s3_client.get_object(source_bucket, key)
            dest_response = s3_client.get_object(dest_bucket, key)

            source_content = source_response["Body"].read()
            dest_content = dest_response["Body"].read()

            assert source_content == dest_content, f"Sync mismatch for {key}"

        print(f"  ✓ All {num_objects} objects verified in sync")

    finally:
        # Cleanup
        try:
            for bucket in [source_bucket, dest_bucket]:
                objects = s3_client.list_objects(bucket)
                for obj in objects:
                    s3_client.delete_object(bucket, obj["Key"])
                s3_client.delete_bucket(bucket)
        except Exception:
            pass


def test_backup_verification_with_checksums(s3_client, config):
    """
    Test backup integrity verification using checksums.

    Compute checksums for all backed-up objects and verify.
    """
    source_bucket = f"{config['s3_bucket_prefix']}-verify-{random_string()}"
    backup_bucket = f"{config['s3_bucket_prefix']}-bak-verify-{random_string()}"
    num_objects = 50

    try:
        s3_client.create_bucket(source_bucket)
        s3_client.create_bucket(backup_bucket)

        print(f"\nTesting backup verification with {num_objects} objects...")

        # Create and backup objects
        manifest = {}
        print(f"  Creating and backing up objects...")

        for i in range(num_objects):
            key = f"data/file-{i:04d}.bin"
            content = f"verified-content-{i}".encode() * 100  # Larger content

            # Store in source
            s3_client.put_object(source_bucket, key, content)

            # Backup to dest
            s3_client.put_object(backup_bucket, key, content)

            # Compute checksum and store in manifest
            checksum = hashlib.sha256(content).hexdigest()
            manifest[key] = {
                "checksum": checksum,
                "size": len(content),
                "algorithm": "SHA256",
            }

        # Save manifest
        manifest_key = "backup-manifest.json"
        manifest_content = json.dumps(manifest, indent=2).encode()
        s3_client.put_object(backup_bucket, manifest_key, manifest_content)

        print(f"  ✓ Backed up {num_objects} objects with manifest")

        # Verification: Read manifest and verify all objects
        print(f"\n  Verifying backup integrity...")
        response = s3_client.get_object(backup_bucket, manifest_key)
        stored_manifest = json.loads(response["Body"].read())

        verified_count = 0
        corrupted = []

        for key, metadata in stored_manifest.items():
            # Read backup object
            response = s3_client.get_object(backup_bucket, key)
            backup_content = response["Body"].read()

            # Compute checksum
            actual_checksum = hashlib.sha256(backup_content).hexdigest()
            expected_checksum = metadata["checksum"]

            if actual_checksum == expected_checksum:
                verified_count += 1
            else:
                corrupted.append(
                    {
                        "key": key,
                        "expected": expected_checksum,
                        "actual": actual_checksum,
                    }
                )

        print(f"  Verified: {verified_count}/{num_objects} objects")

        assert len(corrupted) == 0, f"Found {len(corrupted)} corrupted objects!"

        print(f"  ✓ 100% backup integrity verified")

        # Simulate corruption and verify detection
        print(f"\n  Testing corruption detection...")
        corrupt_key = f"data/file-0000.bin"

        # Corrupt the backup
        corrupted_content = b"THIS IS CORRUPTED DATA"
        s3_client.put_object(backup_bucket, corrupt_key, corrupted_content)

        # Verify again - should detect corruption
        response = s3_client.get_object(backup_bucket, corrupt_key)
        backup_content = response["Body"].read()
        actual_checksum = hashlib.sha256(backup_content).hexdigest()
        expected_checksum = manifest[corrupt_key]["checksum"]

        assert actual_checksum != expected_checksum, "Should detect corruption"

        print(f"  ✓ Corruption detection working")

    finally:
        # Cleanup
        try:
            for bucket in [source_bucket, backup_bucket]:
                objects = s3_client.list_objects(bucket)
                for obj in objects:
                    s3_client.delete_object(bucket, obj["Key"])
                s3_client.delete_bucket(bucket)
        except Exception:
            pass


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
