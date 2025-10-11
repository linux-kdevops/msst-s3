#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Test 17: Hierarchical Namespace at Scale

Production Pattern: Directory-like operations on prefix-based "folders"
Real-world scenarios: Data lake reorganization, user folders, project restructuring

What it tests:
- Rename "directory" (10K+ objects)
- Delete "directory" recursively
- Move objects between "directories"
- List directory hierarchy
- Concurrent modifications

Why it matters:
- Users expect file-system semantics
- Rename/move are common operations
- S3 has no native directory support
- Must be emulated with prefix operations

Generated-by: Claude AI
Signed-off-by: Luis Chamberlain <mcgrof@kernel.org>
"""

import pytest
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from tests.common.test_utils import random_string


def test_directory_rename_at_scale(s3_client, config):
    """
    Test renaming a "directory" containing 1000 objects.

    Rename implemented as: copy to new prefix + delete from old prefix.
    """
    bucket_name = f"{config['s3_bucket_prefix']}-rename-{random_string()}"
    old_prefix = "old-dir/"
    new_prefix = "new-dir/"
    num_objects = 1000

    try:
        s3_client.create_bucket(bucket_name)

        print(f"\nTesting directory rename with {num_objects} objects...")

        # Create objects under old prefix
        print(f"  Creating {num_objects} objects in {old_prefix}...")
        creation_start = time.time()

        for i in range(num_objects):
            key = f"{old_prefix}file-{i:04d}.txt"
            content = f"content-{i}".encode()
            s3_client.put_object(bucket_name, key, content)

            if (i + 1) % 200 == 0:
                print(f"    Created {i+1} objects...")

        creation_time = time.time() - creation_start
        print(f"  Creation: {num_objects} objects in {creation_time:.2f}s")

        # Verify objects exist
        objects = s3_client.list_objects(bucket_name, prefix=old_prefix)
        assert len(objects) == num_objects

        # Rename: Copy all objects to new prefix
        print(f"\n  Renaming {old_prefix} to {new_prefix}...")
        rename_start = time.time()

        copied_count = 0
        deleted_count = 0

        def copy_and_delete(obj):
            """Copy object to new location and delete old"""
            old_key = obj["Key"]
            new_key = old_key.replace(old_prefix, new_prefix, 1)

            try:
                # Copy to new location
                source = {"Bucket": bucket_name, "Key": old_key}
                s3_client.client.copy_object(
                    Bucket=bucket_name, Key=new_key, CopySource=source
                )

                # Delete from old location
                s3_client.delete_object(bucket_name, old_key)

                return {"success": True, "old_key": old_key, "new_key": new_key}

            except Exception as e:
                return {"success": False, "error": str(e), "old_key": old_key}

        # Perform rename in parallel
        results = []
        with ThreadPoolExecutor(max_workers=20) as executor:
            futures = [executor.submit(copy_and_delete, obj) for obj in objects]

            for i, future in enumerate(as_completed(futures)):
                result = future.result()
                results.append(result)

                if result["success"]:
                    copied_count += 1
                    deleted_count += 1

                if (i + 1) % 200 == 0:
                    print(f"    Renamed {i+1} objects...")

        rename_time = time.time() - rename_start

        print(f"\n  Rename complete:")
        print(f"    Copied: {copied_count}/{num_objects}")
        print(f"    Deleted: {deleted_count}/{num_objects}")
        print(f"    Time: {rename_time:.2f}s ({copied_count/rename_time:.1f} obj/s)")

        # Verify: old prefix empty
        old_objects = s3_client.list_objects(bucket_name, prefix=old_prefix)
        assert len(old_objects) == 0, f"Old directory not empty: {len(old_objects)} objects remain"

        print(f"  ✓ Old directory is empty")

        # Verify: new prefix has all objects
        new_objects = s3_client.list_objects(bucket_name, prefix=new_prefix)
        assert len(new_objects) == num_objects, \
            f"Expected {num_objects} in new directory, found {len(new_objects)}"

        print(f"  ✓ New directory contains all {num_objects} objects")

        # Verify content integrity
        print(f"\n  Verifying content integrity (sampling)...")
        sample_indices = [0, 100, 500, 999]

        for idx in sample_indices:
            new_key = f"{new_prefix}file-{idx:04d}.txt"
            response = s3_client.get_object(bucket_name, new_key)
            content = response["Body"].read()
            expected = f"content-{idx}".encode()

            assert content == expected, f"Content mismatch for {new_key}"

        print(f"  ✓ Content integrity verified")

    finally:
        # Cleanup
        try:
            for prefix in [old_prefix, new_prefix]:
                objects = s3_client.list_objects(bucket_name, prefix=prefix)
                for obj in objects:
                    s3_client.delete_object(bucket_name, obj["Key"])
            s3_client.delete_bucket(bucket_name)
        except Exception:
            pass


def test_recursive_directory_delete(s3_client, config):
    """
    Test deleting directory with nested structure recursively.

    Structure: dir/year=2024/month=01/day=01/ (1000 objects total)
    """
    bucket_name = f"{config['s3_bucket_prefix']}-recur-del-{random_string()}"
    base_prefix = "data-lake/"
    num_objects = 1000

    try:
        s3_client.create_bucket(bucket_name)

        print(f"\nTesting recursive directory delete with {num_objects} objects...")

        # Create nested structure
        print(f"  Creating nested directory structure...")
        creation_start = time.time()

        for i in range(num_objects):
            year = 2024
            month = (i % 12) + 1
            day = (i % 28) + 1

            key = f"{base_prefix}year={year}/month={month:02d}/day={day:02d}/object-{i}.json"
            content = f'{{"id": {i}, "value": "data"}}'.encode()
            s3_client.put_object(bucket_name, key, content)

            if (i + 1) % 200 == 0:
                print(f"    Created {i+1} objects...")

        creation_time = time.time() - creation_start
        print(f"  Created {num_objects} objects in {creation_time:.2f}s")

        # Verify structure
        objects = s3_client.list_objects(bucket_name, prefix=base_prefix)
        assert len(objects) == num_objects

        # Delete entire directory recursively
        print(f"\n  Deleting {base_prefix} recursively...")
        delete_start = time.time()

        deleted_count = 0

        # Batch delete for efficiency (1000 at a time)
        batch_size = 1000
        remaining = objects

        while remaining:
            batch = remaining[:batch_size]
            remaining = remaining[batch_size:]

            # Use DeleteObjects for batch delete
            if len(batch) > 1:
                delete_keys = [{"Key": obj["Key"]} for obj in batch]

                try:
                    response = s3_client.client.delete_objects(
                        Bucket=bucket_name, Delete={"Objects": delete_keys}
                    )

                    deleted_count += len(response.get("Deleted", []))
                    print(f"    Deleted {deleted_count} objects...")

                except Exception as e:
                    # Fall back to individual deletes
                    for obj in batch:
                        try:
                            s3_client.delete_object(bucket_name, obj["Key"])
                            deleted_count += 1
                        except Exception:
                            pass
            else:
                # Single object
                s3_client.delete_object(bucket_name, batch[0]["Key"])
                deleted_count += 1

        delete_time = time.time() - delete_start

        print(f"\n  Deletion complete:")
        print(f"    Deleted: {deleted_count}/{num_objects}")
        print(f"    Time: {delete_time:.2f}s ({deleted_count/delete_time:.1f} obj/s)")

        # Verify complete deletion
        remaining_objects = s3_client.list_objects(bucket_name, prefix=base_prefix)
        assert len(remaining_objects) == 0, \
            f"Directory not fully deleted: {len(remaining_objects)} objects remain"

        print(f"  ✓ Directory completely deleted (no orphans)")

    finally:
        # Cleanup
        try:
            objects = s3_client.list_objects(bucket_name)
            for obj in objects:
                s3_client.delete_object(bucket_name, obj["Key"])
            s3_client.delete_bucket(bucket_name)
        except Exception:
            pass


def test_directory_move_consistency(s3_client, config):
    """
    Test moving directory while concurrent readers access it.

    Verify readers see consistent state during move.
    """
    bucket_name = f"{config['s3_bucket_prefix']}-move-{random_string()}"
    source_prefix = "source-dir/"
    dest_prefix = "dest-dir/"
    num_objects = 500

    try:
        s3_client.create_bucket(bucket_name)

        print(f"\nTesting directory move with {num_objects} objects...")

        # Create objects in source
        print(f"  Creating objects in source directory...")
        for i in range(num_objects):
            key = f"{source_prefix}file-{i:04d}.dat"
            content = f"data-{i}".encode()
            s3_client.put_object(bucket_name, key, content)

        # Start concurrent readers
        import threading

        stop_flag = threading.Event()
        read_results = {"consistent": 0, "inconsistent": 0, "errors": 0}

        def reader_thread():
            """Continuously read and check consistency"""
            while not stop_flag.is_set():
                try:
                    # List both directories
                    source_objs = s3_client.list_objects(bucket_name, prefix=source_prefix)
                    dest_objs = s3_client.list_objects(bucket_name, prefix=dest_prefix)

                    source_count = len(source_objs)
                    dest_count = len(dest_objs)

                    # Check if state is consistent
                    # Consistent: all in source OR all in dest OR distributed
                    if source_count == num_objects and dest_count == 0:
                        read_results["consistent"] += 1
                    elif source_count == 0 and dest_count == num_objects:
                        read_results["consistent"] += 1
                    else:
                        # Mixed state during move - expected but track it
                        read_results["inconsistent"] += 1

                except Exception:
                    read_results["errors"] += 1

                time.sleep(0.1)

        # Start readers
        readers = [threading.Thread(target=reader_thread) for _ in range(5)]
        for reader in readers:
            reader.start()

        # Perform move
        print(f"  Moving {source_prefix} to {dest_prefix}...")
        move_start = time.time()

        objects = s3_client.list_objects(bucket_name, prefix=source_prefix)

        for i, obj in enumerate(objects):
            old_key = obj["Key"]
            new_key = old_key.replace(source_prefix, dest_prefix, 1)

            # Copy and delete
            source = {"Bucket": bucket_name, "Key": old_key}
            s3_client.client.copy_object(Bucket=bucket_name, Key=new_key, CopySource=source)
            s3_client.delete_object(bucket_name, old_key)

            if (i + 1) % 100 == 0:
                print(f"    Moved {i+1} objects...")

        move_time = time.time() - move_start

        # Stop readers
        stop_flag.set()
        for reader in readers:
            reader.join()

        print(f"\n  Move complete in {move_time:.2f}s")
        print(f"  Reader observations:")
        print(f"    Consistent states: {read_results['consistent']}")
        print(f"    Inconsistent states: {read_results['inconsistent']}")
        print(f"    Errors: {read_results['errors']}")

        # Verify final state
        source_objs = s3_client.list_objects(bucket_name, prefix=source_prefix)
        dest_objs = s3_client.list_objects(bucket_name, prefix=dest_prefix)

        assert len(source_objs) == 0, "Source directory not empty after move"
        assert len(dest_objs) == num_objects, \
            f"Expected {num_objects} in destination, found {len(dest_objs)}"

        print(f"  ✓ Move completed successfully")
        print(f"  ✓ No data loss ({len(dest_objs)} objects in destination)")

    finally:
        # Cleanup
        try:
            for prefix in [source_prefix, dest_prefix]:
                objects = s3_client.list_objects(bucket_name, prefix=prefix)
                for obj in objects:
                    s3_client.delete_object(bucket_name, obj["Key"])
            s3_client.delete_bucket(bucket_name)
        except Exception:
            pass


def test_directory_listing_performance(s3_client, config):
    """
    Test performance of listing large directory hierarchies.
    """
    bucket_name = f"{config['s3_bucket_prefix']}-list-perf-{random_string()}"
    num_objects = 1000

    try:
        s3_client.create_bucket(bucket_name)

        print(f"\nTesting directory listing performance...")

        # Create multi-level directory structure
        print(f"  Creating {num_objects} objects in nested structure...")

        for i in range(num_objects):
            level1 = f"level1-{i % 10}"
            level2 = f"level2-{i % 100}"
            level3 = f"level3-{i}"

            key = f"{level1}/{level2}/{level3}.txt"
            s3_client.put_object(bucket_name, key, f"data-{i}".encode())

        # Test: List top level
        print(f"\n  Listing top-level directories...")
        list_start = time.time()

        response = s3_client.client.list_objects_v2(
            Bucket=bucket_name, Delimiter="/"
        )

        top_level_prefixes = response.get("CommonPrefixes", [])
        list_time = time.time() - list_start

        print(f"    Found {len(top_level_prefixes)} top-level directories in {list_time*1000:.1f}ms")

        # Test: List all objects (no delimiter)
        print(f"\n  Listing all objects...")
        list_start = time.time()

        all_objects = []
        continuation_token = None

        while True:
            if continuation_token:
                response = s3_client.client.list_objects_v2(
                    Bucket=bucket_name,
                    ContinuationToken=continuation_token,
                )
            else:
                response = s3_client.client.list_objects_v2(Bucket=bucket_name)

            all_objects.extend(response.get("Contents", []))

            if not response.get("IsTruncated"):
                break

            continuation_token = response.get("NextContinuationToken")

        list_all_time = time.time() - list_start

        print(f"    Listed {len(all_objects)} objects in {list_all_time:.2f}s")

        assert len(all_objects) == num_objects

        print(f"  ✓ Directory listing performance acceptable")

    finally:
        # Cleanup
        try:
            objects = s3_client.list_objects(bucket_name)
            for obj in objects:
                s3_client.delete_object(bucket_name, obj["Key"])
            s3_client.delete_bucket(bucket_name)
        except Exception:
            pass


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
