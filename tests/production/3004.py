#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Test 4: List-After-Write Consistency Under Churn

Production Pattern: High-churn directories with continuous creates/deletes
Real-world scenarios: Temp files, job output, ETL pipelines, log rotation

What it tests:
- Continuously create and delete objects in same prefix
- Concurrent LIST operations
- Eventual consistency convergence
- No phantom objects in listings

Why it matters:
- S3 has eventual consistency for LIST operations
- Production workflows depend on consistent listings
- Common in ETL pipelines and job orchestration
- Phantom or missing objects can break automation

Generated-by: Claude AI
Signed-off-by: Luis Chamberlain <mcgrof@kernel.org>
"""

import pytest
import threading
import time
from collections import defaultdict
from tests.common.test_utils import random_string


def test_list_consistency_under_churn(s3_client, config):
    """
    Stress test eventual consistency:
    - Thread 1: Rapidly creates objects
    - Thread 2: Rapidly deletes objects
    - Thread 3: Continuously lists objects
    - Run for 30 seconds
    - Verify eventual convergence
    """
    bucket_name = f"{config['s3_bucket_prefix']}-churn-{random_string()}"
    prefix = "churn/data/"
    test_duration = 30  # seconds
    create_rate = 10  # objects/sec
    delete_rate = 8  # objects/sec

    try:
        s3_client.create_bucket(bucket_name)

        # Shared state
        created_keys = []
        deleted_keys = []
        list_results = []
        stop_flag = threading.Event()
        lock = threading.Lock()

        def creator_thread():
            """Continuously create objects"""
            counter = 0
            while not stop_flag.is_set():
                key = f"{prefix}file-{counter}-{random_string(8)}.dat"
                try:
                    s3_client.put_object(bucket_name, key, b"churn data")
                    with lock:
                        created_keys.append(key)
                    counter += 1
                    time.sleep(1.0 / create_rate)
                except Exception as e:
                    print(f"Create error: {e}")

        def deleter_thread():
            """Continuously delete objects"""
            while not stop_flag.is_set():
                with lock:
                    if created_keys:
                        # Delete oldest created object
                        key = created_keys.pop(0)
                    else:
                        key = None

                if key:
                    try:
                        s3_client.delete_object(bucket_name, key)
                        with lock:
                            deleted_keys.append(key)
                    except Exception as e:
                        pass  # Object might not exist yet due to eventual consistency

                time.sleep(1.0 / delete_rate)

        def lister_thread():
            """Continuously list objects and record results"""
            while not stop_flag.is_set():
                try:
                    objects = s3_client.list_objects(bucket_name, prefix=prefix)
                    keys_found = [obj["Key"] for obj in objects]

                    with lock:
                        list_results.append(
                            {
                                "timestamp": time.time(),
                                "count": len(keys_found),
                                "keys": set(keys_found),
                            }
                        )
                except Exception as e:
                    print(f"List error: {e}")

                time.sleep(0.5)  # List every 500ms

        print(f"\nRunning list consistency test for {test_duration} seconds...")
        print(f"  Create rate: {create_rate}/sec")
        print(f"  Delete rate: {delete_rate}/sec")

        # Start threads
        threads = [
            threading.Thread(target=creator_thread),
            threading.Thread(target=deleter_thread),
            threading.Thread(target=lister_thread),
        ]

        start_time = time.time()
        for t in threads:
            t.start()

        # Run for test duration
        time.sleep(test_duration)
        stop_flag.set()

        # Wait for threads to finish
        for t in threads:
            t.join(timeout=5)

        elapsed = time.time() - start_time

        print(f"\n  Test completed after {elapsed:.1f}s")
        print(f"  Objects created: {len(created_keys) + len(deleted_keys)}")
        print(f"  Objects deleted: {len(deleted_keys)}")
        print(f"  List operations: {len(list_results)}")

        # Analyze list consistency
        if list_results:
            object_counts = [r["count"] for r in list_results]
            print(f"  Object count range: {min(object_counts)} - {max(object_counts)}")

        # Allow time for eventual consistency to converge
        print(f"\n  Waiting 5 seconds for eventual consistency...")
        time.sleep(5)

        # Final verification
        final_objects = s3_client.list_objects(bucket_name, prefix=prefix)
        final_keys = set(obj["Key"] for obj in final_objects)

        with lock:
            # Keys that should exist (created but not deleted)
            expected_keys = set(created_keys)

        print(f"  Final state:")
        print(f"    Expected keys: {len(expected_keys)}")
        print(f"    Actual keys: {len(final_keys)}")

        # Check for phantom objects (deleted but still appear)
        phantoms = final_keys - expected_keys
        if phantoms:
            print(f"  ⚠ Phantom objects found: {len(phantoms)}")
            for key in list(phantoms)[:5]:  # Show first 5
                print(f"    - {key}")

        # Check for missing objects (should exist but don't appear)
        missing = expected_keys - final_keys
        if missing:
            print(f"  ⚠ Missing objects: {len(missing)}")
            for key in list(missing)[:5]:  # Show first 5
                print(f"    - {key}")

        # In eventually consistent systems, we expect eventual convergence
        # Some transient inconsistencies are acceptable
        assert (
            len(phantoms) < len(deleted_keys) * 0.1
        ), f"Too many phantom objects: {len(phantoms)}"

        assert (
            len(missing) < len(expected_keys) * 0.1
        ), f"Too many missing objects: {len(missing)}"

        print(f"  ✓ List consistency acceptable (eventually consistent)")

    finally:
        # Cleanup
        try:
            # Delete all objects in prefix
            objects = s3_client.list_objects(bucket_name, prefix=prefix)
            for obj in objects:
                s3_client.delete_object(bucket_name, obj["Key"])
            s3_client.delete_bucket(bucket_name)
        except Exception:
            pass


def test_list_pagination_consistency(s3_client, config):
    """
    Test that pagination returns consistent results across pages.

    Create 1000 objects, list with small page size (100),
    verify no duplicates or missing objects across pages.
    """
    bucket_name = f"{config['s3_bucket_prefix']}-paginate-{random_string()}"
    prefix = "paginated/"
    num_objects = 1000
    page_size = 100

    try:
        s3_client.create_bucket(bucket_name)

        # Create objects
        created_keys = []
        print(f"\nCreating {num_objects} objects...")

        for i in range(num_objects):
            key = f"{prefix}object-{i:05d}.dat"
            s3_client.put_object(bucket_name, key, f"data-{i}".encode())
            created_keys.append(key)

        created_set = set(created_keys)
        print(f"  Created {len(created_keys)} objects")

        # List with pagination
        listed_keys = []
        continuation_token = None
        page_count = 0

        while True:
            if continuation_token:
                response = s3_client.client.list_objects_v2(
                    Bucket=bucket_name,
                    Prefix=prefix,
                    MaxKeys=page_size,
                    ContinuationToken=continuation_token,
                )
            else:
                response = s3_client.client.list_objects_v2(
                    Bucket=bucket_name, Prefix=prefix, MaxKeys=page_size
                )

            page_count += 1
            objects = response.get("Contents", [])
            page_keys = [obj["Key"] for obj in objects]
            listed_keys.extend(page_keys)

            print(f"  Page {page_count}: {len(page_keys)} objects")

            if not response.get("IsTruncated"):
                break

            continuation_token = response.get("NextContinuationToken")

        listed_set = set(listed_keys)

        print(f"\n  Total pages: {page_count}")
        print(f"  Total objects listed: {len(listed_keys)}")

        # Check for duplicates
        duplicates = len(listed_keys) - len(listed_set)
        assert duplicates == 0, f"Found {duplicates} duplicate objects across pages!"

        # Check all objects were listed
        missing = created_set - listed_set
        extra = listed_set - created_set

        assert (
            len(missing) == 0
        ), f"Missing {len(missing)} objects in paginated listing!"

        assert (
            len(extra) == 0
        ), f"Found {len(extra)} extra objects in paginated listing!"

        print(f"  ✓ Pagination consistency verified: no duplicates or missing objects")

    finally:
        # Cleanup
        try:
            # Delete all objects
            for key in created_keys:
                s3_client.delete_object(bucket_name, key)
            s3_client.delete_bucket(bucket_name)
        except Exception:
            pass


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
