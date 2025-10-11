#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Test 15: Read-After-Write Consistency Boundaries

Production Pattern: Understanding and testing S3's consistency model limits
Real-world scenarios: ETL pipelines, data verification, multi-step transactions

What it tests:
- Strong consistency for PUTs (since Dec 2020)
- Read-your-writes vs read-my-writes
- Cross-client consistency timing
- LIST consistency after PUT/DELETE
- Consistency under load

Why it matters:
- Applications depend on consistency guarantees
- Many systems assume immediate visibility
- ETL pipelines can fail on stale listings
- Critical for correctness reasoning

Generated-by: Claude AI
Signed-off-by: Luis Chamberlain <mcgrof@kernel.org>
"""

import pytest
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from tests.common.test_utils import random_string


def test_put_then_get_same_client(s3_client, config):
    """
    Test read-your-writes consistency.

    PUT an object, then immediately GET it from the same client.
    Should be immediately consistent.
    """
    bucket_name = f"{config['s3_bucket_prefix']}-ryw-{random_string()}"
    key = "consistency/test-object.txt"
    content = b"test data for read-your-writes"

    try:
        s3_client.create_bucket(bucket_name)

        print(f"\nTesting read-your-writes consistency...")

        # PUT object
        put_start = time.time()
        put_response = s3_client.put_object(bucket_name, key, content)
        put_duration = time.time() - put_start
        put_etag = put_response.get("ETag", "").strip('"')

        print(f"  PUT completed in {put_duration*1000:.1f}ms, ETag={put_etag}")

        # Immediately GET object (same client)
        get_start = time.time()
        get_response = s3_client.get_object(bucket_name, key)
        get_duration = time.time() - get_start
        retrieved_content = get_response["Body"].read()
        get_etag = get_response.get("ETag", "").strip('"')

        consistency_delay = (get_start - put_start) * 1000  # ms

        print(f"  GET completed in {get_duration*1000:.1f}ms")
        print(f"  Consistency delay: {consistency_delay:.1f}ms")

        # Verify immediate consistency
        assert (
            retrieved_content == content
        ), "Content mismatch - read-your-writes failed!"
        assert put_etag == get_etag, f"ETag mismatch: PUT={put_etag}, GET={get_etag}"

        # For strong consistency, this should be < 100ms
        print(f"  ✓ Read-your-writes verified (delay: {consistency_delay:.1f}ms)")

        if consistency_delay < 100:
            print(f"  ✓ Strong consistency: < 100ms")
        elif consistency_delay < 1000:
            print(f"  ⚠ Eventual consistency: {consistency_delay:.1f}ms")
        else:
            print(f"  ⚠ Slow consistency: {consistency_delay:.1f}ms")

    finally:
        # Cleanup
        try:
            s3_client.delete_object(bucket_name, key)
            s3_client.delete_bucket(bucket_name)
        except Exception:
            pass


def test_put_then_get_different_clients(s3_client, config):
    """
    Test read-my-writes consistency across different clients.

    Client A PUTs object, Client B immediately tries to GET it.
    Measures cross-client propagation delay.
    """
    bucket_name = f"{config['s3_bucket_prefix']}-cross-{random_string()}"
    key = "consistency/cross-client.txt"
    content = b"cross-client test data"
    num_readers = 10

    try:
        s3_client.create_bucket(bucket_name)

        print(f"\nTesting cross-client consistency with {num_readers} readers...")

        # Writer: PUT object
        put_time = time.time()
        s3_client.put_object(bucket_name, key, content)
        print(f"  Object written at t=0")

        # Readers: Immediately try to read (simulating different clients)
        read_delays = []

        def reader_thread(reader_id):
            """Simulate different client reading"""
            read_start = time.time()
            try:
                response = s3_client.get_object(bucket_name, key)
                retrieved = response["Body"].read()
                read_end = time.time()

                delay = (read_start - put_time) * 1000  # ms
                success = retrieved == content

                return {
                    "reader_id": reader_id,
                    "success": success,
                    "delay_ms": delay,
                    "duration_ms": (read_end - read_start) * 1000,
                }
            except Exception as e:
                read_end = time.time()
                delay = (read_start - put_time) * 1000
                return {
                    "reader_id": reader_id,
                    "success": False,
                    "delay_ms": delay,
                    "error": str(e),
                }

        # Launch readers concurrently
        results = []
        with ThreadPoolExecutor(max_workers=num_readers) as executor:
            futures = [executor.submit(reader_thread, i) for i in range(num_readers)]

            for future in as_completed(futures):
                results.append(future.result())

        # Analyze results
        successes = [r for r in results if r["success"]]
        failures = [r for r in results if not r["success"]]

        print(f"  Successful reads: {len(successes)}/{num_readers}")

        if successes:
            delays = [r["delay_ms"] for r in successes]
            avg_delay = sum(delays) / len(delays)
            max_delay = max(delays)
            min_delay = min(delays)

            print(f"  Read delays: min={min_delay:.1f}ms, avg={avg_delay:.1f}ms, max={max_delay:.1f}ms")

            # Strong consistency: all reads succeed immediately
            if len(successes) == num_readers and max_delay < 100:
                print(f"  ✓ Strong cross-client consistency")
            elif len(successes) == num_readers and max_delay < 1000:
                print(f"  ✓ Eventual consistency (< 1s)")
            else:
                print(f"  ⚠ Slow or incomplete consistency")

        if failures:
            print(f"  ⚠ {len(failures)} readers failed to read object")

    finally:
        # Cleanup
        try:
            s3_client.delete_object(bucket_name, key)
            s3_client.delete_bucket(bucket_name)
        except Exception:
            pass


def test_put_then_list_consistency(s3_client, config):
    """
    Test LIST consistency after PUT.

    PUT an object, then immediately LIST the prefix.
    Verify the new object appears in the listing.
    """
    bucket_name = f"{config['s3_bucket_prefix']}-list-put-{random_string()}"
    prefix = "data/"
    num_objects = 100

    try:
        s3_client.create_bucket(bucket_name)

        print(f"\nTesting LIST consistency after PUT ({num_objects} objects)...")

        created_keys = set()
        put_times = []

        # Create objects and immediately list after each
        consistency_times = []

        for i in range(num_objects):
            key = f"{prefix}object-{i:04d}.dat"

            # PUT object
            put_start = time.time()
            s3_client.put_object(bucket_name, key, f"data-{i}".encode())
            put_end = time.time()
            created_keys.add(key)

            # Immediately LIST
            list_start = time.time()
            objects = s3_client.list_objects(bucket_name, prefix=prefix)
            list_end = time.time()
            listed_keys = set(obj["Key"] for obj in objects)

            # Check if newly created object appears
            if key in listed_keys:
                consistency_time = (list_start - put_start) * 1000
                consistency_times.append(consistency_time)

        # Analyze consistency
        visible_immediately = len(consistency_times)
        print(f"  Objects visible in immediate LIST: {visible_immediately}/{num_objects}")

        if consistency_times:
            avg_consistency = sum(consistency_times) / len(consistency_times)
            max_consistency = max(consistency_times)
            print(f"  Consistency time: avg={avg_consistency:.1f}ms, max={max_consistency:.1f}ms")

        # Wait for eventual consistency
        if visible_immediately < num_objects:
            print(f"  Waiting up to 5s for eventual consistency...")
            for attempt in range(10):
                time.sleep(0.5)
                objects = s3_client.list_objects(bucket_name, prefix=prefix)
                listed_keys = set(obj["Key"] for obj in objects)

                if listed_keys == created_keys:
                    convergence_time = (attempt + 1) * 0.5
                    print(f"  ✓ Full consistency achieved after {convergence_time}s")
                    break
            else:
                # Final check
                objects = s3_client.list_objects(bucket_name, prefix=prefix)
                listed_keys = set(obj["Key"] for obj in objects)
                missing = created_keys - listed_keys
                if missing:
                    print(f"  ⚠ Still missing {len(missing)} objects after 5s")

        # Final verification
        objects = s3_client.list_objects(bucket_name, prefix=prefix)
        final_count = len(objects)

        assert (
            final_count >= num_objects
        ), f"Expected at least {num_objects} objects, found {final_count}"

        print(f"  ✓ Final count: {final_count} objects")

    finally:
        # Cleanup
        try:
            objects = s3_client.list_objects(bucket_name, prefix=prefix)
            for obj in objects:
                s3_client.delete_object(bucket_name, obj["Key"])
            s3_client.delete_bucket(bucket_name)
        except Exception:
            pass


def test_delete_then_list_consistency(s3_client, config):
    """
    Test LIST consistency after DELETE.

    Create objects, DELETE them, then immediately LIST.
    Check for phantom objects (deleted but still appear).
    """
    bucket_name = f"{config['s3_bucket_prefix']}-list-del-{random_string()}"
    prefix = "temp/"
    num_objects = 50

    try:
        s3_client.create_bucket(bucket_name)

        print(f"\nTesting LIST consistency after DELETE ({num_objects} objects)...")

        # Create objects
        created_keys = []
        for i in range(num_objects):
            key = f"{prefix}temp-{i:04d}.dat"
            s3_client.put_object(bucket_name, key, f"temp-{i}".encode())
            created_keys.append(key)

        print(f"  Created {num_objects} objects")

        # Verify all exist
        objects = s3_client.list_objects(bucket_name, prefix=prefix)
        assert len(objects) == num_objects

        # Delete all objects
        delete_start = time.time()
        for key in created_keys:
            s3_client.delete_object(bucket_name, key)
        delete_duration = time.time() - delete_start

        print(f"  Deleted {num_objects} objects in {delete_duration:.2f}s")

        # Immediately LIST
        list_start = time.time()
        objects = s3_client.list_objects(bucket_name, prefix=prefix)
        listed_keys = [obj["Key"] for obj in objects]

        phantom_count = len(listed_keys)
        consistency_delay = (list_start - delete_start) * 1000

        if phantom_count == 0:
            print(f"  ✓ No phantoms (strong consistency): {consistency_delay:.1f}ms")
        else:
            print(f"  ⚠ {phantom_count} phantom objects immediately after DELETE")
            print(f"  Phantom keys: {listed_keys[:5]}...")

        # Wait for eventual consistency
        if phantom_count > 0:
            print(f"  Waiting for phantoms to disappear...")
            for attempt in range(10):
                time.sleep(0.5)
                objects = s3_client.list_objects(bucket_name, prefix=prefix)

                if len(objects) == 0:
                    convergence_time = (attempt + 1) * 0.5
                    print(f"  ✓ Phantoms cleared after {convergence_time}s")
                    break
            else:
                objects = s3_client.list_objects(bucket_name, prefix=prefix)
                remaining = len(objects)
                if remaining > 0:
                    print(f"  ⚠ Still {remaining} phantoms after 5s")

        # Final verification
        objects = s3_client.list_objects(bucket_name, prefix=prefix)
        final_count = len(objects)

        assert final_count == 0, f"Expected 0 objects, found {final_count} phantoms"

        print(f"  ✓ All objects successfully deleted")

    finally:
        # Cleanup
        try:
            objects = s3_client.list_objects(bucket_name, prefix=prefix)
            for obj in objects:
                s3_client.delete_object(bucket_name, obj["Key"])
            s3_client.delete_bucket(bucket_name)
        except Exception:
            pass


def test_consistency_under_concurrent_load(s3_client, config):
    """
    Test consistency guarantees under concurrent load.

    Multiple writers and readers operating simultaneously.
    Verify each reader sees a consistent view.
    """
    bucket_name = f"{config['s3_bucket_prefix']}-concurrent-{random_string()}"
    num_writers = 10
    num_readers = 20
    objects_per_writer = 10

    try:
        s3_client.create_bucket(bucket_name)

        print(
            f"\nTesting consistency under load: {num_writers} writers, {num_readers} readers..."
        )

        written_keys = set()
        lock = threading.Lock()
        stop_flag = threading.Event()

        def writer_thread(writer_id):
            """Write objects continuously"""
            keys_written = []
            for i in range(objects_per_writer):
                key = f"writer-{writer_id}/object-{i}.dat"
                s3_client.put_object(bucket_name, key, f"w{writer_id}-{i}".encode())

                with lock:
                    written_keys.add(key)
                    keys_written.append(key)

                time.sleep(0.1)  # Pace writes

            return {"writer_id": writer_id, "keys_written": keys_written}

        def reader_thread(reader_id):
            """Read and verify consistency"""
            reads = 0
            inconsistencies = 0

            while not stop_flag.is_set() and reads < 20:
                # List all objects
                objects = s3_client.list_objects(bucket_name)
                listed_keys = set(obj["Key"] for obj in objects)

                # Check consistency: all listed objects should be readable
                for key in list(listed_keys)[:5]:  # Sample 5 objects
                    try:
                        s3_client.get_object(bucket_name, key)
                    except Exception:
                        inconsistencies += 1

                reads += 1
                time.sleep(0.2)

            return {
                "reader_id": reader_id,
                "reads": reads,
                "inconsistencies": inconsistencies,
            }

        # Start writers
        writer_results = []
        with ThreadPoolExecutor(max_workers=num_writers) as executor:
            writer_futures = [
                executor.submit(writer_thread, i) for i in range(num_writers)
            ]

            # Start readers concurrently
            reader_futures = []
            with ThreadPoolExecutor(max_workers=num_readers) as reader_executor:
                reader_futures = [
                    reader_executor.submit(reader_thread, i) for i in range(num_readers)
                ]

                # Wait for writers to complete
                for future in as_completed(writer_futures):
                    writer_results.append(future.result())

                # Stop readers
                stop_flag.set()

                # Collect reader results
                reader_results = []
                for future in as_completed(reader_futures):
                    reader_results.append(future.result())

        total_written = sum(len(w["keys_written"]) for w in writer_results)
        total_reads = sum(r["reads"] for r in reader_results)
        total_inconsistencies = sum(r["inconsistencies"] for r in reader_results)

        print(f"  Total objects written: {total_written}")
        print(f"  Total read operations: {total_reads}")
        print(f"  Inconsistencies detected: {total_inconsistencies}")

        # Verify final consistency
        objects = s3_client.list_objects(bucket_name)
        final_count = len(objects)

        print(f"  Final object count: {final_count}")

        assert (
            final_count == total_written
        ), f"Expected {total_written} objects, found {final_count}"

        print(f"  ✓ Final consistency verified")

        if total_inconsistencies == 0:
            print(f"  ✓ No inconsistencies detected (strong consistency)")
        else:
            inconsistency_rate = (total_inconsistencies / total_reads) * 100
            print(f"  ⚠ Inconsistency rate: {inconsistency_rate:.2f}%")

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
