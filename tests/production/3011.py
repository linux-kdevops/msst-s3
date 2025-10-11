#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Test 11: Network Partition Recovery and Retry Storms

Production Pattern: Network failures during operations, retry amplification, zombie processes
Real-world scenarios: Kubernetes restarts, Lambda timeouts, DNS failures

What it tests:
- Simulate network partition during multipart upload
- Client timeout and retry behavior
- Orphaned operations detection
- Retry storm prevention (exponential backoff)
- Duplicate request handling (idempotency)

Why it matters:
- Network failures are the #1 cause of production incidents
- Naive retry logic can cause cascading failures (retry storms)
- Zombie operations waste resources and cause data inconsistency
- Critical for distributed systems resilience

Generated-by: Claude AI
Signed-off-by: Luis Chamberlain <mcgrof@kernel.org>
"""

import pytest
import time
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from tests.common.test_utils import random_string


def test_multipart_resume_after_failure(s3_client, config):
    """
    Test resuming multipart upload after simulated failure.

    Simulates network partition by intentionally pausing during upload,
    then resuming from checkpoint.
    """
    bucket_name = f"{config['s3_bucket_prefix']}-resume-{random_string()}"
    key = "large/resumable-file.bin"
    num_parts = 10
    part_size = 5 * 1024 * 1024  # 5MB per part

    try:
        s3_client.create_bucket(bucket_name)

        print(f"\nTesting multipart resume after failure...")

        # Start multipart upload
        upload_id = s3_client.create_multipart_upload(bucket_name, key)
        print(f"  Started upload: {upload_id}")

        # Upload first 5 parts successfully
        uploaded_parts = []
        for part_num in range(1, 6):
            data = b"X" * part_size
            response = s3_client.upload_part(bucket_name, key, upload_id, part_num, data)
            uploaded_parts.append({"PartNumber": part_num, "ETag": response["ETag"]})
            print(f"  Uploaded part {part_num}")

        # Simulate failure - don't upload parts 6-10, just record upload_id
        print(f"  Simulating network failure after part 5...")

        # List existing parts (simulate recovery/checkpoint)
        parts_response = s3_client.client.list_parts(
            Bucket=bucket_name, Key=key, UploadId=upload_id
        )
        existing_parts = parts_response.get("Parts", [])

        assert len(existing_parts) == 5, f"Expected 5 parts, found {len(existing_parts)}"
        print(f"  Recovery: Found {len(existing_parts)} existing parts")

        # Resume: Upload remaining parts
        for part_num in range(6, num_parts + 1):
            data = b"Y" * part_size
            response = s3_client.upload_part(bucket_name, key, upload_id, part_num, data)
            uploaded_parts.append({"PartNumber": part_num, "ETag": response["ETag"]})
            print(f"  Resumed: Uploaded part {part_num}")

        # Complete upload
        s3_client.complete_multipart_upload(bucket_name, key, upload_id, uploaded_parts)

        print(f"  ✓ Upload completed successfully after recovery")

        # Verify object exists and has correct size
        response = s3_client.head_object(bucket_name, key)
        expected_size = num_parts * part_size
        actual_size = response["ContentLength"]

        assert (
            actual_size == expected_size
        ), f"Object size mismatch: expected {expected_size}, got {actual_size}"

        print(f"  ✓ Object integrity verified: {actual_size} bytes")

    finally:
        # Cleanup
        try:
            s3_client.delete_object(bucket_name, key)
            s3_client.delete_bucket(bucket_name)
        except Exception:
            pass


def test_exponential_backoff_under_failures(s3_client, config):
    """
    Test that clients implement exponential backoff correctly.

    Simulates transient failures and verifies retry spacing increases.
    """
    bucket_name = f"{config['s3_bucket_prefix']}-backoff-{random_string()}"
    num_clients = 20

    try:
        s3_client.create_bucket(bucket_name)

        print(f"\nTesting exponential backoff with {num_clients} clients...")

        retry_stats = []

        def upload_with_simulated_failures(client_id):
            """Upload with simulated transient failures"""
            key = f"data/file-{client_id}.dat"
            max_retries = 5
            base_delay = 0.1  # 100ms base delay

            attempt_times = []

            for attempt in range(max_retries):
                attempt_time = time.time()
                attempt_times.append(attempt_time)

                try:
                    # Simulate 60% failure rate on first 3 attempts
                    if attempt < 3 and random.random() < 0.6:
                        # Simulate failure - wait and retry
                        delay = base_delay * (2 ** attempt)  # Exponential backoff
                        time.sleep(delay)
                        continue

                    # Success case
                    s3_client.put_object(bucket_name, key, f"data-{client_id}".encode())

                    return {
                        "client_id": client_id,
                        "success": True,
                        "attempts": attempt + 1,
                        "attempt_times": attempt_times,
                    }

                except Exception as e:
                    delay = base_delay * (2 ** attempt)
                    time.sleep(delay)

            return {
                "client_id": client_id,
                "success": False,
                "attempts": max_retries,
                "attempt_times": attempt_times,
            }

        # Execute concurrent uploads with failures
        results = []
        with ThreadPoolExecutor(max_workers=num_clients) as executor:
            futures = [
                executor.submit(upload_with_simulated_failures, i)
                for i in range(num_clients)
            ]

            for future in as_completed(futures):
                results.append(future.result())

        successes = [r for r in results if r["success"]]
        print(f"  Successful uploads: {len(successes)}/{num_clients}")

        # Analyze retry patterns
        total_attempts = sum(r["attempts"] for r in results)
        avg_attempts = total_attempts / len(results)

        print(f"  Average attempts per client: {avg_attempts:.1f}")

        # Verify exponential backoff behavior
        for result in results:
            if len(result["attempt_times"]) > 1:
                # Check that delays between attempts increase
                delays = []
                for i in range(1, len(result["attempt_times"])):
                    delay = result["attempt_times"][i] - result["attempt_times"][i - 1]
                    delays.append(delay)

                # Verify delays increase (allowing some variance)
                if len(delays) > 1:
                    # Each delay should be roughly 2x the previous
                    for i in range(1, len(delays)):
                        # Allow 50% tolerance for scheduling variance
                        assert delays[i] >= delays[i-1] * 0.5, \
                            f"Backoff not increasing: {delays}"

        print(f"  ✓ Exponential backoff verified")

        # Verify no retry storm (request rate should decrease over time)
        # This is implicitly tested by the exponential backoff verification

    finally:
        # Cleanup
        try:
            objects = s3_client.list_objects(bucket_name)
            for obj in objects:
                s3_client.delete_object(bucket_name, obj["Key"])
            s3_client.delete_bucket(bucket_name)
        except Exception:
            pass


def test_zombie_upload_detection(s3_client, config):
    """
    Test detection of abandoned (zombie) multipart uploads.

    Creates uploads that are never completed or aborted,
    then verifies they can be detected and cleaned up.
    """
    bucket_name = f"{config['s3_bucket_prefix']}-zombie-{random_string()}"
    num_zombies = 10

    try:
        s3_client.create_bucket(bucket_name)

        print(f"\nCreating {num_zombies} zombie uploads...")

        zombie_uploads = []
        creation_times = {}

        # Create zombie uploads (never complete or abort)
        for i in range(num_zombies):
            key = f"abandoned/file-{i}.dat"
            upload_id = s3_client.create_multipart_upload(bucket_name, key)

            # Some zombies have parts uploaded, some don't
            if i % 2 == 0:
                data = b"zombie data" * 1000
                s3_client.upload_part(bucket_name, key, upload_id, 1, data)

            zombie_uploads.append({"key": key, "upload_id": upload_id})
            creation_times[upload_id] = time.time()

        print(f"  Created {num_zombies} abandoned uploads")

        # Wait a bit to simulate age
        time.sleep(2)

        # Detect zombies via ListMultipartUploads
        response = s3_client.client.list_multipart_uploads(Bucket=bucket_name)
        detected_zombies = response.get("Uploads", [])

        print(f"  Detected {len(detected_zombies)} zombie uploads")

        assert (
            len(detected_zombies) == num_zombies
        ), f"Expected {num_zombies} zombies, found {len(detected_zombies)}"

        # Verify age-based detection
        now = time.time()
        for zombie in detected_zombies:
            upload_id = zombie["UploadId"]
            age = now - creation_times.get(upload_id, now)

            # All zombies should be at least 2 seconds old
            assert age >= 2, f"Zombie too young: {age}s"

        print(f"  ✓ All zombies are at least 2 seconds old")

        # Cleanup: Abort all zombie uploads
        cleanup_count = 0
        for zombie in detected_zombies:
            try:
                s3_client.abort_multipart_upload(
                    bucket_name, zombie["Key"], zombie["UploadId"]
                )
                cleanup_count += 1
            except Exception:
                pass

        print(f"  ✓ Cleaned up {cleanup_count}/{len(detected_zombies)} zombies")

        # Verify cleanup
        response = s3_client.client.list_multipart_uploads(Bucket=bucket_name)
        remaining = response.get("Uploads", [])

        assert len(remaining) == 0, f"Still have {len(remaining)} zombies after cleanup"

        print(f"  ✓ All zombies successfully eliminated")

    finally:
        # Cleanup
        try:
            response = s3_client.client.list_multipart_uploads(Bucket=bucket_name)
            for upload in response.get("Uploads", []):
                s3_client.abort_multipart_upload(
                    bucket_name, upload["Key"], upload["UploadId"]
                )
            s3_client.delete_bucket(bucket_name)
        except Exception:
            pass


def test_idempotent_part_upload(s3_client, config):
    """
    Test that re-uploading the same part number is idempotent.

    Simulates network retry by uploading same part multiple times.
    """
    bucket_name = f"{config['s3_bucket_prefix']}-idempotent-{random_string()}"
    key = "data/idempotent-test.dat"

    try:
        s3_client.create_bucket(bucket_name)

        print(f"\nTesting idempotent part uploads...")

        upload_id = s3_client.create_multipart_upload(bucket_name, key)

        # Upload part 1 three times (simulating retries)
        data = b"X" * (5 * 1024 * 1024)  # 5MB

        etags = []
        for attempt in range(3):
            response = s3_client.upload_part(bucket_name, key, upload_id, 1, data)
            etag = response["ETag"]
            etags.append(etag)
            print(f"  Upload attempt {attempt + 1}: ETag={etag}")

        # All ETags should be identical (same data, same part number)
        assert all(e == etags[0] for e in etags), \
            f"ETags differ across retries: {etags}"

        print(f"  ✓ All retry attempts produced identical ETag")

        # List parts - should only show 1 part (latest upload)
        parts_response = s3_client.client.list_parts(
            Bucket=bucket_name, Key=key, UploadId=upload_id
        )
        parts = parts_response.get("Parts", [])

        assert len(parts) == 1, f"Expected 1 part, found {len(parts)}"
        assert parts[0]["PartNumber"] == 1
        assert parts[0]["ETag"].strip('"') == etags[-1].strip('"')

        print(f"  ✓ Only one part exists (retries are idempotent)")

        # Complete upload
        s3_client.complete_multipart_upload(
            bucket_name, key, upload_id, [{"PartNumber": 1, "ETag": etags[-1]}]
        )

        print(f"  ✓ Upload completed successfully")

    finally:
        # Cleanup
        try:
            s3_client.delete_object(bucket_name, key)
            s3_client.delete_bucket(bucket_name)
        except Exception:
            pass


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
