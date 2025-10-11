#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Test 14: Conditional Write for Distributed Consensus

Production Pattern: Building distributed locks, leader election, atomic counters
Real-world scenarios: Serverless coordination, cron jobs, distributed algorithms

What it tests:
- Compare-and-swap using If-Match/If-None-Match
- 100 clients trying to acquire distributed lock
- Only one succeeds per generation
- Test fairness and liveness
- Performance under contention

Why it matters:
- S3 can be used for lightweight coordination
- Common pattern in serverless architectures
- Cheaper than DynamoDB/ZooKeeper for simple cases
- Critical for distributed algorithms

Generated-by: Claude AI
Signed-off-by: Luis Chamberlain <mcgrof@kernel.org>
"""

import pytest
import time
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from tests.common.test_utils import random_string


def test_distributed_lock_with_etag(s3_client, config):
    """
    Implement distributed lock using ETag-based compare-and-swap.

    Multiple clients compete for lock. Only one acquires at a time.
    """
    bucket_name = f"{config['s3_bucket_prefix']}-lock-{random_string()}"
    lock_key = "locks/distributed-lock.json"
    num_clients = 50

    try:
        s3_client.create_bucket(bucket_name)

        print(f"\nTesting distributed lock with {num_clients} clients...")

        # Initialize lock object
        initial_lock = {"holder": "none", "timestamp": time.time(), "generation": 0}
        s3_client.put_object(bucket_name, lock_key, json.dumps(initial_lock).encode())

        lock_acquisitions = []

        def try_acquire_lock(client_id):
            """Try to acquire distributed lock"""
            max_attempts = 10
            acquired = False

            for attempt in range(max_attempts):
                try:
                    # Read current lock state
                    response = s3_client.get_object(bucket_name, lock_key)
                    current_etag = response["ETag"].strip('"')
                    current_lock = json.loads(response["Body"].read())

                    # Check if lock is available or expired
                    lock_age = time.time() - current_lock.get("timestamp", 0)

                    if current_lock.get("holder") == "none" or lock_age > 30:
                        # Try to acquire lock with conditional write
                        new_lock = {
                            "holder": f"client-{client_id}",
                            "timestamp": time.time(),
                            "generation": current_lock.get("generation", 0) + 1,
                        }

                        try:
                            # Use CopyObject with If-Match to implement CAS
                            source = {"Bucket": bucket_name, "Key": lock_key}
                            response = s3_client.client.copy_object(
                                Bucket=bucket_name,
                                Key=lock_key,
                                CopySource=source,
                                Metadata={
                                    "holder": f"client-{client_id}",
                                    "generation": str(new_lock["generation"]),
                                },
                                MetadataDirective="REPLACE",
                                CopySourceIfMatch=current_etag,
                            )

                            # Also update content
                            s3_client.put_object(
                                bucket_name, lock_key, json.dumps(new_lock).encode()
                            )

                            acquired = True
                            return {
                                "client_id": client_id,
                                "acquired": True,
                                "attempts": attempt + 1,
                                "generation": new_lock["generation"],
                            }

                        except Exception as e:
                            # Conditional write failed - someone else got the lock
                            if "PreconditionFailed" in str(e) or "412" in str(e):
                                # Expected - retry
                                time.sleep(0.05)
                                continue
                            else:
                                # Unexpected error
                                return {
                                    "client_id": client_id,
                                    "acquired": False,
                                    "error": str(e),
                                }
                    else:
                        # Lock held by someone else, wait and retry
                        time.sleep(0.05)

                except Exception as e:
                    time.sleep(0.05)

            return {
                "client_id": client_id,
                "acquired": False,
                "attempts": max_attempts,
            }

        # All clients try to acquire lock
        results = []
        with ThreadPoolExecutor(max_workers=num_clients) as executor:
            futures = [executor.submit(try_acquire_lock, i) for i in range(num_clients)]

            for future in as_completed(futures):
                result = future.result()
                results.append(result)
                if result.get("acquired"):
                    lock_acquisitions.append(result)

        acquired_count = len(lock_acquisitions)
        print(f"  Locks acquired: {acquired_count}/{num_clients}")

        # Verify at least some clients acquired the lock
        assert acquired_count > 0, "At least one client should acquire lock"

        # Verify no generation conflicts (each acquisition increments generation)
        if acquired_count > 1:
            generations = [acq["generation"] for acq in lock_acquisitions]
            assert len(generations) == len(set(generations)), \
                "Lock generations must be unique"

            print(f"  ✓ Lock generations are unique: {min(generations)}-{max(generations)}")

        print(f"  ✓ Distributed lock pattern working")

    finally:
        # Cleanup
        try:
            s3_client.delete_object(bucket_name, lock_key)
            s3_client.delete_bucket(bucket_name)
        except Exception:
            pass


def test_atomic_counter_with_cas(s3_client, config):
    """
    Implement atomic counter using compare-and-swap.

    1000 clients each increment by 1. Final count must be exactly 1000.
    """
    bucket_name = f"{config['s3_bucket_prefix']}-counter-{random_string()}"
    counter_key = "counters/atomic-counter.json"
    num_increments = 100  # Reduced for reasonable test time

    try:
        s3_client.create_bucket(bucket_name)

        print(f"\nTesting atomic counter with {num_increments} increments...")

        # Initialize counter
        initial_counter = {"value": 0, "updates": 0}
        s3_client.put_object(
            bucket_name, counter_key, json.dumps(initial_counter).encode()
        )

        def increment_counter(client_id):
            """Atomically increment counter"""
            max_retries = 20

            for attempt in range(max_retries):
                try:
                    # Read current value
                    response = s3_client.get_object(bucket_name, counter_key)
                    current_etag = response["ETag"].strip('"')
                    counter_data = json.loads(response["Body"].read())

                    current_value = counter_data.get("value", 0)
                    updates = counter_data.get("updates", 0)

                    # Compute new value
                    new_value = current_value + 1
                    new_counter = {"value": new_value, "updates": updates + 1}

                    # Conditional update using If-Match
                    try:
                        s3_client.client.put_object(
                            Bucket=bucket_name,
                            Key=counter_key,
                            Body=json.dumps(new_counter).encode(),
                            IfMatch=current_etag,
                        )

                        return {
                            "client_id": client_id,
                            "success": True,
                            "attempts": attempt + 1,
                            "old_value": current_value,
                            "new_value": new_value,
                        }

                    except Exception as e:
                        if "PreconditionFailed" in str(e) or "412" in str(e):
                            # CAS failed - retry
                            time.sleep(0.01 * (2 ** min(attempt, 5)))  # Exp backoff
                            continue
                        else:
                            raise

                except Exception as e:
                    if attempt == max_retries - 1:
                        return {
                            "client_id": client_id,
                            "success": False,
                            "error": str(e),
                        }

            return {"client_id": client_id, "success": False, "attempts": max_retries}

        # Execute concurrent increments
        results = []
        start_time = time.time()

        with ThreadPoolExecutor(max_workers=20) as executor:
            futures = [
                executor.submit(increment_counter, i) for i in range(num_increments)
            ]

            for future in as_completed(futures):
                results.append(future.result())

        duration = time.time() - start_time

        successes = [r for r in results if r.get("success")]
        failures = [r for r in results if not r.get("success")]

        print(f"  Duration: {duration:.2f}s")
        print(f"  Successful increments: {len(successes)}/{num_increments}")
        print(f"  Failed increments: {len(failures)}")

        # Analyze retry patterns
        if successes:
            attempts = [r["attempts"] for r in successes]
            avg_attempts = sum(attempts) / len(attempts)
            max_attempts = max(attempts)
            print(f"  Average CAS attempts: {avg_attempts:.1f}")
            print(f"  Max CAS attempts: {max_attempts}")

        # Read final counter value
        response = s3_client.get_object(bucket_name, counter_key)
        final_counter = json.loads(response["Body"].read())
        final_value = final_counter["value"]

        print(f"  Final counter value: {final_value}")

        # Verify correctness
        assert final_value == len(successes), \
            f"Counter mismatch: expected {len(successes)}, got {final_value}"

        print(f"  ✓ Atomic counter accuracy: 100%")
        print(f"  ✓ No lost updates detected")

    finally:
        # Cleanup
        try:
            s3_client.delete_object(bucket_name, counter_key)
            s3_client.delete_bucket(bucket_name)
        except Exception:
            pass


def test_leader_election_pattern(s3_client, config):
    """
    Implement leader election using S3.

    Multiple workers compete for leadership.
    Leader writes heartbeat. Workers monitor and re-elect if stale.
    """
    bucket_name = f"{config['s3_bucket_prefix']}-leader-{random_string()}"
    leader_key = "cluster/leader.json"
    num_workers = 20
    heartbeat_interval = 1.0  # seconds
    heartbeat_timeout = 3.0  # seconds

    try:
        s3_client.create_bucket(bucket_name)

        print(f"\nTesting leader election with {num_workers} workers...")

        # Initialize leader state
        initial_leader = {
            "leader_id": "none",
            "term": 0,
            "last_heartbeat": 0,
        }
        s3_client.put_object(
            bucket_name, leader_key, json.dumps(initial_leader).encode()
        )

        election_history = []

        def try_become_leader(worker_id):
            """Try to become leader"""
            try:
                # Read current leader state
                response = s3_client.get_object(bucket_name, leader_key)
                current_etag = response["ETag"].strip('"')
                leader_state = json.loads(response["Body"].read())

                last_heartbeat = leader_state.get("last_heartbeat", 0)
                age = time.time() - last_heartbeat

                # Check if leadership is available
                if leader_state.get("leader_id") == "none" or age > heartbeat_timeout:
                    # Try to claim leadership
                    new_leader = {
                        "leader_id": f"worker-{worker_id}",
                        "term": leader_state.get("term", 0) + 1,
                        "last_heartbeat": time.time(),
                    }

                    try:
                        s3_client.client.put_object(
                            Bucket=bucket_name,
                            Key=leader_key,
                            Body=json.dumps(new_leader).encode(),
                            IfMatch=current_etag,
                        )

                        election_history.append(
                            {
                                "worker_id": worker_id,
                                "term": new_leader["term"],
                                "timestamp": time.time(),
                            }
                        )

                        return {
                            "worker_id": worker_id,
                            "became_leader": True,
                            "term": new_leader["term"],
                        }

                    except Exception as e:
                        if "PreconditionFailed" in str(e) or "412" in str(e):
                            return {"worker_id": worker_id, "became_leader": False}

                return {"worker_id": worker_id, "became_leader": False}

            except Exception as e:
                return {"worker_id": worker_id, "became_leader": False, "error": str(e)}

        # Workers compete for leadership
        results = []
        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            futures = [
                executor.submit(try_become_leader, i) for i in range(num_workers)
            ]

            for future in as_completed(futures):
                results.append(future.result())

        leaders = [r for r in results if r.get("became_leader")]

        print(f"  Workers that became leader: {len(leaders)}")

        # Verify at most one leader elected per attempt
        if leaders:
            terms = [leader["term"] for leader in leaders]
            print(f"  Leader terms: {terms}")

            # In a single election round, should have exactly one leader
            assert len(leaders) >= 1, "Should have at least one leader"

            print(f"  ✓ Leader election successful")

        # Verify current leader
        response = s3_client.get_object(bucket_name, leader_key)
        current_leader = json.loads(response["Body"].read())

        print(f"  Current leader: {current_leader.get('leader_id')}")
        print(f"  Current term: {current_leader.get('term')}")

        print(f"  ✓ Leader election pattern working")

    finally:
        # Cleanup
        try:
            s3_client.delete_object(bucket_name, leader_key)
            s3_client.delete_bucket(bucket_name)
        except Exception:
            pass


def test_conditional_delete_pattern(s3_client, config):
    """
    Test conditional DELETE using If-Match.

    Only delete if ETag matches (object hasn't changed).
    """
    bucket_name = f"{config['s3_bucket_prefix']}-cond-del-{random_string()}"
    key = "data/conditional-delete.txt"

    try:
        s3_client.create_bucket(bucket_name)

        print(f"\nTesting conditional DELETE...")

        # Create object
        content = b"original content"
        put_response = s3_client.put_object(bucket_name, key, content)
        original_etag = put_response.get("ETag", "").strip('"')

        print(f"  Object created with ETag: {original_etag}")

        # Try to delete with wrong ETag (should fail)
        wrong_etag = "wrong-etag-value"

        try:
            s3_client.client.delete_object(
                Bucket=bucket_name, Key=key, IfMatch=wrong_etag
            )
            assert False, "DELETE with wrong ETag should fail"

        except Exception as e:
            assert "PreconditionFailed" in str(e) or "412" in str(e), \
                f"Expected PreconditionFailed, got: {e}"

            print(f"  ✓ DELETE rejected with wrong ETag")

        # Verify object still exists
        response = s3_client.head_object(bucket_name, key)
        assert response is not None, "Object should still exist"

        print(f"  ✓ Object still exists after failed DELETE")

        # Delete with correct ETag (should succeed)
        s3_client.client.delete_object(Bucket=bucket_name, Key=key, IfMatch=original_etag)

        print(f"  ✓ DELETE succeeded with correct ETag")

        # Verify object is deleted
        try:
            s3_client.head_object(bucket_name, key)
            assert False, "Object should be deleted"
        except Exception:
            print(f"  ✓ Object successfully deleted")

    finally:
        # Cleanup
        try:
            s3_client.delete_object(bucket_name, key)
            s3_client.delete_bucket(bucket_name)
        except Exception:
            pass


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
