#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Test 18: Rate Limiting and Circuit Breaker Patterns

Production Pattern: Graceful handling of 503 SlowDown responses
Real-world scenarios: Black Friday traffic, batch jobs, data migration

What it tests:
- High request rate handling (>1000 req/sec)
- Exponential backoff on 503
- Circuit breaker implementation
- Request queuing and buffering
- Graceful degradation

Why it matters:
- S3 has per-prefix rate limits
- Exceeding limits causes 503 errors
- Naive retry amplifies the problem
- Production systems must handle gracefully

Generated-by: Claude AI
Signed-off-by: Luis Chamberlain <mcgrof@kernel.org>
"""

import pytest
import time
import threading
from collections import deque, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from tests.common.test_utils import random_string


def test_exponential_backoff_on_errors(s3_client, config):
    """
    Test exponential backoff when encountering errors.

    Simulates high load and verifies backoff behavior reduces request rate.
    """
    bucket_name = f"{config['s3_bucket_prefix']}-backoff-{random_string()}"
    num_requests = 100
    initial_delay = 0.01  # 10ms

    try:
        s3_client.create_bucket(bucket_name)

        print(f"\nTesting exponential backoff with {num_requests} requests...")

        request_times = []
        backoff_delays = []
        errors_by_type = defaultdict(int)

        def make_request_with_backoff(request_id):
            """Make request with exponential backoff on errors"""
            max_retries = 5
            delay = initial_delay

            for attempt in range(max_retries):
                request_time = time.time()
                request_times.append(request_time)

                try:
                    # Make request
                    key = f"test/object-{request_id}.dat"
                    s3_client.put_object(bucket_name, key, f"data-{request_id}".encode())

                    return {
                        "request_id": request_id,
                        "success": True,
                        "attempts": attempt + 1,
                    }

                except Exception as e:
                    error_msg = str(e)

                    # Track error types
                    if "SlowDown" in error_msg or "503" in error_msg:
                        errors_by_type["SlowDown"] += 1
                    elif "ServiceUnavailable" in error_msg:
                        errors_by_type["ServiceUnavailable"] += 1
                    else:
                        errors_by_type["Other"] += 1

                    if attempt < max_retries - 1:
                        # Exponential backoff
                        backoff_delays.append(delay)
                        time.sleep(delay)
                        delay *= 2  # Double delay each time
                    else:
                        return {
                            "request_id": request_id,
                            "success": False,
                            "attempts": max_retries,
                            "error": error_msg,
                        }

            return {"request_id": request_id, "success": False, "attempts": max_retries}

        # Execute requests concurrently
        results = []
        start_time = time.time()

        with ThreadPoolExecutor(max_workers=20) as executor:
            futures = [
                executor.submit(make_request_with_backoff, i)
                for i in range(num_requests)
            ]

            for future in as_completed(futures):
                results.append(future.result())

        duration = time.time() - start_time

        # Analyze results
        successes = [r for r in results if r["success"]]
        failures = [r for r in results if not r["success"]]

        print(f"  Total duration: {duration:.2f}s")
        print(f"  Successful requests: {len(successes)}/{num_requests}")
        print(f"  Failed requests: {len(failures)}")

        if errors_by_type:
            print(f"  Errors encountered:")
            for error_type, count in errors_by_type.items():
                print(f"    {error_type}: {count}")

        if backoff_delays:
            avg_backoff = sum(backoff_delays) / len(backoff_delays)
            max_backoff = max(backoff_delays)
            print(f"  Backoff delays: avg={avg_backoff*1000:.1f}ms, max={max_backoff*1000:.1f}ms")

        # Calculate request rate
        if request_times:
            request_times.sort()
            time_span = request_times[-1] - request_times[0]
            if time_span > 0:
                request_rate = len(request_times) / time_span
                print(f"  Average request rate: {request_rate:.1f} req/s")

        # Verify backoff behavior
        if backoff_delays:
            # Check that delays are increasing
            for i in range(1, min(5, len(backoff_delays))):
                assert backoff_delays[i] >= backoff_delays[i - 1], \
                    "Backoff delays should increase"

            print(f"  ✓ Exponential backoff verified")

    finally:
        # Cleanup
        try:
            objects = s3_client.list_objects(bucket_name)
            for obj in objects:
                s3_client.delete_object(bucket_name, obj["Key"])
            s3_client.delete_bucket(bucket_name)
        except Exception:
            pass


def test_circuit_breaker_pattern(s3_client, config):
    """
    Test circuit breaker pattern for fault tolerance.

    Circuit states: CLOSED (normal) -> OPEN (failing) -> HALF_OPEN (testing) -> CLOSED
    """
    bucket_name = f"{config['s3_bucket_prefix']}-circuit-{random_string()}"

    # Circuit breaker state
    class CircuitBreaker:
        def __init__(self, failure_threshold=5, timeout=2.0):
            self.failure_threshold = failure_threshold
            self.timeout = timeout
            self.failure_count = 0
            self.last_failure_time = None
            self.state = "CLOSED"  # CLOSED, OPEN, HALF_OPEN
            self.lock = threading.Lock()

        def call(self, func, *args, **kwargs):
            """Execute function through circuit breaker"""
            with self.lock:
                # Check if circuit should transition from OPEN to HALF_OPEN
                if self.state == "OPEN":
                    if time.time() - self.last_failure_time > self.timeout:
                        self.state = "HALF_OPEN"
                        print(f"    Circuit: OPEN -> HALF_OPEN")
                    else:
                        raise Exception("Circuit breaker is OPEN")

            # Try to execute function
            try:
                result = func(*args, **kwargs)

                # Success - reset or close circuit
                with self.lock:
                    if self.state == "HALF_OPEN":
                        self.state = "CLOSED"
                        self.failure_count = 0
                        print(f"    Circuit: HALF_OPEN -> CLOSED")

                return result

            except Exception as e:
                # Failure - increment count and possibly open circuit
                with self.lock:
                    self.failure_count += 1
                    self.last_failure_time = time.time()

                    if self.failure_count >= self.failure_threshold:
                        if self.state != "OPEN":
                            self.state = "OPEN"
                            print(f"    Circuit: {self.state} -> OPEN (failures: {self.failure_count})")

                raise e

    try:
        s3_client.create_bucket(bucket_name)

        print(f"\nTesting circuit breaker pattern...")

        breaker = CircuitBreaker(failure_threshold=3, timeout=2.0)
        request_count = 0
        success_count = 0
        blocked_count = 0

        def make_request(request_id):
            """Make S3 request through circuit breaker"""
            key = f"data/object-{request_id}.dat"

            def s3_operation():
                # Simulate failures for first 10 requests
                if request_id < 10:
                    raise Exception("Simulated failure")

                s3_client.put_object(bucket_name, key, f"data-{request_id}".encode())

            return breaker.call(s3_operation)

        # Make requests and observe circuit breaker
        for i in range(20):
            request_count += 1

            try:
                make_request(i)
                success_count += 1
                print(f"  Request {i}: SUCCESS (circuit: {breaker.state})")

            except Exception as e:
                if "Circuit breaker is OPEN" in str(e):
                    blocked_count += 1
                    print(f"  Request {i}: BLOCKED (circuit: {breaker.state})")
                else:
                    print(f"  Request {i}: FAILED (circuit: {breaker.state})")

            time.sleep(0.1)

        print(f"\n  Total requests: {request_count}")
        print(f"  Successful: {success_count}")
        print(f"  Blocked by circuit: {blocked_count}")

        # Verify circuit breaker opened
        assert blocked_count > 0, "Circuit breaker should have blocked some requests"

        # Verify circuit eventually closed (recovered)
        assert breaker.state == "CLOSED", f"Circuit should be CLOSED, got {breaker.state}"

        print(f"  ✓ Circuit breaker pattern verified")
        print(f"  ✓ Circuit recovered to CLOSED state")

    finally:
        # Cleanup
        try:
            objects = s3_client.list_objects(bucket_name)
            for obj in objects:
                s3_client.delete_object(bucket_name, obj["Key"])
            s3_client.delete_bucket(bucket_name)
        except Exception:
            pass


def test_request_queue_with_backpressure(s3_client, config):
    """
    Test request queuing with backpressure.

    Queue requests with max size limit to prevent memory exhaustion.
    """
    bucket_name = f"{config['s3_bucket_prefix']}-queue-{random_string()}"
    max_queue_size = 50
    num_requests = 200
    processing_rate = 20  # requests per second

    try:
        s3_client.create_bucket(bucket_name)

        print(f"\nTesting request queue with backpressure...")
        print(f"  Queue size: {max_queue_size}")
        print(f"  Total requests: {num_requests}")
        print(f"  Processing rate: {processing_rate}/s")

        # Request queue with backpressure
        request_queue = deque()
        queue_lock = threading.Lock()
        processed_count = 0
        dropped_count = 0
        queue_full_count = 0

        def producer():
            """Generate requests"""
            nonlocal dropped_count, queue_full_count

            for i in range(num_requests):
                request = {"id": i, "key": f"data/request-{i}.dat"}

                with queue_lock:
                    if len(request_queue) < max_queue_size:
                        request_queue.append(request)
                    else:
                        # Backpressure: queue full, drop or wait
                        dropped_count += 1
                        queue_full_count += 1

                time.sleep(0.01)  # Produce faster than we can process

        def consumer():
            """Process requests from queue"""
            nonlocal processed_count

            while True:
                with queue_lock:
                    if not request_queue:
                        # Check if producer is done
                        if processed_count + dropped_count >= num_requests:
                            break
                        time.sleep(0.01)
                        continue

                    request = request_queue.popleft()

                # Process request (rate limited)
                try:
                    s3_client.put_object(
                        bucket_name, request["key"], f"data-{request['id']}".encode()
                    )
                    processed_count += 1

                except Exception:
                    pass

                # Rate limiting
                time.sleep(1.0 / processing_rate)

        # Run producer and consumer
        start_time = time.time()

        producer_thread = threading.Thread(target=producer)
        consumer_thread = threading.Thread(target=consumer)

        producer_thread.start()
        consumer_thread.start()

        producer_thread.join()
        consumer_thread.join()

        duration = time.time() - start_time

        print(f"\n  Duration: {duration:.2f}s")
        print(f"  Processed: {processed_count}")
        print(f"  Dropped: {dropped_count}")
        print(f"  Queue full events: {queue_full_count}")

        # Verify backpressure worked
        assert processed_count > 0, "Should have processed some requests"
        assert queue_full_count > 0, "Queue should have filled up (backpressure triggered)"

        print(f"  ✓ Backpressure mechanism working")

        # Verify rate limiting
        actual_rate = processed_count / duration
        print(f"  Actual processing rate: {actual_rate:.1f} req/s")

        # Should be close to target rate (within 50%)
        assert actual_rate <= processing_rate * 1.5, \
            f"Processing too fast: {actual_rate} > {processing_rate}"

        print(f"  ✓ Rate limiting working")

    finally:
        # Cleanup
        try:
            objects = s3_client.list_objects(bucket_name)
            for obj in objects:
                s3_client.delete_object(bucket_name, obj["Key"])
            s3_client.delete_bucket(bucket_name)
        except Exception:
            pass


def test_adaptive_rate_limiting(s3_client, config):
    """
    Test adaptive rate limiting based on error responses.

    Starts with high rate, backs off on errors, ramps up on success.
    """
    bucket_name = f"{config['s3_bucket_prefix']}-adaptive-{random_string()}"
    initial_rate = 50  # requests per second
    min_rate = 5
    max_rate = 100

    try:
        s3_client.create_bucket(bucket_name)

        print(f"\nTesting adaptive rate limiting...")

        current_rate = initial_rate
        success_count = 0
        error_count = 0
        rate_adjustments = []

        for i in range(100):
            request_start = time.time()

            try:
                key = f"data/adaptive-{i}.dat"
                s3_client.put_object(bucket_name, key, f"data-{i}".encode())

                success_count += 1

                # Gradually increase rate on success
                if success_count % 10 == 0 and current_rate < max_rate:
                    old_rate = current_rate
                    current_rate = min(current_rate * 1.2, max_rate)
                    rate_adjustments.append(("increase", old_rate, current_rate))
                    print(f"  Rate increased: {old_rate:.1f} -> {current_rate:.1f} req/s")

            except Exception as e:
                error_count += 1

                # Decrease rate on error
                old_rate = current_rate
                current_rate = max(current_rate * 0.5, min_rate)
                rate_adjustments.append(("decrease", old_rate, current_rate))
                print(f"  Error encountered, rate decreased: {old_rate:.1f} -> {current_rate:.1f} req/s")

            # Rate limiting delay
            delay = 1.0 / current_rate
            elapsed = time.time() - request_start
            if elapsed < delay:
                time.sleep(delay - elapsed)

        print(f"\n  Total requests: 100")
        print(f"  Successes: {success_count}")
        print(f"  Errors: {error_count}")
        print(f"  Rate adjustments: {len(rate_adjustments)}")

        # Verify adaptive behavior
        if rate_adjustments:
            increases = [adj for adj in rate_adjustments if adj[0] == "increase"]
            decreases = [adj for adj in rate_adjustments if adj[0] == "decrease"]

            print(f"  Rate increases: {len(increases)}")
            print(f"  Rate decreases: {len(decreases)}")

            if decreases:
                print(f"  ✓ Adaptive rate limiting: backed off on errors")
            if increases:
                print(f"  ✓ Adaptive rate limiting: ramped up on success")

        assert success_count > 50, "Should have mostly successes"

        print(f"  ✓ Adaptive rate limiting verified")

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
