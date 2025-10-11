#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Test 1: Thundering Herd Consistency

Production Pattern: Multiple clients racing to create/update the same object
Real-world scenarios: Cache stampede, CDN origin updates, distributed cache population

What it tests:
- Concurrent PUT to same key from many clients
- Last-writer-wins consistency
- ETag consistency across clients
- No data corruption under contention

Why it matters:
- CDN origin servers experience this during cache misses
- Distributed caches race to populate same key
- Common in microservices with shared state
- Data corruption here would be catastrophic

Generated-by: Claude AI
Signed-off-by: Luis Chamberlain <mcgrof@kernel.org>
"""

import pytest
import threading
import hashlib
from concurrent.futures import ThreadPoolExecutor, as_completed
from tests.common.test_utils import random_string


def test_thundering_herd_consistency(s3_client, config):
    """
    100 concurrent clients PUT different content to same key.

    Verifies:
    - All PUT operations succeed (no lock contention failures)
    - Final object contains data from ONE of the PUT operations (not corrupted)
    - ETag is consistent
    - GetObject returns consistent data
    """
    bucket_name = f"{config['s3_bucket_prefix']}-thundering-{random_string()}"
    key = "hotkey/shared-resource.dat"
    num_clients = 100

    try:
        # Create bucket
        s3_client.create_bucket(bucket_name)

        # Track which client data was written
        client_data = {}
        results = []

        def client_put_operation(client_id):
            """Each client PUTs unique content"""
            content = f"client-{client_id}-data-{random_string(32)}".encode()
            content_hash = hashlib.md5(content).hexdigest()
            client_data[client_id] = (content, content_hash)

            try:
                response = s3_client.put_object(bucket_name, key, content)
                return {
                    "client_id": client_id,
                    "success": True,
                    "etag": response.get("ETag", "").strip('"'),
                    "content_hash": content_hash,
                }
            except Exception as e:
                return {"client_id": client_id, "success": False, "error": str(e)}

        # Execute concurrent PUTs
        with ThreadPoolExecutor(max_workers=num_clients) as executor:
            futures = [
                executor.submit(client_put_operation, i) for i in range(num_clients)
            ]

            for future in as_completed(futures):
                results.append(future.result())

        # Verify all operations succeeded
        successes = [r for r in results if r["success"]]
        failures = [r for r in results if not r["success"]]

        print(f"\nThundering Herd Results:")
        print(f"  Successful PUTs: {len(successes)}/{num_clients}")
        print(f"  Failed PUTs: {len(failures)}")

        # All PUTs should succeed (no lock contention)
        assert (
            len(successes) == num_clients
        ), f"Expected all {num_clients} PUTs to succeed, got {len(successes)}"

        # Get final object state
        response = s3_client.get_object(bucket_name, key)
        final_content = response["Body"].read()
        final_etag = response["ETag"].strip('"')
        final_content_hash = hashlib.md5(final_content).hexdigest()

        print(f"  Final object ETag: {final_etag}")
        print(f"  Final content hash: {final_content_hash}")

        # Verify final content matches ONE of the client uploads (not corrupted)
        matching_client = None
        for client_id, (content, content_hash) in client_data.items():
            if content == final_content:
                matching_client = client_id
                break

        assert (
            matching_client is not None
        ), "Final object content doesn't match any client upload - possible corruption!"

        print(f"  Final object matches client-{matching_client}'s upload")

        # Verify ETag is consistent - get object again and compare
        response2 = s3_client.get_object(bucket_name, key)
        second_etag = response2["ETag"].strip('"')
        second_content = response2["Body"].read()

        assert (
            second_etag == final_etag
        ), "ETag changed between consecutive GetObject calls - inconsistency!"
        assert (
            second_content == final_content
        ), "Content changed between consecutive GetObject calls - inconsistency!"

        print(f"  ✓ Consistency verified: ETag and content remain stable")

        # Verify all successful PUTs got a valid ETag
        etags = [r["etag"] for r in successes]
        assert all(etag for etag in etags), "Some PUT operations didn't return an ETag"

        print(f"  ✓ All {len(successes)} PUTs returned valid ETags")

    finally:
        # Cleanup
        try:
            s3_client.delete_object(bucket_name, key)
            s3_client.delete_bucket(bucket_name)
        except Exception:
            pass


def test_thundering_herd_versioned(s3_client, config):
    """
    Test thundering herd with versioning enabled.

    With versioning, all writes should create versions.
    Verify each version is intact and retrievable.
    """
    bucket_name = f"{config['s3_bucket_prefix']}-thundering-ver-{random_string()}"
    key = "versioned-hotkey.dat"
    num_clients = 50

    try:
        # Create bucket with versioning
        s3_client.create_bucket(bucket_name)
        s3_client.put_bucket_versioning(bucket_name, {"Status": "Enabled"})

        client_data = {}
        version_ids = []

        def client_put_with_version(client_id):
            """Each client PUTs unique content and records version"""
            content = f"versioned-client-{client_id}-{random_string(16)}".encode()
            client_data[client_id] = content

            try:
                response = s3_client.put_object(bucket_name, key, content)
                version_id = response.get("VersionId")
                return {
                    "client_id": client_id,
                    "success": True,
                    "version_id": version_id,
                    "content": content,
                }
            except Exception as e:
                return {"client_id": client_id, "success": False, "error": str(e)}

        # Execute concurrent versioned PUTs
        with ThreadPoolExecutor(max_workers=num_clients) as executor:
            futures = [
                executor.submit(client_put_with_version, i) for i in range(num_clients)
            ]

            for future in as_completed(futures):
                result = future.result()
                if result["success"]:
                    version_ids.append(result)

        print(f"\nVersioned Thundering Herd Results:")
        print(f"  Total versions created: {len(version_ids)}")

        # All operations should succeed
        assert (
            len(version_ids) == num_clients
        ), f"Expected {num_clients} versions, got {len(version_ids)}"

        # Verify each version is retrievable and intact
        for i, ver_info in enumerate(version_ids):
            version_id = ver_info["version_id"]
            expected_content = ver_info["content"]

            response = s3_client.get_object(bucket_name, key, VersionId=version_id)
            actual_content = response["Body"].read()

            assert (
                actual_content == expected_content
            ), f"Version {version_id} content corrupted!"

        print(f"  ✓ All {len(version_ids)} versions verified intact")

        # List all versions and verify count
        versions_response = s3_client.list_object_versions(bucket_name, Prefix=key)
        versions = versions_response.get("Versions", [])

        assert (
            len(versions) >= num_clients
        ), f"Expected at least {num_clients} versions listed, got {len(versions)}"

        print(f"  ✓ ListObjectVersions shows {len(versions)} versions")

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


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
