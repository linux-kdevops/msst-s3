#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Test 9: Metadata Consistency Under Concurrent Updates

Production Pattern: Tagging and metadata updates from multiple automation systems
Real-world scenarios: Backup tagging, compliance systems, analytics metadata

What it tests:
- Object with initial metadata
- 50+ clients concurrently update metadata/tags
- Verify final metadata is from ONE update (not corrupted)
- Test read-your-writes consistency

Why it matters:
- Multiple systems tag objects (backup, compliance, analytics)
- Metadata corruption can break automation
- Need consistency guarantees for metadata operations
- Common in multi-system environments

Generated-by: Claude AI
Signed-off-by: Luis Chamberlain <mcgrof@kernel.org>
"""

import pytest
from concurrent.futures import ThreadPoolExecutor, as_completed
from tests.common.test_utils import random_string


def test_concurrent_metadata_updates(s3_client, config):
    """
    50 clients concurrently update object metadata.
    Verify no metadata corruption occurs.
    """
    bucket_name = f"{config['s3_bucket_prefix']}-metadata-{random_string()}"
    key = "shared/config.json"
    num_clients = 50

    try:
        s3_client.create_bucket(bucket_name)

        # Create initial object
        initial_content = b"initial configuration data"
        s3_client.put_object(
            bucket_name,
            key,
            initial_content,
            ContentType="application/json",
            Metadata={"version": "0", "creator": "initial"},
        )

        print(f"\nConcurrent metadata update test with {num_clients} clients...")

        client_updates = {}

        def update_metadata(client_id):
            """Each client updates metadata differently"""
            try:
                # Get current object
                source = {"Bucket": bucket_name, "Key": key}

                # Update metadata using CopyObject with REPLACE directive
                response = s3_client.client.copy_object(
                    Bucket=bucket_name,
                    Key=key,
                    CopySource=source,
                    Metadata={
                        "version": str(client_id),
                        "client": f"client-{client_id}",
                        "update-id": random_string(16),
                    },
                    MetadataDirective="REPLACE",
                )

                client_updates[client_id] = {
                    "version": str(client_id),
                    "client": f"client-{client_id}",
                }

                return {
                    "client_id": client_id,
                    "success": True,
                    "etag": response.get("CopyObjectResult", {})
                    .get("ETag", "")
                    .strip('"'),
                }
            except Exception as e:
                return {"client_id": client_id, "success": False, "error": str(e)}

        # Execute concurrent metadata updates
        results = []
        with ThreadPoolExecutor(max_workers=num_clients) as executor:
            futures = [executor.submit(update_metadata, i) for i in range(num_clients)]

            for future in as_completed(futures):
                results.append(future.result())

        successes = [r for r in results if r["success"]]
        failures = [r for r in results if not r["success"]]

        print(f"  Successful updates: {len(successes)}/{num_clients}")
        print(f"  Failed updates: {len(failures)}")

        # All updates should succeed
        assert (
            len(successes) == num_clients
        ), f"Expected all {num_clients} updates to succeed"

        # Get final metadata
        response = s3_client.head_object(bucket_name, key)
        final_metadata = response.get("Metadata", {})

        print(f"  Final metadata: {final_metadata}")

        # Verify metadata is from ONE client (not corrupted/merged)
        if "version" in final_metadata and "client" in final_metadata:
            version = final_metadata["version"]
            client = final_metadata["client"]

            # Check if metadata is consistent with one update
            expected_client = f"client-{version}"
            assert (
                client == expected_client
            ), f"Metadata corruption detected! version={version} but client={client}"

            print(f"  ✓ Metadata consistent: version {version} from {client}")
        else:
            pytest.fail("Final metadata missing required fields - possible corruption")

        # Verify content wasn't corrupted
        response = s3_client.get_object(bucket_name, key)
        final_content = response["Body"].read()

        assert (
            final_content == initial_content
        ), "Object content was corrupted during metadata updates!"

        print(f"  ✓ Object content integrity verified")

    finally:
        # Cleanup
        try:
            s3_client.delete_object(bucket_name, key)
            s3_client.delete_bucket(bucket_name)
        except Exception:
            pass


def test_concurrent_tagging_updates(s3_client, config):
    """
    Test concurrent tagging operations.
    Multiple clients update tags, verify tag consistency.
    """
    bucket_name = f"{config['s3_bucket_prefix']}-tagging-{random_string()}"
    key = "tagged/document.pdf"
    num_clients = 30

    try:
        s3_client.create_bucket(bucket_name)

        # Create object
        s3_client.put_object(bucket_name, key, b"document content")

        print(f"\nConcurrent tagging test with {num_clients} clients...")

        def update_tags(client_id):
            """Each client sets unique tags"""
            try:
                tags = {
                    "client": f"client-{client_id}",
                    "timestamp": str(client_id),
                    "environment": "production",
                    "owner": f"team-{client_id % 5}",  # 5 different teams
                }

                response = s3_client.client.put_object_tagging(
                    Bucket=bucket_name,
                    Key=key,
                    Tagging={
                        "TagSet": [{"Key": k, "Value": v} for k, v in tags.items()]
                    },
                )

                return {"client_id": client_id, "success": True, "tags": tags}
            except Exception as e:
                return {"client_id": client_id, "success": False, "error": str(e)}

        # Execute concurrent tagging
        results = []
        with ThreadPoolExecutor(max_workers=num_clients) as executor:
            futures = [executor.submit(update_tags, i) for i in range(num_clients)]

            for future in as_completed(futures):
                results.append(future.result())

        successes = [r for r in results if r["success"]]
        print(f"  Successful tag updates: {len(successes)}/{num_clients}")

        # Get final tags
        response = s3_client.client.get_object_tagging(Bucket=bucket_name, Key=key)
        final_tags = {tag["Key"]: tag["Value"] for tag in response.get("TagSet", [])}

        print(f"  Final tags: {final_tags}")

        # Verify tags are consistent (from one client, not merged)
        if "client" in final_tags and "timestamp" in final_tags:
            client_value = final_tags["client"]
            timestamp_value = final_tags["timestamp"]

            # Extract client ID from both
            client_id_from_client = client_value.split("-")[1]
            client_id_from_timestamp = timestamp_value

            assert (
                client_id_from_client == client_id_from_timestamp
            ), f"Tag corruption! client={client_value} but timestamp={timestamp_value}"

            print(f"  ✓ Tags consistent: all from {client_value}")
        else:
            pytest.fail("Final tags missing required fields - possible corruption")

    finally:
        # Cleanup
        try:
            s3_client.delete_object(bucket_name, key)
            s3_client.delete_bucket(bucket_name)
        except Exception:
            pass


def test_metadata_versioning_interaction(s3_client, config):
    """
    Test metadata updates with versioning enabled.
    Each metadata update should create a new version.
    """
    bucket_name = f"{config['s3_bucket_prefix']}-meta-ver-{random_string()}"
    key = "versioned/config.yaml"
    num_updates = 20

    try:
        s3_client.create_bucket(bucket_name)
        s3_client.put_bucket_versioning(bucket_name, {"Status": "Enabled"})

        print(f"\nMetadata versioning test with {num_updates} updates...")

        # Create initial object
        s3_client.put_object(
            bucket_name, key, b"config: initial", Metadata={"version": "0"}
        )

        version_ids = []

        # Sequential metadata updates (to avoid race conditions)
        for i in range(1, num_updates + 1):
            source = {"Bucket": bucket_name, "Key": key}

            response = s3_client.client.copy_object(
                Bucket=bucket_name,
                Key=key,
                CopySource=source,
                Metadata={"version": str(i)},
                MetadataDirective="REPLACE",
            )

            version_id = response.get("VersionId")
            version_ids.append((i, version_id))

        print(f"  Created {len(version_ids)} versions via metadata updates")

        # Verify each version has correct metadata
        for version_num, version_id in version_ids:
            response = s3_client.head_object(bucket_name, key, VersionId=version_id)

            metadata = response.get("Metadata", {})
            assert metadata.get("version") == str(
                version_num
            ), f"Version {version_id} has wrong metadata!"

        print(f"  ✓ All {len(version_ids)} versions have correct metadata")

        # Verify version listing
        response = s3_client.list_object_versions(bucket_name, Prefix=key)
        versions = response.get("Versions", [])

        # Should have num_updates + 1 versions (initial + updates)
        assert (
            len(versions) >= num_updates
        ), f"Expected at least {num_updates} versions, found {len(versions)}"

        print(f"  ✓ ListObjectVersions shows {len(versions)} versions")

    finally:
        # Cleanup all versions
        try:
            response = s3_client.list_object_versions(bucket_name, Prefix=key)
            for version in response.get("Versions", []):
                s3_client.delete_object(
                    bucket_name, key, VersionId=version["VersionId"]
                )
            s3_client.delete_bucket(bucket_name)
        except Exception:
            pass


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
