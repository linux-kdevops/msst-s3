"""
Pytest configuration and fixtures for S3 tests

Generated-by: Claude AI
Signed-off-by: Luis Chamberlain <mcgrof@kernel.org>
"""

import pytest
import os
import json
from pathlib import Path
from tests.common.s3_client import S3Client

# Try to import SDK capabilities module
try:
    from tests.common.sdk_capabilities import (
        SDKSpec,
        build_caps_document,
        load_caps_for_tests,
    )

    SDK_CAPS_AVAILABLE = True
except ImportError:
    SDK_CAPS_AVAILABLE = False


@pytest.fixture(scope="session")
def config():
    """
    Test configuration fixture

    Returns configuration for S3 testing
    """
    return {
        "s3_endpoint": os.getenv("S3_ENDPOINT", "http://localhost:9000"),
        "s3_access_key": os.getenv("S3_ACCESS_KEY", "minioadmin"),
        "s3_secret_key": os.getenv("S3_SECRET_KEY", "minioadmin"),
        "s3_region": os.getenv("S3_REGION", "us-east-1"),
        "s3_bucket_prefix": os.getenv("S3_BUCKET_PREFIX", "msst-test"),
        "verify_ssl": os.getenv("S3_VERIFY_SSL", "false").lower() == "true",
        "s3_sdk": os.getenv("S3_SDK", "boto3"),
        "s3_sdk_version": os.getenv("S3_SDK_VERSION", "latest"),
    }


@pytest.fixture(scope="session")
def sdk_capabilities(config):
    """
    SDK capability profile fixture

    Generates or loads SDK capability profile for the current SDK/version
    """
    if not SDK_CAPS_AVAILABLE:
        # Return default capabilities if module not available
        return {
            "sdk": config["s3_sdk"],
            "version": config["s3_sdk_version"],
            "profile": {
                "sigv4_chunked": True,
                "unsigned_payload_allowed": True,
                "virtual_hosted_default": True,
                "list_objects_v1": False,
                "list_objects_url_plus_treated_as_space": False,
                "retry_mode": "standard",
                "follows_301_region_redirect": True,
                "follows_307_on_put": True,
                "crc32c_default": False,
            },
            "sources": ["defaults"],
        }

    # Check if capabilities were already generated
    caps_path = Path(os.getenv("S3_CAPS_JSON_PATH", ".sdk_capabilities.json"))
    if caps_path.exists():
        try:
            return load_caps_for_tests(str(caps_path))
        except Exception:
            pass  # Fall through to generate new capabilities

    # Generate capabilities
    try:
        spec = SDKSpec(name=config["s3_sdk"], version=config["s3_sdk_version"])
        override_json = os.getenv("S3_CAP_PROFILE_JSON")
        force_override = os.getenv("S3_CAP_PROFILE_OVERRIDE", "0") == "1"
        endpoint_hint = config.get("s3_endpoint")

        capabilities = build_caps_document(
            spec=spec,
            endpoint_hint=endpoint_hint,
            override_json_path=override_json if force_override else None,
            force_override=force_override,
        )

        # Save for future use
        with open(caps_path, "w") as f:
            json.dump(capabilities, f, indent=2)

        return capabilities

    except Exception as e:
        # Return defaults on error
        print(f"Warning: Failed to generate SDK capabilities: {e}")
        return {
            "sdk": config["s3_sdk"],
            "version": config["s3_sdk_version"],
            "profile": {},
            "sources": ["error-fallback"],
        }


@pytest.fixture(scope="function")
def s3_client(config, sdk_capabilities):
    """
    S3 client fixture

    Creates an S3Client instance configured for the test environment
    with SDK capability awareness
    """
    client = S3Client(
        endpoint_url=config["s3_endpoint"],
        access_key=config["s3_access_key"],
        secret_key=config["s3_secret_key"],
        region=config["s3_region"],
        use_ssl=config["s3_endpoint"].startswith("https"),
        verify_ssl=config["verify_ssl"],
        capabilities=sdk_capabilities.get("profile"),
    )

    yield client

    # Cleanup happens in test fixtures
