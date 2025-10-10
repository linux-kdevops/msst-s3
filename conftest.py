"""
Pytest configuration and fixtures for S3 tests

Generated-by: Claude AI
Signed-off-by: Luis Chamberlain <mcgrof@kernel.org>
"""

import pytest
import os
from tests.common.s3_client import S3Client


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
    }


@pytest.fixture(scope="function")
def s3_client(config):
    """
    S3 client fixture

    Creates an S3Client instance configured for the test environment
    """
    client = S3Client(
        endpoint_url=config["s3_endpoint"],
        access_key=config["s3_access_key"],
        secret_key=config["s3_secret_key"],
        region=config["s3_region"],
        use_ssl=config["s3_endpoint"].startswith("https"),
        verify_ssl=config["verify_ssl"],
    )

    yield client

    # Cleanup happens in test fixtures
