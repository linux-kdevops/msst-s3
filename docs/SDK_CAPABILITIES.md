# SDK Capability System

## Overview

The MSST-S3 test suite includes a sophisticated SDK capability system that allows
tests to run against different AWS SDK implementations and versions while
accounting for their behavioral differences.

## The Problem

Different AWS SDK implementations (boto3, aws-sdk-go, aws-sdk-java, etc.) and even
different versions of the same SDK can have different behaviors:

- **URL Encoding**: Some SDKs treat `+` as a space in URLs, others don't
- **Retry Policies**: Different retry modes (standard, adaptive, legacy)
- **Checksum Algorithms**: Some default to CRC32C, others to MD5
- **Addressing Styles**: Virtual-hosted vs path-style bucket addressing
- **Request Signing**: SigV4 with chunked encoding vs other methods
- **API Defaults**: ListObjectsV2 vs ListObjects (v1)

Without accounting for these differences, tests can produce false failures when
testing legitimate SDK-specific behavior.

## The Solution: Capability Profiles

The capability system models SDK behaviors as **capability profiles**. Each profile
is a set of flags that describe how a particular SDK version behaves.

### Example Capability Profile

```json
{
  "sdk": "boto3",
  "version": "1.26.0",
  "profile": {
    "sigv4_chunked": true,
    "unsigned_payload_allowed": true,
    "virtual_hosted_default": true,
    "list_objects_v1": false,
    "list_objects_url_plus_treated_as_space": false,
    "retry_mode": "standard",
    "follows_301_region_redirect": true,
    "follows_307_on_put": true,
    "crc32c_default": false
  },
  "sources": ["mapping", "probes"]
}
```

## How It Works

The capability system uses a three-layer approach:

1. **Static Mapping**: Pre-defined capability profiles for known SDK/version combinations
2. **Runtime Probes**: Dynamic detection of SDK behavior (optional)
3. **Manual Overrides**: Explicit capability overrides via JSON files

These three sources are merged (last wins) to produce a final capability profile
that tests can use.

## Usage

### 1. Command Line (Recommended)

```bash
# Test with specific SDK
./scripts/test-runner.py --sdk boto3 --sdk-version latest -v

# Test with Go SDK v2
./scripts/test-runner.py --sdk aws-sdk-go-v2 --sdk-version 1.30.0 -v

# Use a defconfig file
./scripts/test-runner.py --defconfig defconfigs/boto3_latest.yaml -v
```

### 2. Configuration File

Edit `s3_config.yaml`:

```yaml
s3_sdk: "boto3"
s3_sdk_version: "latest"
s3_cap_profile_override: False
s3_cap_profile_json: ""
s3_caps_json_path: ".sdk_capabilities.json"
```

### 3. Environment Variables

```bash
export S3_SDK=boto3
export S3_SDK_VERSION=1.26.0
pytest tests/edge/test_sdk_capability_example.py -v
```

### 4. Defconfig Files

```bash
# Run with a predefined SDK configuration
./scripts/test-runner.py --defconfig defconfigs/boto3_latest.yaml
```

See `defconfigs/README.md` for available defconfigs.

## Writing Capability-Aware Tests

Tests can access capabilities through fixtures or the S3Client:

### Method 1: Using sdk_capabilities Fixture

```python
def test_url_encoding(s3_client, sdk_capabilities, config):
    """Test URL encoding behavior based on SDK capabilities"""
    bucket_name = f"{config['s3_bucket_prefix']}-test"

    # Create and upload object with '+' in key
    s3_client.create_bucket(bucket_name)
    key = "test+object.txt"
    s3_client.put_object(bucket_name, key, b"data")

    # Get capability
    caps = sdk_capabilities.get("profile", {})
    plus_as_space = caps.get("list_objects_url_plus_treated_as_space", False)

    # Adapt test behavior
    if plus_as_space:
        prefix = "test "  # Space
    else:
        prefix = "test+"  # Plus

    objects = s3_client.list_objects(bucket_name, prefix=prefix)
    assert any(obj["Key"] == key for obj in objects)

    # Cleanup
    s3_client.delete_object(bucket_name, key)
    s3_client.delete_bucket(bucket_name)
```

### Method 2: Using S3Client Methods

```python
def test_checksum_behavior(s3_client, config):
    """Test checksum behavior using client capabilities"""

    # Check capability directly from client
    uses_crc32c = s3_client.has_capability("crc32c_default")

    bucket_name = f"{config['s3_bucket_prefix']}-checksum"
    s3_client.create_bucket(bucket_name)

    response = s3_client.put_object(bucket_name, "test.txt", b"data")

    if uses_crc32c:
        # Verify CRC32C checksum in response
        assert "ChecksumCRC32C" in response or "ETag" in response
    else:
        # Verify MD5 ETag
        assert "ETag" in response

    s3_client.delete_object(bucket_name, "test.txt")
    s3_client.delete_bucket(bucket_name)
```

### Method 3: Access All Capabilities

```python
def test_with_capabilities(s3_client):
    """Access all capabilities from the client"""

    # Get all capabilities
    caps = s3_client.capabilities

    print(f"SigV4 chunked: {caps.get('sigv4_chunked')}")
    print(f"Retry mode: {caps.get('retry_mode')}")
    print(f"Virtual hosted: {caps.get('virtual_hosted_default')}")
```

## Capability Flags Reference

### Authentication & Signing

- **sigv4_chunked**: SDK uses SigV4 with chunked encoding
- **unsigned_payload_allowed**: SDK supports UNSIGNED-PAYLOAD header

### Addressing & Encoding

- **virtual_hosted_default**: SDK defaults to virtual-hosted style URLs
- **list_objects_v1**: SDK defaults to ListObjects v1 API
- **list_objects_url_plus_treated_as_space**: SDK treats '+' as space in URL encoding

### Retries & Redirects

- **retry_mode**: Retry policy mode ("standard", "adaptive", "legacy")
- **follows_301_region_redirect**: SDK follows 301 redirects for cross-region requests
- **follows_307_on_put**: SDK replays request body on 307/308 redirects

### Checksums & I/O

- **crc32c_default**: SDK uses CRC32C checksums by default

## Adding New SDKs

To add support for a new SDK or version:

1. **Update Static Mapping** in `tests/common/sdk_capabilities.py`:

```python
STATIC_CAPABILITY_MAPPING = [
    # ... existing entries ...
    {
        "sdk": "new-sdk-name",
        "version_constraint": ">= 1.0.0, < 2.0.0",
        "profile": {
            "sigv4_chunked": True,
            "unsigned_payload_allowed": True,
            # ... other capabilities ...
        },
        "rationale": "Description of behavioral characteristics"
    },
]
```

2. **Create a Defconfig** in `defconfigs/`:

```yaml
---
s3_sdk: "new-sdk-name"
s3_sdk_version: "latest"
```

3. **Test the Profile**:

```bash
./scripts/test-runner.py --sdk new-sdk-name --sdk-version 1.0.0 -v
```

## Manual Capability Overrides

For special cases, you can override capabilities:

1. Create an override JSON file:

```json
{
  "list_objects_url_plus_treated_as_space": true,
  "retry_mode": "adaptive"
}
```

2. Use it in configuration:

```yaml
s3_cap_profile_override: True
s3_cap_profile_json: "path/to/overrides.json"
```

Or via command line:

```bash
export S3_CAP_PROFILE_OVERRIDE=1
export S3_CAP_PROFILE_JSON=/path/to/overrides.json
./scripts/test-runner.py -v
```

## Runtime Probes (Advanced)

The capability system supports runtime probes that automatically detect SDK behavior.
To implement probes, edit `tests/common/sdk_capabilities.py`:

```python
def run_probes_for_sdk(spec: SDKSpec, endpoint_hint: Optional[str] = None) -> ProbeResult:
    """
    Implement minimal probes to detect SDK behavior
    """
    result = ProbeResult()

    # Example: Probe for URL encoding behavior
    try:
        # Create test client with SDK
        # Upload object with '+'
        # Try listing with different prefixes
        # Set result.list_objects_url_plus_treated_as_space based on what works
        pass
    except:
        pass

    return result
```

## CI/CD Integration

### Matrix Testing Example

```yaml
# GitHub Actions example
jobs:
  test-sdks:
    strategy:
      matrix:
        sdk: [boto3, aws-sdk-go-v2, aws-sdk-java-v2]
        version: [latest, lts]
    steps:
      - name: Run tests
        run: |
          ./scripts/test-runner.py \
            --sdk ${{ matrix.sdk }} \
            --sdk-version ${{ matrix.version }} \
            --output-format junit
```

### Makefile Example

```makefile
test-boto3:
	./scripts/test-runner.py --defconfig defconfigs/boto3_latest.yaml

test-go-v2:
	./scripts/test-runner.py --defconfig defconfigs/aws_sdk_go_v2.yaml

test-all-sdks: test-boto3 test-go-v2
	@echo "All SDK tests completed"
```

## Best Practices

1. **Always Check Capabilities**: When testing edge cases related to URL encoding,
   retries, checksums, or other SDK-specific behavior, check the appropriate capability.

2. **Document Assumptions**: If your test relies on a specific capability, document
   it clearly in the test docstring.

3. **Fail Explicitly**: If a test requires a capability that's not available, skip
   the test or fail with a clear message:

   ```python
   if not s3_client.has_capability("sigv4_chunked"):
       pytest.skip("This test requires SigV4 chunked encoding support")
   ```

4. **Test Behavior, Not Implementation**: Focus on testing S3 API behavior, not
   specific SDK implementation details.

5. **Use Defconfigs**: For common SDK configurations, use defconfig files rather
   than manual configuration.

## Troubleshooting

### Capabilities Not Loading

```bash
# Check if SDK capabilities module is available
python3 -c "from tests.common.sdk_capabilities import SDKSpec; print('OK')"

# Verify capability file was generated
cat .sdk_capabilities.json
```

### Incorrect Capabilities

```bash
# Force regeneration
rm .sdk_capabilities.json
./scripts/test-runner.py --sdk boto3 --sdk-version latest -v

# Use manual override
echo '{"retry_mode": "standard"}' > custom_caps.json
export S3_CAP_PROFILE_OVERRIDE=1
export S3_CAP_PROFILE_JSON=custom_caps.json
./scripts/test-runner.py -v
```

### Tests Failing Due to SDK Differences

1. Check if the failure is due to a known SDK difference
2. Add capability check to the test
3. Adapt expectations based on capability
4. If needed, update the static capability mapping

## Examples

See `tests/edge/test_sdk_capability_example.py` for complete examples of
capability-aware tests.

---

Generated-by: Claude AI
Signed-off-by: Luis Chamberlain <mcgrof@kernel.org>
