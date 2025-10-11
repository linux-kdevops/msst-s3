# SDK Capability System Implementation Summary

## What Was Implemented

A comprehensive SDK capability profile system has been integrated into the MSST-S3 test suite to handle behavioral differences between different AWS SDK implementations and versions.

## The Problem Solved

Different AWS SDKs (boto3, aws-sdk-go-v2, aws-sdk-java-v2, etc.) have different behaviors:
- URL encoding varies ('+' treated as space vs '+')
- Different retry policies (standard, adaptive, legacy)
- Different checksum algorithms (CRC32C vs MD5)
- Different addressing styles (virtual-hosted vs path-style)
- Different API defaults (ListObjectsV2 vs ListObjects v1)

Without this system, tests would produce false failures when testing against different SDKs.

## Files Created/Modified

### Created Files

1. **tests/common/sdk_capabilities.py** (moved from AWS-SDKs.py)
   - Core capability profile system
   - SDK specification and version parsing
   - Static capability mappings for known SDKs
   - Runtime probe framework (extensible)
   - Manual override support

2. **tests/edge/test_sdk_capability_example.py**
   - Example tests demonstrating capability-aware testing
   - Shows how to adapt test expectations based on SDK behavior
   - Multiple test patterns (URL encoding, checksums, retry modes)

3. **defconfigs/** (Directory with SDK configuration files)
   - `boto3_latest.yaml` - Latest boto3 configuration
   - `boto3_1.26.yaml` - Specific boto3 version
   - `aws_sdk_go_v2.yaml` - Go SDK v2 configuration
   - `aws_sdk_java_v2.yaml` - Java SDK v2 configuration
   - `README.md` - Documentation for defconfig usage

4. **SDK_CAPABILITIES.md**
   - Comprehensive documentation of the capability system
   - Usage examples
   - Capability flags reference
   - Best practices
   - Troubleshooting guide

5. **SDK_IMPLEMENTATION_SUMMARY.md** (this file)
   - Summary of what was implemented

### Modified Files

1. **s3_config.yaml**
   - Added SDK selection fields (`s3_sdk`, `s3_sdk_version`)
   - Added capability profile configuration options
   - Added path for storing resolved capabilities

2. **tests/common/s3_client.py**
   - Added `capabilities` parameter to `__init__`
   - Added default capabilities constant
   - Added `get_capability()` and `has_capability()` methods
   - Capability profile is now accessible from all tests

3. **scripts/test-runner.py**
   - Added `--sdk` CLI option to specify SDK
   - Added `--sdk-version` CLI option to specify version
   - Added `--defconfig` CLI option to load SDK configurations
   - Integrated capability profile generation
   - Capability profiles passed to S3Client instances
   - Saves generated capabilities to `.sdk_capabilities.json`

4. **conftest.py**
   - Added `sdk_capabilities` pytest fixture
   - Automatically generates/loads capability profiles for tests
   - Passes capabilities to S3Client fixture
   - Supports environment variable configuration

## How It Works

### Three-Layer Capability Resolution

1. **Static Mapping** (in sdk_capabilities.py)
   - Pre-defined profiles for known SDK/version combinations
   - Version constraint matching (e.g., ">= 1.26.0, < 2.0.0")

2. **Runtime Probes** (extensible framework)
   - Can dynamically detect SDK behavior at runtime
   - Framework in place, probes can be implemented as needed

3. **Manual Overrides** (via JSON files)
   - Allow explicit overrides for special cases
   - Useful for testing or working around edge cases

These three sources merge (last wins) to create final capability profile.

## Usage Examples

### Command Line

```bash
# Test with specific SDK
python3 scripts/test-runner.py --sdk boto3 --sdk-version latest -v

# Test with Go SDK v2
python3 scripts/test-runner.py --sdk aws-sdk-go-v2 --sdk-version 1.30.0 -v

# Use a defconfig file
python3 scripts/test-runner.py --defconfig defconfigs/boto3_latest.yaml -v

# Run specific test group with SDK selection
python3 scripts/test-runner.py --sdk boto3 --group edge -v
```

### Configuration File

Edit `s3_config.yaml`:
```yaml
s3_sdk: "boto3"
s3_sdk_version: "latest"
s3_cap_profile_override: False
s3_caps_json_path: ".sdk_capabilities.json"
```

### Environment Variables

```bash
export S3_SDK=boto3
export S3_SDK_VERSION=1.26.0
pytest tests/edge/test_sdk_capability_example.py -v
```

### In Tests

```python
def test_url_encoding(s3_client, sdk_capabilities, config):
    """Test that adapts to SDK URL encoding behavior"""
    caps = sdk_capabilities.get("profile", {})

    # Check how SDK handles '+'
    if caps.get("list_objects_url_plus_treated_as_space", False):
        prefix = "test "  # SDK treats + as space
    else:
        prefix = "test+"  # SDK treats + as +

    # Test adapts its expectations
    objects = s3_client.list_objects(bucket, prefix=prefix)
    # ... assertions
```

Or using S3Client directly:

```python
def test_checksum(s3_client):
    """Test using client capability methods"""
    if s3_client.has_capability("crc32c_default"):
        # Expect CRC32C checksums
        pass
    else:
        # Expect MD5 ETag
        pass
```

## Capability Flags

Current supported capability flags:

- `sigv4_chunked` - SDK uses SigV4 with chunked encoding
- `unsigned_payload_allowed` - SDK supports UNSIGNED-PAYLOAD
- `virtual_hosted_default` - SDK uses virtual-hosted style URLs
- `list_objects_v1` - SDK defaults to ListObjects v1
- `list_objects_url_plus_treated_as_space` - SDK URL encoding behavior
- `retry_mode` - Retry policy ("standard", "adaptive", "legacy")
- `follows_301_region_redirect` - SDK follows cross-region redirects
- `follows_307_on_put` - SDK replays body on 307/308 redirects
- `crc32c_default` - SDK uses CRC32C checksums by default

## Supported SDKs

The system supports (with extensible mappings):

- boto3 (Python)
- botocore (Python low-level)
- aws-sdk-go-v1 (Go SDK v1)
- aws-sdk-go-v2 (Go SDK v2)
- aws-sdk-java-v1 (Java SDK v1)
- aws-sdk-java-v2 (Java SDK v2)
- aws-sdk-js-v2 (JavaScript SDK v2)
- aws-sdk-js-v3 (JavaScript SDK v3)
- aws-sdk-dotnet (.NET SDK)
- aws-sdk-rust (Rust SDK)

## Directory Structure

```
msst-s3/
├── defconfigs/                      # SDK configuration templates
│   ├── boto3_latest.yaml
│   ├── boto3_1.26.yaml
│   ├── aws_sdk_go_v2.yaml
│   ├── aws_sdk_java_v2.yaml
│   └── README.md
├── tests/
│   ├── common/
│   │   ├── sdk_capabilities.py      # Core capability system
│   │   └── s3_client.py             # Enhanced with capabilities
│   └── edge/
│       └── test_sdk_capability_example.py  # Example tests
├── scripts/
│   └── test-runner.py               # Enhanced with SDK options
├── conftest.py                      # Enhanced with SDK fixtures
├── s3_config.yaml                   # Enhanced with SDK config
├── SDK_CAPABILITIES.md              # Full documentation
└── SDK_IMPLEMENTATION_SUMMARY.md    # This file
```

## Next Steps

### For Users

1. **Test with different SDKs**:
   ```bash
   python3 scripts/test-runner.py --sdk boto3 --group edge
   python3 scripts/test-runner.py --sdk aws-sdk-go-v2 --group edge
   ```

2. **Create custom defconfigs** for your specific SDK versions

3. **Update existing tests** to be capability-aware where appropriate

### For Developers

1. **Add more SDK mappings** to `STATIC_CAPABILITY_MAPPING` in `sdk_capabilities.py`

2. **Implement runtime probes** in `run_probes_for_sdk()` for dynamic detection

3. **Add new capability flags** as needed for discovered SDK differences

4. **Create more defconfigs** for different SDK combinations

## Testing the Implementation

### Verify Installation

```bash
# Check SDK capabilities module loads
python3 -c "import sys; sys.path.insert(0, 'tests'); \
from common.sdk_capabilities import SDKSpec; \
print('SDK Capabilities: OK')"

# Check CLI options
python3 scripts/test-runner.py --help | grep sdk

# List defconfigs
ls -l defconfigs/*.yaml
```

### Run Example Tests

```bash
# Run example capability-aware tests
pytest tests/edge/test_sdk_capability_example.py -v

# Run with specific SDK
python3 scripts/test-runner.py \
  --sdk boto3 \
  --test test_sdk_capability_example \
  -v
```

### Test with Defconfig

```bash
# Test with boto3 defconfig
python3 scripts/test-runner.py \
  --defconfig defconfigs/boto3_latest.yaml \
  --group edge \
  -v

# Check generated capabilities
cat .sdk_capabilities.json
```

## Benefits

1. **No More False Failures**: Tests adapt to legitimate SDK differences
2. **Multi-SDK Testing**: Easy to test against different SDK implementations
3. **Version Compatibility**: Track SDK version-specific behaviors
4. **CI/CD Matrix Testing**: Simple to test across SDK matrix
5. **Clear Documentation**: SDK differences are explicitly documented
6. **Extensible**: Easy to add new SDKs and capabilities

## Documentation References

- **SDK_CAPABILITIES.md** - Full system documentation
- **defconfigs/README.md** - Defconfig usage guide
- **tests/edge/test_sdk_capability_example.py** - Code examples
- **tests/common/sdk_capabilities.py** - Implementation details

---

Generated-by: Claude AI
Signed-off-by: Luis Chamberlain <mcgrof@kernel.org>
