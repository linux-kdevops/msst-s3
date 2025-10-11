# SDK Defconfig Files

This directory contains pre-configured SDK profiles (defconfigs) for testing against
different AWS SDK implementations and versions.

## What are Defconfigs?

Defconfig files are YAML configurations that specify which AWS SDK and version to test
against. They allow you to quickly switch between different SDK implementations without
manually editing configuration files.

## Available Defconfigs

- `boto3_latest.yaml` - Latest version of Python boto3 SDK
- `boto3_1.26.yaml` - boto3 version 1.26.x (stable reference version)
- `aws_sdk_go_v2.yaml` - Latest version of Go SDK v2
- `aws_sdk_java_v2.yaml` - Latest version of Java SDK v2

## Usage

### With test-runner.py

```bash
# Use a defconfig with test-runner.py
./scripts/test-runner.py --defconfig defconfigs/boto3_latest.yaml --verbose

# Run specific test group with a defconfig
./scripts/test-runner.py --defconfig defconfigs/aws_sdk_go_v2.yaml --group edge
```

### With Command Line Override

```bash
# Specify SDK directly without a defconfig file
./scripts/test-runner.py --sdk boto3 --sdk-version 1.28.0 --verbose
```

### With pytest

```bash
# Set environment variables for pytest
export S3_SDK=boto3
export S3_SDK_VERSION=latest
pytest tests/edge/test_sdk_capability_example.py -v
```

## Creating Custom Defconfigs

You can create your own defconfig files for specific SDK versions you want to test:

```yaml
---
s3_sdk: "your-sdk-name"
s3_sdk_version: "x.y.z"

# Optional: Override capability profile
# s3_cap_profile_override: True
# s3_cap_profile_json: "path/to/custom_capabilities.json"
```

## SDK Capability Profiles

When you run tests with a defconfig or SDK specification, the test runner automatically:

1. Generates a capability profile based on the SDK and version
2. Saves it to `.sdk_capabilities.json` (configurable)
3. Makes it available to all tests via fixtures

This allows tests to adapt their behavior and expectations based on known SDK differences.

## Supported SDKs

The capability system supports the following SDKs:

- **boto3** (Python) - v1.26.0+
- **botocore** (Python low-level)
- **aws-sdk-go-v1** (Go SDK v1)
- **aws-sdk-go-v2** (Go SDK v2) - v1.25.0+
- **aws-sdk-java-v1** (Java SDK v1)
- **aws-sdk-java-v2** (Java SDK v2)
- **aws-sdk-js-v2** (JavaScript SDK v2)
- **aws-sdk-js-v3** (JavaScript SDK v3)
- **aws-sdk-dotnet** (.NET SDK)
- **aws-sdk-rust** (Rust SDK)

## Example: Testing Against Multiple SDKs

```bash
# Test with boto3
./scripts/test-runner.py --defconfig defconfigs/boto3_latest.yaml -g edge

# Test with Go SDK v2
./scripts/test-runner.py --defconfig defconfigs/aws_sdk_go_v2.yaml -g edge

# Compare results between SDKs
diff results/boto3_*.json results/go_v2_*.json
```

## Matrix Testing

For CI/CD environments, you can loop through multiple defconfigs:

```bash
for defconfig in defconfigs/*.yaml; do
  echo "Testing with $defconfig"
  ./scripts/test-runner.py --defconfig "$defconfig" --output-format junit
done
```

Generated-by: Claude AI
Signed-off-by: Luis Chamberlain <mcgrof@kernel.org>
