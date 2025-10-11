# SDK Defconfig Files

This directory contains pre-configured SDK profiles for testing against different AWS SDK implementations and versions.

## Available Defconfigs

- `boto3_latest.yaml` - Latest version of Python boto3 SDK
- `boto3_1.26.yaml` - boto3 version 1.26.x
- `aws_sdk_go_v2.yaml` - Latest version of Go SDK v2
- `aws_sdk_java_v2.yaml` - Latest version of Java SDK v2

## Quick Usage

```bash
# Use a defconfig with test-runner.py
./scripts/test-runner.py --defconfig defconfigs/boto3_latest.yaml --verbose

# Run specific test group
./scripts/test-runner.py --defconfig defconfigs/aws_sdk_go_v2.yaml --group edge
```

## Full Documentation

For complete documentation on the SDK capability system, defconfig usage, and examples, see:
- **[docs/DEFCONFIGS.md](../docs/DEFCONFIGS.md)** - Detailed defconfig documentation
- **[docs/SDK_CAPABILITIES.md](../docs/SDK_CAPABILITIES.md)** - Complete SDK capability system guide

Generated-by: Claude AI
Signed-off-by: Luis Chamberlain <mcgrof@kernel.org>
