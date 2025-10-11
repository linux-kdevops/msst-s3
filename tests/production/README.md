# Production Workload Test Suite

## Overview

This directory contains advanced S3 compatibility tests designed to validate distributed storage systems under real-world production workload patterns. These tests go beyond basic CRUD operations to test concurrency, consistency, scalability, and failure modes.

## Implemented Tests

### Test 1: Thundering Herd Consistency (`test_thundering_herd.py`)
**Pattern**: Cache stampede, hot key scenario, CDN origin updates

**Tests:**
- 100 concurrent clients PUTting to same key
- Last-writer-wins consistency
- ETag stability
- Data integrity under extreme contention

**Why It Matters**: Common in CDN origins, distributed caches, microservices with shared state

### Test 2: Multipart Upload Orphan Management (`test_orphan_management.py`)
**Pattern**: Cloud cost optimization, incomplete upload cleanup

**Tests:**
- Detection of abandoned multipart uploads
- Storage cost calculation for orphans
- Cleanup routines
- Large part handling (MB+ parts)

**Why It Matters**: Orphaned uploads cost money and consume storage. Critical for cloud cost management.

### Test 4: List Consistency Under Churn (`test_list_consistency.py`)
**Pattern**: High-frequency create/delete, temp files, job output

**Tests:**
- Eventual consistency for LIST operations
- Pagination correctness
- No phantom or missing objects
- Consistency convergence

**Why It Matters**: S3 has eventual consistency. Production workflows depend on consistent listings.

### Test 9: Metadata Consistency (`test_metadata_consistency.py`)
**Pattern**: Multi-system tagging, compliance automation, metadata updates

**Tests:**
- 50+ concurrent metadata updates
- Tag consistency under concurrent updates
- Metadata with versioning
- No corruption or merging

**Why It Matters**: Multiple systems update metadata (backup, compliance, analytics). Corruption breaks automation.

## Tests To Be Implemented

### Test 3: Version Explosion and List Performance
- 10,000 versions of single object
- ListObjectVersions scalability
- Batch delete performance

### Test 5: Cross-Feature Interaction
- Versioning + Lifecycle + Replication
- Feature interaction bugs
- Enterprise data management patterns

### Test 6: Large-Scale Prefix Enumeration
- 100,000+ objects in hierarchical prefixes
- Data lake query patterns
- Memory usage under load

### Test 7: Concurrent Multipart to Same Key
- Multiple clients racing to complete
- Last-completer-wins
- No data corruption

### Test 8: Batch Delete at Scale
- 10,000 object bulk delete
- Partial failure handling
- Performance metrics

### Test 10: Distributed Coordination
- Compare-and-swap with ETags
- Optimistic locking
- Distributed counter pattern

## Running the Tests

### Run All Production Tests
```bash
pytest tests/production/ -v
```

### Run Specific Test
```bash
# Thundering herd
pytest tests/production/test_thundering_herd.py -v

# Orphan management
pytest tests/production/test_orphan_management.py -v

# List consistency
pytest tests/production/test_list_consistency.py -v

# Metadata consistency
pytest tests/production/test_metadata_consistency.py -v
```

### With Detailed Output
```bash
pytest tests/production/ -v -s
```

### Run with SDK Capability Profiles
```bash
# Test with boto3
python3 scripts/test-runner.py \
  --sdk boto3 \
  --group production \
  -v

# Test with Go SDK
python3 scripts/test-runner.py \
  --sdk aws-sdk-go-v2 \
  --group production \
  -v
```

## Test Characteristics

| Test | Duration | Concurrency | Scale | Resource Usage |
|------|----------|-------------|-------|----------------|
| Thundering Herd | 10-20s | 100 clients | 100 ops | Low |
| Orphan Management | 30-60s | Sequential | 50 uploads | Medium (MB data) |
| List Consistency | 30-60s | 3 threads | 1000s ops | Low |
| Metadata Consistency | 20-40s | 50 clients | 50 updates | Low |

## Performance Expectations

### MinIO (Local Instance)
- **Thundering Herd**: 100% success rate, <5s total duration
- **Orphan Management**: Correct detection, <2s cleanup per orphan
- **List Consistency**: Convergence <5s after quiescence
- **Metadata Consistency**: 100% success rate, no corruption

### AWS S3
- **Thundering Herd**: 100% success rate, <10s total duration
- **Orphan Management**: Correct detection, <5s cleanup per orphan
- **List Consistency**: Convergence <10s (eventual consistency)
- **Metadata Consistency**: 100% success rate, no corruption

## Interpreting Results

### Success Criteria
✅ **All operations succeed** - No lock contention or resource exhaustion
✅ **Data integrity maintained** - No corruption under concurrent access
✅ **Consistency guarantees met** - Eventual or strong consistency as appropriate
✅ **Resources properly managed** - No leaks, correct cleanup

### Common Issues
⚠️ **Timeout errors** - May indicate performance issues or network problems
⚠️ **Phantom objects** - Expected in eventually consistent systems (should converge)
⚠️ **Resource limits** - Some implementations have concurrency limits

## Production Relevance

These tests validate patterns from real production systems:

1. **Data Lakes** (Spark, Presto) - List consistency, large-scale enumeration
2. **Streaming Ingestion** (Kafka, Kinesis) - High-frequency writes, consistency
3. **Backup Systems** (Veeam) - Multipart uploads, resource management
4. **ML Pipelines** (SageMaker) - Concurrent access, versioning
5. **CDN Origins** (CloudFront) - Thundering herd, cache patterns
6. **Container Registries** (Docker Hub) - Concurrent uploads, consistency
7. **Log Aggregation** (Fluentd) - Append patterns, list consistency
8. **ETL Pipelines** (Airflow) - Batch operations, metadata management

## Contributing

To add new production tests:

1. Identify a real production pattern not yet covered
2. Design test cases that stress the pattern
3. Implement with proper error handling and cleanup
4. Document the production scenario and why it matters
5. Add to this README

### Test Template
```python
def test_production_pattern_name(s3_client, config):
    """
    Brief description of production pattern.

    What it tests:
    - Specific behaviors
    - Edge cases
    - Failure modes

    Why it matters:
    - Production relevance
    - What could go wrong
    """
    bucket_name = f"{config['s3_bucket_prefix']}-test-{random_string()}"

    try:
        s3_client.create_bucket(bucket_name)

        # Test implementation

        # Assertions
        assert condition, "Failure message"

    finally:
        # Cleanup
        pass
```

## References

- [Production Workload Test Design](../../docs/PRODUCTION_WORKLOAD_TESTS.md) - Detailed design document
- [Test Results](../../docs/TEST_RESULTS.md) - Overall test suite results
- [Testing Guide](../../docs/TESTING_GUIDE.md) - General testing documentation

---

Generated-by: Claude AI
Signed-off-by: Luis Chamberlain <mcgrof@kernel.org>
