# Production Workload Test Suite - Batch 2

## Overview

This document describes 10 additional advanced S3 tests focusing on failure recovery, consistency boundaries, performance under stress, and compliance patterns. These tests complement Batch 1 by covering network failures, rate limiting, distributed consensus, and real-world backup/recovery scenarios.

## Production Gaps from Batch 1

Batch 1 covered:
- ✅ Concurrency and race conditions
- ✅ Resource management
- ✅ Eventual consistency basics
- ✅ Metadata operations

Batch 2 focuses on:
- ❌ Network failures and partition recovery
- ❌ Rate limiting and backpressure
- ❌ Point-in-time recovery patterns
- ❌ Distributed consensus primitives
- ❌ Consistency model boundaries
- ❌ Atomic operations at scale
- ❌ Compliance and immutability
- ❌ Real-world backup patterns

## 10 Additional Production Tests

---

### Test 11: Network Partition Recovery and Retry Storms
**Production Pattern**: Network failures during operations, retry amplification, zombie processes

**What It Tests:**
- Simulate network partition during multipart upload
- Client timeout and retry behavior
- Orphaned operations detection
- Retry storm prevention (exponential backoff)
- Duplicate request handling (idempotency)

**Why It Matters:**
- Network failures are the #1 cause of production incidents
- Naive retry logic can cause cascading failures (retry storms)
- Zombie operations waste resources and cause data inconsistency
- Critical for distributed systems resilience

**Real-World Scenarios:**
- Kubernetes pod restarts during upload
- Network timeout in Lambda functions (15min limit)
- Load balancer connection drops
- DNS resolution failures
- Thundering herd of retries after incident

**Implementation Plan:**
```python
def test_network_partition_multipart_recovery():
    """
    Simulate network failure during multipart upload:
    1. Start multipart upload with 10 parts
    2. Upload 5 parts successfully
    3. Simulate network failure (close connection, timeout)
    4. Resume upload from checkpoint
    5. Verify:
       - No duplicate parts uploaded
       - Original parts still valid
       - Upload completes successfully
       - No orphaned uploads
    """

def test_retry_storm_prevention():
    """
    Test exponential backoff under failures:
    1. 100 clients try to upload simultaneously
    2. Inject transient failures (503 errors)
    3. Verify clients implement exponential backoff
    4. Measure retry spacing (should increase over time)
    5. Verify no retry storm (>1000 requests/sec spike)
    6. All clients eventually succeed
    """

def test_zombie_operation_detection():
    """
    Test detection of abandoned operations:
    1. Start 50 multipart uploads
    2. Simulate client crashes (no abort, no complete)
    3. After timeout period (e.g., 24 hours)
    4. Detect zombie uploads via ListMultipartUploads
    5. Cleanup with age-based policy
    6. Verify storage reclamation
    """
```

**Success Criteria:**
- Resume from checkpoint works without data loss
- Exponential backoff prevents retry storms
- Zombie detection accuracy >99%
- No duplicate data

---

### Test 12: Bandwidth Fairness and QoS Under Contention
**Production Pattern**: Multiple clients competing for bandwidth, fair resource allocation

**What It Tests:**
- 10 clients uploading large files (1GB+) simultaneously
- Bandwidth allocation fairness
- No client starvation
- Throughput degradation under contention
- QoS tiers (if supported)

**Why It Matters:**
- Production systems have multiple tenants/workloads
- One greedy client shouldn't starve others
- SLA compliance requires fair resource allocation
- Critical for multi-tenant systems

**Real-World Scenarios:**
- Backup job competing with live traffic
- Multiple data pipelines running concurrently
- Batch processing competing with interactive queries
- Multi-tenant storage systems

**Implementation Plan:**
```python
def test_bandwidth_fairness_multiple_uploads():
    """
    Test fair bandwidth allocation:
    1. 10 clients each upload 1GB file
    2. Measure per-client throughput over time
    3. Calculate fairness index (Jain's fairness)
    4. Verify no client gets <10% of fair share
    5. Verify no client dominates (>30% of total)
    6. Measure completion time variance
    """

def test_priority_qos_if_supported():
    """
    Test QoS tiers (if available):
    1. Upload with high-priority tag
    2. Upload with low-priority tag
    3. Measure throughput difference
    4. Verify high-priority gets preference
    5. Verify low-priority not starved (>1% bandwidth)
    """
```

**Success Criteria:**
- Fairness index >0.8 (Jain's fairness)
- No starvation (all clients complete)
- Throughput degradation <50% under 10x load
- Predictable performance

---

### Test 13: Time-Travel Point-in-Time Recovery
**Production Pattern**: Backup and disaster recovery using versioning

**What It Tests:**
- Create object with many versions over time
- Restore to specific point in time
- Verify data integrity at any historical point
- Test with thousands of versions
- Performance of historical queries

**Why It Matters:**
- Core backup/recovery capability
- Compliance requirements (retain historical data)
- Disaster recovery (restore before corruption)
- Common in database backups, configuration management

**Real-World Scenarios:**
- Restore database to before bad migration
- Recover accidentally deleted/corrupted files
- Compliance audit (prove data state at time T)
- Ransomware recovery (restore before encryption)

**Implementation Plan:**
```python
def test_point_in_time_restore():
    """
    Test time-travel recovery:
    1. Create object at T0
    2. Update object every minute for 1 hour (60 versions)
    3. For each version, record timestamp
    4. Pick random time T (e.g., T0 + 37 minutes)
    5. Restore to version closest to time T
    6. Verify content matches expected state at T
    7. Test with 10,000+ versions
    """

def test_consistent_snapshot_across_objects():
    """
    Test consistent point-in-time snapshot:
    1. Bucket with 100 objects, each with versions
    2. Take "snapshot" at time T (record all version IDs)
    3. Continue modifying objects
    4. Restore entire bucket to snapshot T
    5. Verify all objects consistent with time T
    6. No mixed state (some at T, some at T+delta)
    """
```

**Success Criteria:**
- Can restore to any point in time
- Historical query time <5s for 10K versions
- Snapshot consistency verified
- No data loss

---

### Test 14: Conditional Write for Distributed Consensus
**Production Pattern**: Building distributed locks, leader election, atomic counters

**What It Tests:**
- Implement compare-and-swap using If-Match/If-None-Match
- 100 clients trying to acquire distributed lock
- Only one succeeds per generation
- Test fairness and liveness
- Performance under contention

**Why It Matters:**
- S3 can be used for lightweight coordination
- Common pattern in serverless architectures
- Cheaper than DynamoDB/ZooKeeper for simple cases
- Critical for distributed algorithms

**Real-World Scenarios:**
- Distributed lock for cron job (only one runs)
- Leader election for worker fleet
- Atomic counter for ID generation
- Configuration updates with optimistic locking

**Implementation Plan:**
```python
def test_distributed_lock_with_etag():
    """
    Implement distributed lock:
    1. Create lock object with holder ID
    2. 100 clients try to acquire lock using:
       - GetObject (get current holder + ETag)
       - CopyObject with If-Match (try to become holder)
    3. Verify only one holder at a time
    4. Failed clients retry with backoff
    5. Lock holder can renew (update timestamp)
    6. Lock expires if not renewed (TTL check)
    7. Verify all clients eventually get lock (fairness)
    """

def test_atomic_counter_with_cas():
    """
    Implement atomic counter:
    1. Counter object stores current value
    2. 1000 clients each increment by 1
    3. Each client:
       - GetObject (read counter + ETag)
       - Compute new_value = old_value + 1
       - CopyObject with If-Match (conditional update)
       - Retry on conflict (PreconditionFailed)
    4. Verify final counter = 1000 (no lost updates)
    5. Measure conflict rate and retry overhead
    """

def test_leader_election_pattern():
    """
    Implement leader election:
    1. 50 workers compete for leadership
    2. Leader writes heartbeat every 5 seconds
    3. Workers monitor heartbeat
    4. If heartbeat stale (>15s), re-elect
    5. Verify exactly one leader at all times
    6. Measure failover time (<30s)
    """
```

**Success Criteria:**
- Lock acquired atomically (no double-acquire)
- Counter accuracy 100% (no lost updates)
- Leader election convergence <30s
- Fairness across clients

---

### Test 15: Read-After-Write Consistency Boundaries
**Production Pattern**: Understanding and testing S3's consistency model limits

**What It Tests:**
- Strong consistency for PUTs (since Dec 2020)
- Eventual consistency for LIST (older behavior)
- Read-your-writes vs read-my-writes
- Cross-client consistency timing
- Consistency under load

**Why It Matters:**
- Applications depend on consistency guarantees
- Many systems assume immediate visibility
- ETL pipelines can fail on stale listings
- Critical for correctness reasoning

**Real-World Scenarios:**
- Write object, immediately list (is it visible?)
- Multi-step transactions (all steps visible?)
- Data pipeline (producer/consumer)
- Backup verification (just wrote, can I read?)

**Implementation Plan:**
```python
def test_put_then_get_same_client():
    """
    Test read-your-writes:
    1. PUT object
    2. Immediately GET object (same client)
    3. Verify object is readable
    4. Verify content matches what was written
    5. Measure time to consistency (<100ms)
    """

def test_put_then_get_different_client():
    """
    Test read-my-writes across clients:
    1. Client A PUTs object
    2. Client B immediately GETs object
    3. Verify object is readable by client B
    4. Measure propagation delay
    5. Test with 100 clients reading after 1 PUT
    """

def test_put_then_list_consistency():
    """
    Test LIST after PUT:
    1. PUT object to prefix
    2. Immediately LIST prefix
    3. Is new object in listing?
    4. Measure time until object appears in LIST
    5. Test with 1000 objects
    6. Verify eventual convergence (<5s)
    """

def test_delete_then_list_consistency():
    """
    Test LIST after DELETE:
    1. Create object
    2. DELETE object
    3. Immediately LIST
    4. Does deleted object still appear? (phantom)
    5. Measure time until object disappears
    6. Verify eventual convergence
    """
```

**Success Criteria:**
- Read-your-writes: 100% immediate
- Read-my-writes: <1s across clients
- LIST convergence: <5s after quiescence
- No permanent inconsistencies

---

### Test 16: Atomic Checkpoint/Snapshot Creation
**Production Pattern**: Creating consistent snapshots across multiple objects

**What It Tests:**
- Snapshot 1000 objects atomically
- Incremental snapshots (only changed objects)
- Snapshot metadata (what's included)
- Restore from snapshot
- Snapshot performance

**Why It Matters:**
- Backups need consistency (point-in-time)
- Can't have half-updated snapshot
- Common in database backups, VM snapshots
- Critical for disaster recovery

**Real-World Scenarios:**
- Database backup (tables consistent at same time)
- Application state checkpoint
- VM snapshot (disk + memory consistent)
- Git-like commit (all files at same version)

**Implementation Plan:**
```python
def test_atomic_snapshot_multiple_objects():
    """
    Create consistent snapshot:
    1. Start: Mark snapshot start time T
    2. For each object, record (key, version_id) at time T
    3. Continue updates after T (should NOT be in snapshot)
    4. Restore: For each (key, version_id), retrieve that version
    5. Verify all objects are from time T (not mixed)
    6. Test with 1000 objects, 10 concurrent updaters
    """

def test_incremental_snapshot():
    """
    Implement incremental backup:
    1. Full snapshot at T0 (all objects)
    2. Track Last-Modified times
    3. Incremental snapshot at T1 (only changed since T0)
    4. Incremental snapshot at T2 (only changed since T1)
    5. Verify: Restore(T0) + Restore(T0->T1) + Restore(T1->T2) = state at T2
    6. Measure space savings (incremental vs full)
    """

def test_snapshot_metadata_consistency():
    """
    Test snapshot metadata integrity:
    1. Create snapshot of 1000 objects
    2. Store manifest (list of all version IDs)
    3. Verify manifest is complete (no missing objects)
    4. Corrupt one entry in manifest
    5. Restore should detect corruption
    6. Test manifest versioning (snapshots of snapshots)
    """
```

**Success Criteria:**
- Snapshot consistency 100%
- Restore accuracy 100%
- Incremental space savings >80%
- Performance: snapshot 1000 objects <10s

---

### Test 17: Hierarchical Namespace at Scale
**Production Pattern**: Directory-like operations on prefix-based "folders"

**What It Tests:**
- Rename "directory" (10K+ objects)
- Delete "directory" recursively
- Move objects between "directories"
- List directory hierarchy
- Concurrent modifications

**Why It Matters:**
- Users expect file-system semantics
- Rename/move are common operations
- S3 has no native directory support
- Must be emulated with prefix operations

**Real-World Scenarios:**
- Data lake reorganization (rename year=2023 to year=2024)
- User folder management
- Project directory restructuring
- Backup directory archival

**Implementation Plan:**
```python
def test_directory_rename_at_scale():
    """
    Rename 10,000 object "directory":
    1. Create 10,000 objects under prefix "old-dir/"
    2. "Rename" to "new-dir/" by:
       - CopyObject to new-dir/ for each object
       - DeleteObject from old-dir/ for each object
    3. Verify all objects in new-dir/
    4. Verify old-dir/ is empty
    5. Measure performance (objects/sec)
    6. Handle errors (partial completion)
    """

def test_recursive_directory_delete():
    """
    Delete directory with 10,000 objects:
    1. Create nested structure: dir/year=2024/month=*/day=*/ (10K objects)
    2. Delete entire "dir/" recursively
    3. Use ListObjectsV2 + DeleteObjects (batch 1000)
    4. Measure performance
    5. Verify complete deletion (no orphans)
    6. Test with concurrent readers (can they still read during delete?)
    """

def test_directory_move_consistency():
    """
    Test move consistency:
    1. Create 1000 objects in "source/"
    2. 10 clients reading from "source/" continuously
    3. Move to "dest/" (copy + delete)
    4. Verify:
       - Readers see consistent state (all in source OR all in dest)
       - No objects lost during move
       - No objects duplicated
    5. Measure transition period (time with mixed state)
    """
```

**Success Criteria:**
- Rename throughput >100 objects/sec
- Delete throughput >500 objects/sec
- No data loss during operations
- Error handling for partial completion

---

### Test 18: Rate Limiting and Circuit Breaker Patterns
**Production Pattern**: Graceful handling of 503 SlowDown responses

**What It Tests:**
- Trigger rate limits (>1000 req/sec)
- Exponential backoff on 503
- Circuit breaker implementation
- Request queuing and buffering
- Graceful degradation

**Why It Matters:**
- S3 has per-prefix rate limits
- Exceeding limits causes 503 errors
- Naive retry amplifies the problem
- Production systems must handle gracefully

**Real-World Scenarios:**
- Black Friday traffic spike
- Batch job overload
- Data migration (bulk operations)
- Accidental DDoS from misconfigured client

**Implementation Plan:**
```python
def test_rate_limit_handling():
    """
    Test 503 SlowDown handling:
    1. Generate high request rate (2000 req/sec to same prefix)
    2. Monitor for 503 responses
    3. Implement exponential backoff:
       - First 503: wait 1s
       - Second 503: wait 2s
       - Third 503: wait 4s
       - Max wait: 32s
    4. Verify backoff reduces request rate
    5. Verify all requests eventually succeed
    6. Measure total time to completion
    """

def test_circuit_breaker_pattern():
    """
    Implement circuit breaker:
    1. Send requests to S3
    2. Track error rate (503s)
    3. If error rate >50% in 60s window:
       - OPEN circuit (stop sending requests)
       - Wait for cooldown (30s)
       - Try single request (half-open)
       - If success: CLOSE circuit (resume normal)
       - If failure: OPEN circuit again
    4. Verify circuit prevents cascading failures
    5. Measure recovery time
    """

def test_request_queue_with_backpressure():
    """
    Test request buffering:
    1. Generate 10,000 PUT requests
    2. Queue requests with max size 100
    3. If queue full, apply backpressure (slow producer)
    4. Process queue with rate limiting (100/sec)
    5. Verify no request loss
    6. Measure end-to-end latency p50, p99
    """
```

**Success Criteria:**
- All requests eventually succeed
- No retry storms (request rate decreases on error)
- Circuit breaker prevents >10s of failures
- Backpressure prevents memory exhaustion

---

### Test 19: Incremental Backup and Differential Sync
**Production Pattern**: Efficient backup using Last-Modified and ETags

**What It Tests:**
- Initial full backup (all objects)
- Incremental backup (only changed)
- Verification (checksum validation)
- Restore from incremental
- Performance vs full backup

**Why It Matters:**
- Full backups are expensive and slow
- Incremental backups save time and money
- Core capability for backup systems
- Used by rsync, backup tools, sync utilities

**Real-World Scenarios:**
- Daily database backups
- User file synchronization
- Disaster recovery preparation
- Compliance archival

**Implementation Plan:**
```python
def test_incremental_backup_strategy():
    """
    Test incremental backup:
    1. Day 0: Full backup
       - Copy all 10,000 objects to backup bucket
       - Record Last-Modified for each object
    2. Day 1: Modify 1,000 objects
       - Incremental: Only copy objects with Last-Modified > Day 0
       - Verify only 1,000 objects copied
    3. Day 2: Modify 500 objects
       - Incremental: Only copy objects with Last-Modified > Day 1
       - Verify only 500 objects copied
    4. Restore from incremental:
       - Base = Day 0 full
       - Apply Day 1 incremental
       - Apply Day 2 incremental
       - Verify final state matches source
    """

def test_differential_sync_with_etag():
    """
    Test efficient sync:
    1. Source bucket with 10,000 objects
    2. Destination bucket (replica)
    3. Sync algorithm:
       - List both buckets
       - Compare ETags for each key
       - Copy if:
         * Key missing in dest
         * ETag differs (object changed)
       - Delete from dest if not in source
    4. Verify dest identical to source
    5. Measure sync time
    6. Test with 10% changed objects
    """

def test_backup_verification():
    """
    Test backup integrity:
    1. Backup 10,000 objects
    2. For each object, compute checksum (MD5/SHA256)
    3. Store manifest with checksums
    4. Verification:
       - Read each backup object
       - Compute checksum
       - Compare to manifest
    5. Inject corruption (flip bits in one object)
    6. Verification should detect corruption
    7. Test restoration with verification
    """
```

**Success Criteria:**
- Incremental backup >10x faster than full
- Restore accuracy 100%
- Corruption detection 100%
- Sync performance >100 objects/sec

---

### Test 20: Object Lock and WORM Compliance
**Production Pattern**: Write-Once-Read-Many for compliance (SEC, HIPAA, FINRA)

**What It Tests:**
- Object lock configuration
- Retention modes (governance vs compliance)
- Legal hold
- Attempt to delete locked object (should fail)
- Retention period expiry

**Why It Matters:**
- Regulatory compliance requirements
- Prevent accidental or malicious deletion
- Audit trail for compliance
- Common in financial services, healthcare

**Real-World Scenarios:**
- Financial records (SEC 17a-4)
- Healthcare records (HIPAA)
- Legal discovery (e-discovery)
- Audit logs (immutable)

**Implementation Plan:**
```python
def test_object_lock_compliance_mode():
    """
    Test compliance mode (strictest):
    1. Enable object lock on bucket
    2. PUT object with retention mode=COMPLIANCE, days=365
    3. Attempt to delete object (should fail with AccessDenied)
    4. Attempt to delete as root (should still fail - compliance)
    5. Attempt to shorten retention (should fail)
    6. Wait for retention to expire (or use test clock)
    7. Delete should now succeed
    """

def test_object_lock_governance_mode():
    """
    Test governance mode (can be overridden):
    1. PUT object with retention mode=GOVERNANCE, days=30
    2. Attempt to delete without bypass (should fail)
    3. Delete with bypass header (should succeed if permitted)
    4. Verify permission model (only authorized users can bypass)
    """

def test_legal_hold():
    """
    Test legal hold (indefinite lock):
    1. PUT object with legal hold ON
    2. Attempt to delete (should fail)
    3. No expiry (hold remains until explicitly removed)
    4. Remove legal hold
    5. Delete should now succeed
    6. Test with 1000 objects under legal hold
    """

def test_versioning_with_object_lock():
    """
    Test lock with versioning:
    1. Enable versioning + object lock
    2. PUT object (version 1, locked)
    3. Overwrite object (version 2, also locked)
    4. Attempt to delete version 1 (should fail)
    5. Delete bucket (should fail - contains locked objects)
    6. Verify all versions remain locked
    7. Test delete marker behavior with locks
    """
```

**Success Criteria:**
- Locked objects cannot be deleted
- Compliance mode cannot be bypassed
- Legal hold works indefinitely
- Versioning + lock interaction correct

---

## Summary of Batch 2

| Test # | Test Name | Production Pattern | Real-World Use Case |
|--------|-----------|-------------------|---------------------|
| 11 | Network Partition Recovery | Retry storms, zombie operations | Kubernetes restarts, Lambda timeouts |
| 12 | Bandwidth Fairness | QoS, multi-tenancy | Backup vs live traffic |
| 13 | Time-Travel Recovery | Point-in-time restore | Ransomware recovery, compliance |
| 14 | Distributed Consensus | Locks, leader election | Serverless coordination |
| 15 | Consistency Boundaries | Read-after-write | ETL pipeline correctness |
| 16 | Atomic Snapshots | Consistent backup | Database backups |
| 17 | Hierarchical Namespace | Directory operations | Data lake reorganization |
| 18 | Rate Limiting | Circuit breakers | Black Friday traffic |
| 19 | Incremental Backup | Differential sync | Daily backups |
| 20 | WORM Compliance | Object lock | SEC, HIPAA compliance |

## Implementation Priority

### High Priority (Implement First)
1. **Test 11** - Network failures are most common production issue
2. **Test 15** - Consistency is fundamental correctness concern
3. **Test 18** - Rate limiting is critical for stability

### Medium Priority
4. **Test 14** - Distributed coordination is common pattern
5. **Test 19** - Backup is core use case
6. **Test 13** - Point-in-time recovery is important for DR

### Lower Priority (Nice to Have)
7. **Test 16** - Atomic snapshots (advanced)
8. **Test 12** - Bandwidth fairness (performance optimization)
9. **Test 17** - Directory operations (user convenience)
10. **Test 20** - WORM compliance (specialized requirement)

## Test Infrastructure Enhancements

To support these tests, we need:

```python
# Failure injection
class NetworkFailureInjector:
    def inject_partition(self, duration_sec):
        """Simulate network partition"""

    def inject_timeout(self, operation, timeout_ms):
        """Simulate timeout on specific operation"""

# Rate tracking
class RateLimiter:
    def track_requests(self, window_sec=60):
        """Track request rate over time"""

    def detect_503_responses(self):
        """Monitor for rate limit errors"""

# Consistency checker
class ConsistencyValidator:
    def verify_read_your_writes(self):
        """Validate read-your-writes consistency"""

    def measure_propagation_delay(self):
        """Measure cross-client consistency delay"""

# Backup utilities
class BackupManager:
    def create_snapshot(self, bucket, timestamp):
        """Create consistent snapshot"""

    def incremental_backup(self, since_timestamp):
        """Incremental backup"""

    def verify_backup(self, manifest):
        """Verify backup integrity"""
```

---

Generated-by: Claude AI
Signed-off-by: Luis Chamberlain <mcgrof@kernel.org>
