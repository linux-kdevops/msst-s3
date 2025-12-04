# MSST-S3 Example Prompts

This document showcases example prompts used to develop features for MSST-S3
and the resulting commits they produced.

---

## Example 1: Add RustFS Backend Comparison (p1.txt)

### Prompts Used

**P1 - Initial Request:**
```
I'd like you to add support test sa new S3 object storage solution and I
want to compare and contrast it speed, tests, etc against minio. Its
https://github.com/rustfs/rustfs.git in case you need the code its on
/path/server/rustfs but the download part has instructions on how to get it,
script or docker, whatever, pick whatever is easier to maintain. If using docker
for minio then lets use docker for this too. Your output will be a
compare-s3-minio-vs-rustfs.md with beautiful images of results of the tests we
have done . The download page is https://rustfs.com/download/?platform=linux
```

**P2 - Commit Request:**
```
Great commit all this
```

**P3 - Documentation Request:**
```
Great now refer to this on the top level README.md and give instructions on
how to run a comparison
```

### Resulting Commits

| Commit | Description |
|--------|-------------|
| [83b8cf176102](https://github.com/linux-kdevops/msst-s3/commit/83b8cf1761020b53679a6a5ea4187092458a3637) | Add RustFS support and comparison tooling for S3 backend testing |
| [79dfa065d2a4](https://github.com/linux-kdevops/msst-s3/commit/79dfa065d2a495207268fc742856956655098ecb) | docs: Add RustFS backend comparison documentation to README |

### Files Created/Modified

**New Files:**
- `scripts/compare-backends.py` - Backend comparison tool (705 lines)
- `defconfigs/docker-rustfs` - RustFS defconfig (49 lines)
- `compare-s3-minio-vs-rustfs.md` - Comparison report (337 lines)

**Modified Files:**
- `docker-compose.yml` - Added RustFS service
- `README.md` - Added Backend Comparison section

### Test Results Achieved

| Backend | Pass Rate | Avg Duration | Tests |
|---------|-----------|--------------|-------|
| MinIO | 98.7% | 0.867s | 399 |
| RustFS | 94.7% | 0.792s | 399 |

---

## Original Project Prompt

The following was the original prompt used to bootstrap the MSST-S3 project:

### About S3

S3 is an object storage REST API.

### Goal

Write an S3 interoperability test-suite, to address this I want you to focus
on the following tests suites and implement a vendor neutral solution.

1. s3-tests - The official Ceph S3 compatibility test suite
2. minio/mint - MinIO's testing framework for S3 API compatibility
3. aws-sdk- test suites* - AWS SDK test frameworks
4. boto3 with custom test frameworks
5. s3compat or similar S3-specific testing projects

### Before you proceed

Git clone each git tree under ~/devel/ and do a code analysis of each.
Then *think* hard about this problem.

### Adopt kconfig

Look at ~/devel/kconfig/ for a generic implementation of kconfig.
Then look at ~/devel/init-kconfig/ for an example of how to adopt
kconfig into a new project.

Use the origin/yamlconfig branch to embrace kconfig for this new project
as a git subtree. You can see how ~/devel/kdevops/Makefile.subtrees does
this.

### Adopt ansible and Makefiles

Learn to adopt Makefile targets for ansible targets as we do in kdevops,
you can use the kdevops/workflows/demos/reboot-limit/ as a simple demo
of how to do this. Look also at ~/devel/kdevops/playbooks/roles/reboot-limit/
for an example role and ~/devel/kdevops/playbooks/reboot-limit.yml.

### Use Python

Use Python for the test suite.

### Itemize tests

Help come up with itemized tests as itemized in spirit with the Linux
filesystem tests ~/devel/xfstests-dev. You can look for a simpler
example on ~/devel/blktests.
