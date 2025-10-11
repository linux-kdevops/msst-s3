#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
S3 Interoperability Test Suite – SDK Capability Profile Template
================================================================

Core idea
---------
Do NOT bind tests to a specific AWS SDK version only. Instead, model SDK
behaviors as **capability profiles**. Let your runner resolve an effective
profile from:

    1) A static mapping of version ranges -> capability flags
    2) A probe phase that *dynamically* discovers behavior at runtime
    3) An explicit override (e.g., from Kconfig or a JSON file)

Tests then read **capabilities** and adapt *expectations* (assertions) and
*setup* (e.g., addressing mode) without forking entire tests.

Minimal moving parts in this file
---------------------------------
- Data model (`CapabilityProfile`, `SDKSpec`, `ProbeResult`)
- A tiny **semver constraint solver** (no external deps)
- A **static mapping** example for a couple of SDK/version bands
- Probe hooks (no external I/O here; you’ll wire real SDK calls)
- A **merge** routine: mapping ⊕ probes ⊕ overrides
- Kconfig/ENV glue (reads S3_SDK, S3_SDK_VERSION, etc.)
- CLI that writes the resolved capabilities JSON to disk or stdout

How to use
----------
1) Wire your build to set the following (via Kconfig -> env or a small file):
   - S3_SDK (e.g., "aws-sdk-go-v2", "boto3", "aws-sdk-java-v2", ...)
   - S3_SDK_VERSION (e.g., "1.30.0", "1.34.2", "latest")
   - S3_CAP_PROFILE_OVERRIDE (optional: "1" or "0")
   - S3_CAP_PROFILE_JSON (path to JSON overrides if override is "1")

2) (Optional) Implement real probes in `run_probes_for_sdk()` by making
   **small** wire calls against your target endpoint using the selected SDK.
   Each probe should be minimal and return a boolean or small enum.

3) Call:
python s3_caps_template.py --out caps.json

This generates a single JSON file your tests can load:

{
    "sdk": "aws-sdk-go-v2",
    "version": "1.30.0",
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
    "sources": ["mapping", "probes", "override"]
}

4) In tests, branch **expectations** (not test bodies) on flags:
Example (PyTest-ish pseudocode):

def test_plus_space_listing(client, caps):
key = "a+b"
client.put_object(bucket=BUCKET, key=key, body=b"x")
# ...
prefix = "a " if caps["list_objects_url_plus_treated_as_space"] else "a+"
listed = client.list_objects_v2(bucket=BUCKET, prefix=prefix)
assert any(o.key == key for o in listed)


Kconfig snippet (example)
-------------------------
This is illustrative; keep your real one in Kconfig files. You can pipe it to
your build system and export as env variables for this script.

 menu "S3 Interop SDK Selection"

 config S3_SDK
     string "SDK implementation"
     default "boto3"
     help
       Options: boto3, botocore, aws-sdk-java-v1, aws-sdk-java-v2,
                aws-sdk-go-v1, aws-sdk-go-v2, aws-sdk-js-v2, aws-sdk-js-v3,
                dotnet, rust

 config S3_SDK_VERSION
     string "SDK version (semver or VCS ref)"
     default "latest"

 config S3_CAP_PROFILE_OVERRIDE
     bool "Manually override detected capability profile"
     default n

 config S3_CAP_PROFILE_JSON
     string "Path to capabilities override JSON"
     depends on S3_CAP_PROFILE_OVERRIDE

 endmenu

CI Matrix sketch (docker-ish, yaml-ish)
---------------------------------------
Run cells across SDKs and versions, and (optionally) multiple S3-compatible backends.

 jobs:
   interop:
     strategy:
       matrix:
         sdk: [boto3, aws-sdk-go-v2]
         version_band: [min, lts, latest]
         endpoint: [aws, minio, ceph-rgw]
     steps:
       - run: make install-sdk SDK=${{matrix.sdk}} VERSION=${{matrix.version_band}}
       - run: python s3_caps_template.py --out caps.json
       - run: pytest -q --caps caps.json

Extending capability flags (common axes)
----------------------------------------
- Auth & signing: sigv2/sigv4, aws-chunked, UNSIGNED-PAYLOAD, clock-skew
- Addressing & encoding: virtual-hosted vs path-style, '+' vs ' ' in listing,
URL encoding rules, trailing slashes
- Retries & idempotency: retry mode, 301 region redirects, 307/308 body replay
- I/O defaults: Expect: 100-Continue, timeouts, multipart defaults, checksums
- SSE headers: SSE-C/SSE-S3/SSE-KMS casing/signing rules
- Presigned URLs: canonicalization & encoding subtleties

This template bakes in a starter set. Add/remove flags as needed.

"""

from __future__ import annotations

import argparse
import dataclasses
import json
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any


# --------------------------------------------------------------------------------------
# Data Model
# --------------------------------------------------------------------------------------


@dataclass(frozen=True)
class SDKSpec:
    """Identifies the chosen SDK and version.

    Fields
    ------
    name : str
        Canonical SDK name (e.g., "boto3", "aws-sdk-go-v2", "aws-sdk-java-v2").
    version : str
        Version string (e.g., "1.30.0"). Use "latest" to represent "no pin".

    Notes
    -----
    - Keep the name stable; the static mapping below keys off this.
    - The 'version' is interpreted by our tiny semver helper; non-numeric
      suffixes are ignored for ordering (e.g., "1.30.0-rc1" ~ "1.30.0").
    """

    name: str
    version: str


@dataclass
class CapabilityProfile:
    """Represents behavior flags that tests can branch on.

    You should add/remove fields as your interop needs evolve. Start small.
    """

    # Auth & signing
    sigv4_chunked: bool = False
    unsigned_payload_allowed: bool = True  # Allow "UNSIGNED-PAYLOAD" on PUT/POST
    # Addressing & encoding
    virtual_hosted_default: bool = True
    list_objects_v1: bool = False
    list_objects_url_plus_treated_as_space: bool = False
    # Retries & redirects
    retry_mode: str = "standard"  # e.g., "standard", "adaptive", "legacy"
    follows_301_region_redirect: bool = True
    follows_307_on_put: bool = True  # if the client replays body on 307/308
    # Checksums / I/O defaults
    crc32c_default: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return dataclasses.asdict(self)


@dataclass
class ProbeResult:
    """Outputs from lightweight runtime probes.

    Fill in fields as you wire real probe logic. Default None means "unknown".
    """

    sigv4_chunked: Optional[bool] = None
    unsigned_payload_allowed: Optional[bool] = None
    virtual_hosted_default: Optional[bool] = None
    list_objects_v1: Optional[bool] = None
    list_objects_url_plus_treated_as_space: Optional[bool] = None
    retry_mode: Optional[str] = None
    follows_301_region_redirect: Optional[bool] = None
    follows_307_on_put: Optional[bool] = None
    crc32c_default: Optional[bool] = None

    def as_overrides(self) -> Dict[str, Any]:
        """Return only fields that are not None (i.e., actually observed)."""
        d = dataclasses.asdict(self)
        return {k: v for k, v in d.items() if v is not None}


# --------------------------------------------------------------------------------------
# Semver helpers (no external dependencies)
# --------------------------------------------------------------------------------------

_VERSION_RE = re.compile(r"(\d+)(?:\.(\d+))?(?:\.(\d+))?")


def _parse_version_triplet(v: str) -> Tuple[int, int, int]:
    """
    Convert a version string like '1.30.0-rc1' into a comparable (1,30,0).
    Missing parts are treated as zero. Non-numeric suffixes are ignored.

    This is intentionally simple. If you need full semver, consider adding
    'packaging' later. For now this is enough to bucket version bands.
    """
    m = _VERSION_RE.search(v)
    if not m:
        return (0, 0, 0)
    parts = [p for p in m.groups()]
    nums = []
    for p in parts:
        nums.append(int(p) if p is not None else 0)
    while len(nums) < 3:
        nums.append(0)
    return tuple(nums[:3])


def _compare_versions(a: str, b: str) -> int:
    """
    Compare two version strings. Return -1 if a<b, 0 if a==b, 1 if a>b.
    """
    ta = _parse_version_triplet(a)
    tb = _parse_version_triplet(b)
    return (ta > tb) - (ta < tb)


def _match_constraint(version: str, constraint: str) -> bool:
    """
    Very small constraint parser supporting:
      - >= X.Y.Z
      - >  X.Y.Z
      - <= X.Y.Z
      - <  X.Y.Z
      - == X.Y.Z
    Combine with commas to represent AND clauses:
      '>= 1.30.0, < 2.0.0'
    """
    version = version or "0.0.0"
    clauses = [c.strip() for c in constraint.split(",") if c.strip()]
    for clause in clauses:
        if clause.startswith(">="):
            if _compare_versions(version, clause[2:].strip()) < 0:
                return False
        elif clause.startswith("<="):
            if _compare_versions(version, clause[2:].strip()) > 0:
                return False
        elif clause.startswith(">"):
            if _compare_versions(version, clause[1:].strip()) <= 0:
                return False
        elif clause.startswith("<"):
            if _compare_versions(version, clause[1:].strip()) >= 0:
                return False
        elif clause.startswith("=="):
            if _compare_versions(version, clause[2:].strip()) != 0:
                return False
        else:
            # Bare number means equality
            if _compare_versions(version, clause.strip()) != 0:
                return False
    return True


# --------------------------------------------------------------------------------------
# Static Mapping (starter set – extend for your needs)
# --------------------------------------------------------------------------------------

# IMPORTANT:
# - Keep entries coarse and *behavioral*. Don't overfit to every micro version.
# - Prefer ranges that reflect *real* behavioral transitions you observed.
STATIC_CAPABILITY_MAPPING: List[Dict[str, Any]] = [
    # Example: aws-sdk-go-v2 "modern" band
    {
        "sdk": "aws-sdk-go-v2",
        "version_constraint": ">= 1.25.0, < 2.0.0",
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
        "rationale": "Post-1.25 behavior stabilizations; ListObjectsV2 default, standard retries.",
    },
    # Example: boto3/botocore – broad band (you will refine this)
    {
        "sdk": "boto3",
        "version_constraint": ">= 1.26.0, < 2.0.0",
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
        "rationale": "Modern botocore defaults; V2 listing; standard retry policy.",
    },
    # Add further entries for: aws-sdk-java-v1/v2, aws-sdk-js-v2/v3, .NET, Rust, etc.
]


def lookup_static_profile(spec: SDKSpec) -> CapabilityProfile:
    """
    Resolve a CapabilityProfile from STATIC_CAPABILITY_MAPPING or return defaults.

    The *first* matching entry wins. Order your mapping from most-specific to
    most-general ranges.
    """
    for row in STATIC_CAPABILITY_MAPPING:
        if row.get("sdk") != spec.name:
            continue
        vc = row.get("version_constraint") or ""
        if spec.version == "latest" or not vc or _match_constraint(spec.version, vc):
            prof = CapabilityProfile(**row["profile"])
            return prof
    # Fallback default if nothing matched
    return CapabilityProfile()


# --------------------------------------------------------------------------------------
# Probe Phase (wire your SDK calls here)
# --------------------------------------------------------------------------------------


def run_probes_for_sdk(
    spec: SDKSpec, endpoint_hint: Optional[str] = None
) -> ProbeResult:
    """
    Run small, safe probes to *discover* behavior for the selected SDK at runtime.

    IMPORTANT:
    - Keep each probe minimal and idempotent. Favor HEAD/GET and tiny PUTs.
    - Handle absence of permissions gracefully (set the field to None).
    - Execute the minimum viable set that surfaces *behavioral* differences.
    - Endpoint/credentials should be provided by the surrounding harness (env).

    What to probe (starter ideas):
    - sigv4_chunked: Attempt a small streaming upload requiring aws-chunked; see if it succeeds.
    - unsigned_payload_allowed: Try PUT with 'UNSIGNED-PAYLOAD'.
    - virtual_hosted_default: Introspect the generated request's host style.
    - list_objects_url_plus_treated_as_space: Upload keys "a+b" and "a b"; see how listing behaves.
    - follows_301_region_redirect: Create a cross-region bucket URL and see if the client follows.
    - follows_307_on_put: Simulate/force a 307 and verify body replay.
    - retry_mode: Observe headers or consult SDK's runtime config if available.
    - crc32c_default: Check if the client emits CRC32C by default on uploads.

    This template returns a neutral "no observation" result. Replace with real logic.
    """
    # TODO: Replace placeholders with real SDK probes for your environment.
    # For now we return an empty ProbeResult (all None), so nothing overrides.
    return ProbeResult()


# --------------------------------------------------------------------------------------
# Overrides (from Kconfig/env-provided JSON)
# --------------------------------------------------------------------------------------


def load_override_json(path: Optional[str]) -> Dict[str, Any]:
    """
    Load a JSON file that can override the capability flags, e.g.:

        {
          "sigv4_chunked": false,
          "retry_mode": "adaptive"
        }

    Non-specified keys are ignored; only present keys override.
    """
    if not path:
        return {}
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Override JSON not found: {path}")
    with p.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError("Override JSON must contain an object at top-level.")
    return data


# --------------------------------------------------------------------------------------
# Merge logic: mapping ⊕ probes ⊕ override
# --------------------------------------------------------------------------------------


def merge_capabilities(
    base: CapabilityProfile,
    probe: ProbeResult,
    override: Dict[str, Any],
    source_trace: Optional[List[str]] = None,
) -> CapabilityProfile:
    """
    Merge order (last wins):
      1) base profile (from static mapping)
      2) probe-discovered values (only non-None)
      3) explicit overrides (JSON / Kconfig)

    `source_trace` (if passed) is appended with names of sources applied.
    """
    merged = base.to_dict()
    if source_trace is not None:
        source_trace.append("mapping")
    # Apply probe observations
    for k, v in probe.as_overrides().items():
        if k in merged:
            merged[k] = v
    if probe.as_overrides():
        if source_trace is not None:
            source_trace.append("probes")
    # Apply explicit overrides
    for k, v in override.items():
        if k in merged:
            merged[k] = v
    if override:
        if source_trace is not None:
            source_trace.append("override")
    return CapabilityProfile(**merged)


# --------------------------------------------------------------------------------------
# ENV / Kconfig glue
# --------------------------------------------------------------------------------------


def read_env_spec() -> Tuple[SDKSpec, bool, Optional[str], Optional[str]]:
    """
    Read SDK spec and overrides from environment variables (typically exported
    by your Kconfig-driven build):

    - S3_SDK                 (required) e.g., 'aws-sdk-go-v2'
    - S3_SDK_VERSION         (optional) e.g., '1.30.0' or 'latest'
    - S3_CAP_PROFILE_OVERRIDE(optional) '1' or '0' – whether to load override JSON
    - S3_CAP_PROFILE_JSON    (optional) path to JSON with overrides
    - S3_ENDPOINT_HINT       (optional) endpoint to run probes against (string)

    Returns
    -------
    (spec, use_override, override_json_path, endpoint_hint)
    """
    name = os.getenv("S3_SDK", "").strip()
    if not name:
        raise EnvironmentError("S3_SDK is required (e.g., 'boto3', 'aws-sdk-go-v2').")

    version = os.getenv("S3_SDK_VERSION", "latest").strip() or "latest"
    use_override = os.getenv("S3_CAP_PROFILE_OVERRIDE", "0").strip() in (
        "1",
        "true",
        "TRUE",
    )
    override_json = os.getenv("S3_CAP_PROFILE_JSON", "").strip() or None
    endpoint_hint = os.getenv("S3_ENDPOINT_HINT", "").strip() or None

    return (
        SDKSpec(name=name, version=version),
        use_override,
        override_json,
        endpoint_hint,
    )


# --------------------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------------------


def build_caps_document(
    spec: SDKSpec,
    endpoint_hint: Optional[str],
    override_json_path: Optional[str],
    force_override: bool,
) -> Dict[str, Any]:
    """
    Construct the final capabilities document ready for writing to file/stdout.

    The returned dict includes:
      - sdk
      - version
      - profile (dict of flags)
      - sources (list of strings: "mapping", "probes", "override")
    """
    source_trace: List[str] = []
    base = lookup_static_profile(spec)
    probe = run_probes_for_sdk(spec, endpoint_hint=endpoint_hint)
    overrides = load_override_json(override_json_path) if force_override else {}
    merged = merge_capabilities(base, probe, overrides, source_trace=source_trace)

    return {
        "sdk": spec.name,
        "version": spec.version,
        "profile": merged.to_dict(),
        "sources": source_trace,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Resolve S3 SDK capability profile (mapping ⊕ probes ⊕ overrides)."
    )
    parser.add_argument(
        "--out",
        help="Write resulting JSON to this path. Omit to print to stdout.",
        default=None,
    )
    parser.add_argument(
        "--sdk",
        help="Override S3_SDK env (e.g., 'aws-sdk-go-v2').",
        default=None,
    )
    parser.add_argument(
        "--version",
        help="Override S3_SDK_VERSION env (e.g., '1.30.0', 'latest').",
        default=None,
    )
    parser.add_argument(
        "--override-json",
        help="Path to a JSON file with capability overrides.",
        default=None,
    )
    parser.add_argument(
        "--force-override",
        action="store_true",
        help="Force applying --override-json even if S3_CAP_PROFILE_OVERRIDE is not set.",
    )
    parser.add_argument(
        "--endpoint-hint",
        help="Optional endpoint target for probes (e.g., 'https://s3.us-east-1.amazonaws.com').",
        default=None,
    )

    args = parser.parse_args()

    # Read env/Kconfig, then apply CLI overrides (CLI has higher precedence).
    spec, use_override_env, override_json_env, endpoint_hint_env = read_env_spec()

    if args.sdk:
        spec = SDKSpec(name=args.sdk.strip(), version=spec.version)
    if args.version:
        spec = SDKSpec(name=spec.name, version=args.version.strip())

    override_json_final = args.override_json or (
        override_json_env if use_override_env else None
    )
    endpoint_hint_final = args.endpoint_hint or endpoint_hint_env
    force_override = args.force_override or bool(args.override_json) or False

    doc = build_caps_document(
        spec=spec,
        endpoint_hint=endpoint_hint_final,
        override_json_path=override_json_final,
        force_override=force_override,
    )

    payload = json.dumps(doc, indent=2, sort_keys=True)
    if args.out:
        Path(args.out).write_text(payload + "\n", encoding="utf-8")
    else:
        print(payload)


# --------------------------------------------------------------------------------------
# PyTest convenience (optional): load caps.json once per session
# --------------------------------------------------------------------------------------
CAPS_JSON_ENV = "S3_CAPS_JSON_PATH"


def load_caps_for_tests(path: Optional[str] = None) -> Dict[str, Any]:
    """
    Tests can call this to load the resolved capability profile.

    If `path` is None, it looks at the env var S3_CAPS_JSON_PATH.
    """
    path = path or os.getenv(CAPS_JSON_ENV, "")
    if not path:
        raise FileNotFoundError(
            "Capability JSON not specified. Set S3_CAPS_JSON_PATH or pass a path."
        )
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


# Example usage in tests (keep this as reference; not executed here):
EXAMPLE_TEST_SNIPPET = r"""
import pytest
from my_caps_module import load_caps_for_tests

@pytest.fixture(scope="session")
def caps():
 return load_caps_for_tests()

def test_list_plus_behavior(client, caps):
 key = "a+b"
 client.put_object(bucket="bkt", key=key, body=b"x")
 prefix = "a " if caps["profile"]["list_objects_url_plus_treated_as_space"] else "a+"
 listed = client.list_objects_v2(bucket="bkt", prefix=prefix)
 assert any(o.key == key for o in listed)
"""

# --------------------------------------------------------------------------------------
# Entrypoint
# --------------------------------------------------------------------------------------

if __name__ == "__main__":
    main()
