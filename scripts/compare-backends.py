#!/usr/bin/env python3
"""
Compare S3 Backend Performance and Compatibility

This script runs tests against multiple S3 backends and generates
a comprehensive comparison report with visualizations.
"""

import os
import sys
import json
import time
import subprocess
import click
import yaml
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass, asdict
from typing import List, Dict, Any, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed


@dataclass
class BackendConfig:
    """Configuration for an S3 backend"""

    name: str
    endpoint_url: str
    access_key: str
    secret_key: str
    region: str = "us-east-1"
    container_name: str = ""
    docker_service: str = ""


@dataclass
class TestSummary:
    """Summary of test results for a backend"""

    backend: str
    total: int
    passed: int
    failed: int
    errors: int
    skipped: int
    total_duration: float
    avg_duration: float
    pass_rate: float
    results: List[Dict]


# Pre-configured backends
BACKENDS = {
    "minio": BackendConfig(
        name="MinIO",
        endpoint_url="http://localhost:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        container_name="msst-minio",
        docker_service="minio",
    ),
    "rustfs": BackendConfig(
        name="RustFS",
        endpoint_url="http://localhost:9002",
        access_key="rustfsadmin",
        secret_key="rustfsadmin",
        container_name="msst-rustfs",
        docker_service="rustfs",
    ),
}


def wait_for_backend(backend: BackendConfig, timeout: int = 120) -> bool:
    """Wait for backend to be ready"""
    import urllib.request
    import urllib.error

    start_time = time.time()
    health_url = f"{backend.endpoint_url}/minio/health/live"

    click.echo(f"Waiting for {backend.name} to be ready...")

    while time.time() - start_time < timeout:
        try:
            req = urllib.request.Request(health_url, method="GET")
            with urllib.request.urlopen(req, timeout=5) as response:
                if response.status == 200:
                    click.echo(f"  {backend.name} is ready!")
                    return True
        except (urllib.error.URLError, urllib.error.HTTPError, ConnectionRefusedError):
            pass
        except Exception:
            pass
        time.sleep(2)

    click.echo(f"  {backend.name} failed to become ready within {timeout}s")
    return False


def start_docker_service(service: str) -> bool:
    """Start a Docker service"""
    click.echo(f"Starting Docker service: {service}")
    try:
        result = subprocess.run(
            ["docker", "compose", "up", "-d", service],
            capture_output=True,
            text=True,
            timeout=120,
        )
        if result.returncode != 0:
            click.echo(f"  Failed to start {service}: {result.stderr}")
            return False
        return True
    except subprocess.TimeoutExpired:
        click.echo(f"  Timeout starting {service}")
        return False
    except FileNotFoundError:
        # Try docker-compose (older syntax)
        try:
            result = subprocess.run(
                ["docker-compose", "up", "-d", service],
                capture_output=True,
                text=True,
                timeout=120,
            )
            return result.returncode == 0
        except Exception as e:
            click.echo(f"  Error starting service: {e}")
            return False


def stop_docker_service(service: str) -> bool:
    """Stop a Docker service"""
    click.echo(f"Stopping Docker service: {service}")
    try:
        result = subprocess.run(
            ["docker", "compose", "stop", service],
            capture_output=True,
            text=True,
            timeout=60,
        )
        return result.returncode == 0
    except Exception:
        return False


def run_tests(
    backend: BackendConfig,
    test_groups: List[str],
    output_dir: Path,
    parallel_jobs: int = 4,
    timeout: int = 300,
) -> Optional[TestSummary]:
    """Run tests against a specific backend"""

    click.echo(f"\n{'='*60}")
    click.echo(f"Running tests against {backend.name}")
    click.echo(f"{'='*60}")
    click.echo(f"Endpoint: {backend.endpoint_url}")
    click.echo(f"Test groups: {', '.join(test_groups)}")
    click.echo("")

    # Create temporary config file
    config = {
        "s3_endpoint_url": backend.endpoint_url,
        "s3_access_key": backend.access_key,
        "s3_secret_key": backend.secret_key,
        "s3_region": backend.region,
        "s3_bucket_prefix": f"msst-compare-{backend.name.lower()}",
        "s3_sdk": "boto3",
        "s3_sdk_version": "latest",
        "test_run_mode": "parallel",
        "test_parallel_jobs": parallel_jobs,
        "test_timeout": timeout,
    }

    config_file = output_dir / f"config_{backend.name.lower()}.yaml"
    with open(config_file, "w") as f:
        yaml.dump(config, f)

    results_file = output_dir / f"results_{backend.name.lower()}.json"
    all_results = []
    total_duration = 0

    # Run tests for each group
    for group in test_groups:
        click.echo(f"\n  Running {group} tests...")

        try:
            result = subprocess.run(
                [
                    sys.executable,
                    "scripts/test-runner.py",
                    "--config",
                    str(config_file),
                    "--group",
                    group,
                    "--output-dir",
                    str(output_dir / backend.name.lower() / group),
                    "--output-format",
                    "json",
                ],
                capture_output=True,
                text=True,
                timeout=timeout * 10,  # Allow plenty of time
            )

            # Parse results
            group_results_file = (
                output_dir / backend.name.lower() / group / "results.json"
            )
            if group_results_file.exists():
                with open(group_results_file) as f:
                    group_data = json.load(f)
                    all_results.extend(group_data.get("results", []))

        except subprocess.TimeoutExpired:
            click.echo(f"    Timeout running {group} tests")
        except Exception as e:
            click.echo(f"    Error running {group} tests: {e}")

    # Calculate summary
    if not all_results:
        click.echo(f"  No results collected for {backend.name}")
        return None

    total = len(all_results)
    passed = len([r for r in all_results if r.get("status") == "PASSED"])
    failed = len([r for r in all_results if r.get("status") == "FAILED"])
    errors = len([r for r in all_results if r.get("status") == "ERROR"])
    skipped = len([r for r in all_results if r.get("status") == "SKIPPED"])
    total_duration = sum(r.get("duration", 0) for r in all_results)
    avg_duration = total_duration / total if total > 0 else 0
    pass_rate = (passed / total * 100) if total > 0 else 0

    summary = TestSummary(
        backend=backend.name,
        total=total,
        passed=passed,
        failed=failed,
        errors=errors,
        skipped=skipped,
        total_duration=total_duration,
        avg_duration=avg_duration,
        pass_rate=pass_rate,
        results=all_results,
    )

    # Save combined results
    with open(results_file, "w") as f:
        json.dump(asdict(summary), f, indent=2)

    click.echo(f"\n  {backend.name} Summary:")
    click.echo(f"    Total: {total}, Passed: {passed}, Failed: {failed}")
    click.echo(f"    Pass Rate: {pass_rate:.1f}%")
    click.echo(f"    Total Duration: {total_duration:.2f}s")

    return summary


def create_ascii_bar(value: float, max_value: float, width: int = 40) -> str:
    """Create an ASCII progress bar"""
    filled = int((value / max_value) * width) if max_value > 0 else 0
    bar = "█" * filled + "░" * (width - filled)
    return bar


def generate_comparison_report(
    summaries: List[TestSummary],
    output_file: Path,
) -> str:
    """Generate a markdown comparison report with visualizations"""

    lines = []

    # Header
    lines.append("# S3 Backend Comparison Report")
    lines.append("")
    lines.append(f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("")
    lines.append("---")
    lines.append("")

    # Executive Summary
    lines.append("## Executive Summary")
    lines.append("")

    # Overview table
    lines.append(
        "| Backend | Tests | Passed | Failed | Errors | Pass Rate | Avg Time |"
    )
    lines.append(
        "|---------|-------|--------|--------|--------|-----------|----------|"
    )

    for s in summaries:
        lines.append(
            f"| **{s.backend}** | {s.total} | {s.passed} | {s.failed} | "
            f"{s.errors} | {s.pass_rate:.1f}% | {s.avg_duration:.3f}s |"
        )
    lines.append("")

    # Visual Comparison
    lines.append("## Visual Comparison")
    lines.append("")

    # Pass Rate Comparison
    lines.append("### Pass Rate Comparison")
    lines.append("")
    lines.append("```")
    max_rate = 100
    for s in summaries:
        bar = create_ascii_bar(s.pass_rate, max_rate, 50)
        lines.append(f"{s.backend:12} |{bar}| {s.pass_rate:.1f}%")
    lines.append("```")
    lines.append("")

    # Performance Comparison (Average Test Duration)
    lines.append("### Performance Comparison (Avg Test Duration)")
    lines.append("")
    lines.append("*Lower is better*")
    lines.append("")
    lines.append("```")
    max_duration = max(s.avg_duration for s in summaries) if summaries else 1
    for s in summaries:
        bar = create_ascii_bar(s.avg_duration, max_duration, 50)
        lines.append(f"{s.backend:12} |{bar}| {s.avg_duration:.3f}s")
    lines.append("```")
    lines.append("")

    # Total Test Duration
    lines.append("### Total Test Duration")
    lines.append("")
    lines.append("```")
    max_total = max(s.total_duration for s in summaries) if summaries else 1
    for s in summaries:
        bar = create_ascii_bar(s.total_duration, max_total, 50)
        lines.append(f"{s.backend:12} |{bar}| {s.total_duration:.1f}s")
    lines.append("```")
    lines.append("")

    # Detailed Results by Category
    lines.append("## Results by Test Category")
    lines.append("")

    # Group results by test category
    categories = {}
    for s in summaries:
        for r in s.results:
            group = r.get("test_group", "unknown")
            if group not in categories:
                categories[group] = {}
            if s.backend not in categories[group]:
                categories[group][s.backend] = {
                    "passed": 0,
                    "failed": 0,
                    "total": 0,
                    "duration": 0,
                }
            categories[group][s.backend]["total"] += 1
            categories[group][s.backend]["duration"] += r.get("duration", 0)
            if r.get("status") == "PASSED":
                categories[group][s.backend]["passed"] += 1
            else:
                categories[group][s.backend]["failed"] += 1

    for category, backends_data in sorted(categories.items()):
        lines.append(f"### {category.replace('_', ' ').title()}")
        lines.append("")
        lines.append("| Backend | Passed | Failed | Total | Duration | Pass Rate |")
        lines.append("|---------|--------|--------|-------|----------|-----------|")

        for backend_name, data in backends_data.items():
            rate = (data["passed"] / data["total"] * 100) if data["total"] > 0 else 0
            lines.append(
                f"| {backend_name} | {data['passed']} | {data['failed']} | "
                f"{data['total']} | {data['duration']:.2f}s | {rate:.1f}% |"
            )
        lines.append("")

        # Visual comparison for this category
        lines.append("```")
        for backend_name, data in backends_data.items():
            rate = (data["passed"] / data["total"] * 100) if data["total"] > 0 else 0
            bar = create_ascii_bar(rate, 100, 30)
            lines.append(f"{backend_name:12} |{bar}| {rate:.1f}%")
        lines.append("```")
        lines.append("")

    # Test-by-Test Comparison (differences only)
    lines.append("## Test Differences")
    lines.append("")
    lines.append("*Tests where backends produced different results*")
    lines.append("")

    # Build test comparison
    if len(summaries) >= 2:
        test_results = {}
        for s in summaries:
            for r in s.results:
                test_id = r.get("test_id")
                if test_id not in test_results:
                    test_results[test_id] = {
                        "test_name": r.get("test_name"),
                        "group": r.get("test_group"),
                    }
                test_results[test_id][s.backend] = {
                    "status": r.get("status"),
                    "duration": r.get("duration", 0),
                }

        # Find differences
        differences = []
        for test_id, data in test_results.items():
            statuses = set()
            for s in summaries:
                if s.backend in data:
                    statuses.add(data[s.backend].get("status"))
            if len(statuses) > 1:
                differences.append((test_id, data))

        if differences:
            lines.append(
                "| Test ID | Test Name | "
                + " | ".join(s.backend for s in summaries)
                + " |"
            )
            lines.append(
                "|---------|-----------|" + "|".join(["-------"] * len(summaries)) + "|"
            )

            for test_id, data in sorted(differences, key=lambda x: x[0]):
                row = [test_id, data.get("test_name", "")[:30]]
                for s in summaries:
                    if s.backend in data:
                        status = data[s.backend].get("status", "N/A")
                        emoji = {
                            "PASSED": "✅",
                            "FAILED": "❌",
                            "ERROR": "⚠️",
                            "SKIPPED": "⏭️",
                        }.get(status, "❓")
                        row.append(f"{emoji} {status}")
                    else:
                        row.append("N/A")
                lines.append("| " + " | ".join(row) + " |")
            lines.append("")
        else:
            lines.append(
                "*No differences found - all tests produced the same results on all backends.*"
            )
            lines.append("")

    # Performance Analysis
    lines.append("## Performance Analysis")
    lines.append("")

    if len(summaries) >= 2:
        # Find the fastest backend
        fastest = min(summaries, key=lambda s: s.avg_duration)
        slowest = max(summaries, key=lambda s: s.avg_duration)

        lines.append(
            f"**Fastest Backend:** {fastest.backend} (avg {fastest.avg_duration:.3f}s per test)"
        )
        lines.append(
            f"**Slowest Backend:** {slowest.backend} (avg {slowest.avg_duration:.3f}s per test)"
        )
        lines.append("")

        if fastest.avg_duration > 0:
            speedup = slowest.avg_duration / fastest.avg_duration
            lines.append(
                f"**Speed Difference:** {fastest.backend} is {speedup:.2f}x faster than {slowest.backend}"
            )
            lines.append("")

        # Best pass rate
        best_pass = max(summaries, key=lambda s: s.pass_rate)
        lines.append(
            f"**Best Compatibility:** {best_pass.backend} ({best_pass.pass_rate:.1f}% pass rate)"
        )
        lines.append("")

    # Top 10 Slowest Tests
    lines.append("### Top 10 Slowest Tests")
    lines.append("")

    all_test_times = []
    for s in summaries:
        for r in s.results:
            all_test_times.append(
                {
                    "backend": s.backend,
                    "test_id": r.get("test_id"),
                    "test_name": r.get("test_name"),
                    "duration": r.get("duration", 0),
                }
            )

    slowest_tests = sorted(all_test_times, key=lambda x: x["duration"], reverse=True)[
        :10
    ]

    if slowest_tests:
        lines.append("| Backend | Test ID | Test Name | Duration |")
        lines.append("|---------|---------|-----------|----------|")
        for t in slowest_tests:
            lines.append(
                f"| {t['backend']} | {t['test_id']} | {t['test_name'][:30]} | {t['duration']:.3f}s |"
            )
        lines.append("")

    # Conclusion
    lines.append("## Conclusion")
    lines.append("")

    if len(summaries) >= 2:
        # Determine winner
        best_overall = max(summaries, key=lambda s: s.pass_rate * 100 - s.avg_duration)

        lines.append("### Overall Assessment")
        lines.append("")

        for s in summaries:
            pros = []
            cons = []

            if s == fastest:
                pros.append("Fastest test execution")
            if s == slowest:
                cons.append("Slowest test execution")
            if s == best_pass:
                pros.append("Best S3 API compatibility")
            if s.pass_rate < best_pass.pass_rate:
                cons.append(
                    f"Lower pass rate ({s.pass_rate:.1f}% vs {best_pass.pass_rate:.1f}%)"
                )

            lines.append(f"**{s.backend}:**")
            if pros:
                lines.append(f"- Strengths: {', '.join(pros)}")
            if cons:
                lines.append(f"- Weaknesses: {', '.join(cons)}")
            lines.append("")

    lines.append("---")
    lines.append("")
    lines.append("*Report generated by MSST-S3 Backend Comparison Tool*")

    report = "\n".join(lines)

    # Save report
    with open(output_file, "w") as f:
        f.write(report)

    return report


@click.command()
@click.option(
    "--backends",
    "-b",
    multiple=True,
    default=["minio", "rustfs"],
    help="Backends to compare (can specify multiple)",
)
@click.option(
    "--groups",
    "-g",
    multiple=True,
    default=["basic", "multipart", "versioning"],
    help="Test groups to run (can specify multiple)",
)
@click.option(
    "--output-dir",
    "-o",
    type=click.Path(),
    default="comparison-results",
    help="Output directory for results",
)
@click.option(
    "--report",
    "-r",
    type=click.Path(),
    default="compare-s3-minio-vs-rustfs.md",
    help="Output report file",
)
@click.option(
    "--parallel-jobs",
    "-j",
    type=int,
    default=4,
    help="Number of parallel test jobs",
)
@click.option(
    "--start-containers/--no-start-containers",
    default=True,
    help="Start Docker containers before testing",
)
@click.option(
    "--stop-containers/--no-stop-containers",
    default=False,
    help="Stop Docker containers after testing",
)
@click.option(
    "--timeout",
    "-t",
    type=int,
    default=300,
    help="Timeout per test in seconds",
)
def main(
    backends: Tuple[str, ...],
    groups: Tuple[str, ...],
    output_dir: str,
    report: str,
    parallel_jobs: int,
    start_containers: bool,
    stop_containers: bool,
    timeout: int,
):
    """Compare S3 backends - runs tests and generates comparison report"""

    click.echo("=" * 60)
    click.echo("S3 Backend Comparison Tool")
    click.echo("=" * 60)
    click.echo(f"Backends: {', '.join(backends)}")
    click.echo(f"Test Groups: {', '.join(groups)}")
    click.echo("")

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    # Validate backends
    backend_configs = []
    for backend_name in backends:
        if backend_name not in BACKENDS:
            click.echo(f"Error: Unknown backend '{backend_name}'")
            click.echo(f"Available backends: {', '.join(BACKENDS.keys())}")
            sys.exit(1)
        backend_configs.append(BACKENDS[backend_name])

    # Start containers if requested
    if start_containers:
        click.echo("\nStarting Docker containers...")
        for backend in backend_configs:
            if backend.docker_service:
                if not start_docker_service(backend.docker_service):
                    click.echo(f"Warning: Failed to start {backend.name}")

        # Wait for backends to be ready
        click.echo("\nWaiting for backends to be ready...")
        for backend in backend_configs:
            if not wait_for_backend(backend):
                click.echo(f"Warning: {backend.name} may not be ready")

    # Run tests on each backend
    summaries = []
    for backend in backend_configs:
        summary = run_tests(
            backend=backend,
            test_groups=list(groups),
            output_dir=output_path,
            parallel_jobs=parallel_jobs,
            timeout=timeout,
        )
        if summary:
            summaries.append(summary)

    # Generate comparison report
    if summaries:
        click.echo("\n" + "=" * 60)
        click.echo("Generating Comparison Report")
        click.echo("=" * 60)

        report_path = Path(report)
        report_content = generate_comparison_report(summaries, report_path)

        click.echo(f"\nReport saved to: {report_path}")
        click.echo("\n" + "=" * 60)
        click.echo("COMPARISON SUMMARY")
        click.echo("=" * 60)

        # Print summary table
        for s in summaries:
            click.echo(f"\n{s.backend}:")
            click.echo(f"  Tests: {s.total}, Passed: {s.passed}, Failed: {s.failed}")
            click.echo(f"  Pass Rate: {s.pass_rate:.1f}%")
            click.echo(f"  Avg Duration: {s.avg_duration:.3f}s")
    else:
        click.echo("\nNo results collected. Please check if backends are running.")

    # Stop containers if requested
    if stop_containers:
        click.echo("\nStopping Docker containers...")
        for backend in backend_configs:
            if backend.docker_service:
                stop_docker_service(backend.docker_service)

    click.echo("\nDone!")


if __name__ == "__main__":
    main()
