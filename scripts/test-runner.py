#!/usr/bin/env python3
"""
MSST-S3 Test Runner
Main test execution framework for S3 interoperability testing
"""

import os
import sys
import yaml
import json
import time
import click
import importlib.util
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, asdict
from enum import Enum

# Add tests directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "tests"))

# Import SDK capability support
try:
    from common.sdk_capabilities import (
        SDKSpec,
        build_caps_document,
        load_caps_for_tests,
    )

    SDK_CAPS_AVAILABLE = True
except ImportError:
    SDK_CAPS_AVAILABLE = False
    print(
        "Warning: SDK capabilities module not available. Tests will use default capabilities.",
        file=sys.stderr,
    )


class TestStatus(Enum):
    """Test execution status"""

    PASSED = "PASSED"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"
    ERROR = "ERROR"
    TIMEOUT = "TIMEOUT"


@dataclass
class TestResult:
    """Test execution result"""

    test_id: str
    test_name: str
    test_group: str
    status: TestStatus
    duration: float
    message: str = ""
    error: str = ""
    timestamp: str = ""

    def to_dict(self):
        """Convert to dictionary"""
        result = asdict(self)
        result["status"] = self.status.value
        return result


class TestDiscovery:
    """Discover and manage test cases"""

    TEST_GROUPS = {
        "basic": (1, 99),
        "multipart": (100, 199),
        "versioning": (200, 299),
        "acl": (300, 399),
        "encryption": (400, 499),
        "lifecycle": (500, 599),
        "performance": (600, 699),
        "stress": (700, 799),
        "edge": (800, 899),
        "integration": (900, 1000),
        "advanced_basic": (1001, 1100),
        "advanced_multipart": (1101, 1200),
        "replication": (1201, 1300),
        "notifications": (1301, 1400),
        "lambda": (1401, 1500),
        "analytics": (1501, 1600),
        "batch": (1601, 1700),
        "tiering": (1701, 1800),
        "access_points": (1801, 1900),
        "object_lambda": (1901, 2000),
        "compliance": (2001, 2100),
        "disaster_recovery": (2101, 2200),
        "data_lake": (2201, 2300),
        "ml_pipeline": (2301, 2400),
        "iot_streaming": (2401, 2500),
        "security": (2501, 2600),
        "edge_computing": (2601, 2700),
        "microservices": (2701, 2800),
        "cost_optimization": (2801, 2900),
        "hybrid_cloud": (2901, 3000),
    }

    def __init__(self, test_dir: Path):
        self.test_dir = test_dir
        self.tests = {}
        self._discover_tests()

    def _discover_tests(self):
        """Discover all available tests"""
        for group_name, (start_id, end_id) in self.TEST_GROUPS.items():
            group_dir = self.test_dir / group_name
            if not group_dir.exists():
                continue

            # Look for test files (e.g., 1, 2, 001.py, 36.py, etc.)
            for test_file in sorted(group_dir.glob("[0-9]*.py")):
                if test_file.is_file():
                    test_id = test_file.stem.split(".")[0]  # Get numeric part
                    if test_id.isdigit():
                        test_num = int(test_id)
                        if start_id <= test_num <= end_id:
                            self.tests[test_id] = {
                                "id": test_id,
                                "group": group_name,
                                "path": test_file,
                                "name": f"test_{test_id}",
                            }

    def get_tests_by_group(self, group: str) -> List[Dict]:
        """Get all tests in a specific group"""
        return [t for t in self.tests.values() if t["group"] == group]

    def get_test_by_id(self, test_id: str) -> Optional[Dict]:
        """Get a specific test by ID"""
        # Handle both formats: '36' and '036'
        return self.tests.get(test_id)

    def get_all_tests(self) -> List[Dict]:
        """Get all discovered tests"""
        return list(self.tests.values())


class TestExecutor:
    """Execute individual test cases"""

    def __init__(
        self, config: Dict[str, Any], capabilities: Optional[Dict[str, Any]] = None
    ):
        self.config = config
        self.capabilities = capabilities
        self.s3_client = None
        self._setup_s3_client()

    def _setup_s3_client(self):
        """Setup S3 client from configuration"""
        # Import common module for S3 client setup
        from common.s3_client import S3Client

        # Extract capability profile if available
        caps_profile = None
        if self.capabilities and "profile" in self.capabilities:
            caps_profile = self.capabilities["profile"]

        self.s3_client = S3Client(
            endpoint_url=self.config.get("s3_endpoint_url", "http://localhost:9000"),
            access_key=self.config.get("s3_access_key", "minioadmin"),
            secret_key=self.config.get("s3_secret_key", "minioadmin"),
            region=self.config.get("s3_region", "us-east-1"),
            use_ssl=self.config.get("s3_use_ssl", False),
            verify_ssl=self.config.get("s3_verify_ssl", True),
            capabilities=caps_profile,
        )

    def execute_test(self, test_info: Dict) -> TestResult:
        """Execute a single test"""
        test_id = test_info["id"]
        test_path = test_info["path"]
        test_group = test_info["group"]

        start_time = time.time()
        timestamp = datetime.now().isoformat()

        try:
            # Load test module dynamically
            spec = importlib.util.spec_from_file_location(f"test_{test_id}", test_path)
            if spec is None or spec.loader is None:
                raise ImportError(f"Cannot load test from {test_path}")

            test_module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(test_module)

            # Look for test function or class
            test_func_name = f"test_{test_id}"
            if hasattr(test_module, test_func_name):
                test_func = getattr(test_module, test_func_name)
            elif hasattr(test_module, "run"):
                test_func = test_module.run
            else:
                raise AttributeError(
                    f"No test function '{test_func_name}' or 'run' found in {test_path}"
                )

            # Execute test with S3 client and config
            test_func(self.s3_client, self.config)

            duration = time.time() - start_time
            return TestResult(
                test_id=test_id,
                test_name=test_info["name"],
                test_group=test_group,
                status=TestStatus.PASSED,
                duration=duration,
                message="Test passed successfully",
                timestamp=timestamp,
            )

        except AssertionError as e:
            duration = time.time() - start_time
            return TestResult(
                test_id=test_id,
                test_name=test_info["name"],
                test_group=test_group,
                status=TestStatus.FAILED,
                duration=duration,
                message=str(e),
                error=traceback.format_exc(),
                timestamp=timestamp,
            )

        except Exception as e:
            duration = time.time() - start_time
            return TestResult(
                test_id=test_id,
                test_name=test_info["name"],
                test_group=test_group,
                status=TestStatus.ERROR,
                duration=duration,
                message=f"Test error: {str(e)}",
                error=traceback.format_exc(),
                timestamp=timestamp,
            )


class ResultFormatter:
    """Format and save test results"""

    @staticmethod
    def format_json(results: List[TestResult]) -> str:
        """Format results as JSON"""
        data = {
            "timestamp": datetime.now().isoformat(),
            "total": len(results),
            "passed": len([r for r in results if r.status == TestStatus.PASSED]),
            "failed": len([r for r in results if r.status == TestStatus.FAILED]),
            "skipped": len([r for r in results if r.status == TestStatus.SKIPPED]),
            "errors": len([r for r in results if r.status == TestStatus.ERROR]),
            "results": [r.to_dict() for r in results],
        }
        return json.dumps(data, indent=2)

    @staticmethod
    def format_yaml(results: List[TestResult]) -> str:
        """Format results as YAML"""
        data = {
            "timestamp": datetime.now().isoformat(),
            "total": len(results),
            "passed": len([r for r in results if r.status == TestStatus.PASSED]),
            "failed": len([r for r in results if r.status == TestStatus.FAILED]),
            "skipped": len([r for r in results if r.status == TestStatus.SKIPPED]),
            "errors": len([r for r in results if r.status == TestStatus.ERROR]),
            "results": [r.to_dict() for r in results],
        }
        return yaml.dump(data, default_flow_style=False, sort_keys=False)

    @staticmethod
    def format_text(results: List[TestResult]) -> str:
        """Format results as human-readable text"""
        lines = []
        lines.append("=" * 80)
        lines.append("S3 Test Results Summary")
        lines.append("=" * 80)
        lines.append(f"Timestamp: {datetime.now().isoformat()}")
        lines.append("")

        # Summary statistics
        total = len(results)
        passed = len([r for r in results if r.status == TestStatus.PASSED])
        failed = len([r for r in results if r.status == TestStatus.FAILED])
        skipped = len([r for r in results if r.status == TestStatus.SKIPPED])
        errors = len([r for r in results if r.status == TestStatus.ERROR])

        lines.append(f"Total Tests: {total}")
        lines.append(f"Passed: {passed} ({passed*100/total:.1f}%)")
        lines.append(f"Failed: {failed} ({failed*100/total:.1f}%)")
        lines.append(f"Skipped: {skipped} ({skipped*100/total:.1f}%)")
        lines.append(f"Errors: {errors} ({errors*100/total:.1f}%)")
        lines.append("")

        # Detailed results
        lines.append("-" * 80)
        lines.append("Test Results:")
        lines.append("-" * 80)

        for result in sorted(results, key=lambda r: r.test_id):
            status_char = {
                TestStatus.PASSED: "✓",
                TestStatus.FAILED: "✗",
                TestStatus.SKIPPED: "○",
                TestStatus.ERROR: "!",
                TestStatus.TIMEOUT: "⏱",
            }.get(result.status, "?")

            lines.append(
                f"[{status_char}] {result.test_id}: {result.test_name} "
                f"({result.test_group}) - {result.status.value} "
                f"[{result.duration:.3f}s]"
            )

            if result.message and result.status != TestStatus.PASSED:
                lines.append(f"    {result.message}")

        lines.append("=" * 80)
        return "\n".join(lines)

    @staticmethod
    def format_junit(results: List[TestResult]) -> str:
        """Format results as JUnit XML"""
        from xml.etree.ElementTree import Element, SubElement, tostring
        from xml.dom import minidom

        # Create root testsuites element
        testsuites = Element("testsuites")
        testsuites.set("name", "S3 Interoperability Tests")
        testsuites.set("timestamp", datetime.now().isoformat())

        # Group results by test group
        groups = {}
        for result in results:
            if result.test_group not in groups:
                groups[result.test_group] = []
            groups[result.test_group].append(result)

        # Create testsuite for each group
        for group_name, group_results in groups.items():
            testsuite = SubElement(testsuites, "testsuite")
            testsuite.set("name", group_name)
            testsuite.set("tests", str(len(group_results)))
            testsuite.set(
                "failures",
                str(len([r for r in group_results if r.status == TestStatus.FAILED])),
            )
            testsuite.set(
                "errors",
                str(len([r for r in group_results if r.status == TestStatus.ERROR])),
            )
            testsuite.set(
                "skipped",
                str(len([r for r in group_results if r.status == TestStatus.SKIPPED])),
            )
            testsuite.set("time", str(sum(r.duration for r in group_results)))

            for result in group_results:
                testcase = SubElement(testsuite, "testcase")
                testcase.set("classname", f"s3.{result.test_group}")
                testcase.set("name", result.test_name)
                testcase.set("time", str(result.duration))

                if result.status == TestStatus.FAILED:
                    failure = SubElement(testcase, "failure")
                    failure.set("message", result.message)
                    failure.text = result.error
                elif result.status == TestStatus.ERROR:
                    error = SubElement(testcase, "error")
                    error.set("message", result.message)
                    error.text = result.error
                elif result.status == TestStatus.SKIPPED:
                    skipped = SubElement(testcase, "skipped")
                    skipped.set("message", result.message)

        # Pretty print XML
        rough_string = tostring(testsuites, "utf-8")
        reparsed = minidom.parseString(rough_string)
        return reparsed.toprettyxml(indent="  ")


@click.command()
@click.option(
    "--config",
    "-c",
    type=click.Path(exists=True),
    default="s3_config.yaml",
    help="Configuration file",
)
@click.option("--test", "-t", help="Run specific test by ID (e.g., 001)")
@click.option("--group", "-g", help="Run tests from specific group")
@click.option(
    "--output-dir",
    "-o",
    type=click.Path(),
    default="results",
    help="Output directory for results",
)
@click.option(
    "--output-format",
    "-f",
    type=click.Choice(["json", "yaml", "text", "junit"]),
    default="text",
    help="Output format",
)
@click.option("--verbose", "-v", is_flag=True, help="Verbose output")
@click.option("--list-tests", "-l", is_flag=True, help="List available tests")
@click.option("--count", is_flag=True, help="Count total number of tests")
@click.option(
    "--sdk",
    help="SDK to test against (e.g., 'boto3', 'aws-sdk-go-v2')",
)
@click.option(
    "--sdk-version",
    help="SDK version to test (e.g., '1.26.0', 'latest')",
)
@click.option(
    "--defconfig",
    type=click.Path(exists=True),
    help="Load SDK configuration from defconfig file",
)
def main(
    config,
    test,
    group,
    output_dir,
    output_format,
    verbose,
    list_tests,
    count,
    sdk,
    sdk_version,
    defconfig,
):
    """MSST-S3 Test Runner - Execute S3 interoperability tests"""

    # Load configuration
    config_path = Path(config)
    if config_path.exists():
        with open(config_path, "r") as f:
            if config_path.suffix in [".yaml", ".yml"]:
                test_config = yaml.safe_load(f)
            else:
                test_config = {}
    else:
        click.echo(
            f"Warning: Configuration file {config} not found, using defaults", err=True
        )
        test_config = {}

    # Load defconfig if specified (overrides config file SDK settings)
    if defconfig:
        defconfig_path = Path(defconfig)
        if defconfig_path.exists():
            with open(defconfig_path, "r") as f:
                if defconfig_path.suffix in [".yaml", ".yml"]:
                    defconfig_data = yaml.safe_load(f)
                    # Merge defconfig into test_config
                    test_config.update(defconfig_data)
                    if verbose:
                        click.echo(f"Loaded defconfig from {defconfig}")

    # Override SDK settings from CLI arguments
    if sdk:
        test_config["s3_sdk"] = sdk
    if sdk_version:
        test_config["s3_sdk_version"] = sdk_version

    # Generate SDK capability profile
    capabilities = None
    if SDK_CAPS_AVAILABLE:
        sdk_name = test_config.get("s3_sdk", "boto3")
        sdk_ver = test_config.get("s3_sdk_version", "latest")
        override_json = test_config.get("s3_cap_profile_json")
        force_override = test_config.get("s3_cap_profile_override", False)
        endpoint_hint = test_config.get("s3_endpoint_url")
        caps_output = test_config.get("s3_caps_json_path", ".sdk_capabilities.json")

        try:
            spec = SDKSpec(name=sdk_name, version=sdk_ver)
            capabilities = build_caps_document(
                spec=spec,
                endpoint_hint=endpoint_hint,
                override_json_path=override_json if force_override else None,
                force_override=force_override,
            )

            # Save capabilities to file
            caps_path = Path(caps_output)
            with open(caps_path, "w") as f:
                json.dump(capabilities, f, indent=2)

            if verbose:
                click.echo(f"Generated SDK capability profile for {sdk_name} {sdk_ver}")
                click.echo(f"Capabilities saved to {caps_path}")
                click.echo(f"Sources: {', '.join(capabilities.get('sources', []))}")

        except Exception as e:
            click.echo(f"Warning: Failed to generate SDK capabilities: {e}", err=True)
            if verbose:
                traceback.print_exc()
    else:
        if verbose:
            click.echo(
                "SDK capabilities module not available, using default capabilities"
            )

    # Setup test discovery
    test_dir = Path(__file__).parent.parent / "tests"
    discovery = TestDiscovery(test_dir)

    # Count tests if requested
    if count:
        all_tests = discovery.get_all_tests()
        total = len(all_tests)

        # Count by group
        groups_count = {}
        for test_info in all_tests:
            group_name = test_info["group"]
            groups_count[group_name] = groups_count.get(group_name, 0) + 1

        click.echo(f"Total tests: {total}")
        if verbose:
            click.echo("\nTests by category:")
            for group_name in sorted(groups_count.keys()):
                click.echo(f"  {group_name}: {groups_count[group_name]}")
        return

    # List tests if requested
    if list_tests:
        click.echo("Available tests:")
        for test_info in sorted(discovery.get_all_tests(), key=lambda t: t["id"]):
            click.echo(
                f"  {test_info['id']}: {test_info['name']} ({test_info['group']})"
            )
        return

    # Determine which tests to run
    tests_to_run = []
    if test:
        # Run specific test
        test_info = discovery.get_test_by_id(test)
        if test_info:
            tests_to_run = [test_info]
        else:
            click.echo(f"Error: Test {test} not found", err=True)
            sys.exit(1)
    elif group:
        # Run test group
        tests_to_run = discovery.get_tests_by_group(group)
        if not tests_to_run:
            click.echo(f"Error: No tests found in group {group}", err=True)
            sys.exit(1)
    else:
        # Run all enabled tests based on configuration
        enabled_groups = []
        if test_config.get("test_basic", True):
            enabled_groups.append("basic")
        if test_config.get("test_multipart", True):
            enabled_groups.append("multipart")
        if test_config.get("test_versioning", False):
            enabled_groups.append("versioning")
        if test_config.get("test_acl", False):
            enabled_groups.append("acl")
        if test_config.get("test_encryption", False):
            enabled_groups.append("encryption")
        if test_config.get("test_lifecycle", False):
            enabled_groups.append("lifecycle")
        if test_config.get("test_performance", False):
            enabled_groups.append("performance")
        if test_config.get("test_stress", False):
            enabled_groups.append("stress")
        if test_config.get("test_compatibility", False):
            enabled_groups.append("compatibility")

        for group_name in enabled_groups:
            tests_to_run.extend(discovery.get_tests_by_group(group_name))

    if not tests_to_run:
        click.echo("No tests to run", err=True)
        sys.exit(1)

    # Sort tests by ID
    tests_to_run = sorted(tests_to_run, key=lambda t: t["id"])

    # Check if parallel all mode is enabled
    parallel_all = test_config.get("test_parallel_all", False)
    run_mode = test_config.get("test_run_mode", "sequential")
    parallel_jobs = test_config.get("test_parallel_jobs", 4)

    if parallel_all:
        click.echo(f"Running {len(tests_to_run)} tests in PARALLEL ALL mode...")
        click.echo("WARNING: All tests will run simultaneously!")
        # When parallel_all is enabled, run all tests at once
        parallel_jobs = len(tests_to_run)
        run_mode = "parallel"
    else:
        click.echo(f"Running {len(tests_to_run)} tests...")

    # Execute tests
    executor = TestExecutor(test_config, capabilities)
    results = []

    if run_mode == "parallel" and parallel_jobs > 1:
        # Parallel execution
        with ThreadPoolExecutor(max_workers=parallel_jobs) as executor_pool:
            # Submit all test tasks
            future_to_test = {}
            for test_info in tests_to_run:
                if verbose:
                    click.echo(
                        f"Submitting test {test_info['id']}: {test_info['name']} ({test_info['group']})..."
                    )
                future = executor_pool.submit(executor.execute_test, test_info)
                future_to_test[future] = test_info

            # Collect results as they complete
            for future in as_completed(future_to_test):
                test_info = future_to_test[future]
                try:
                    result = future.result()
                    results.append(result)

                    # Show immediate feedback
                    status_char = {
                        TestStatus.PASSED: "✓",
                        TestStatus.FAILED: "✗",
                        TestStatus.SKIPPED: "○",
                        TestStatus.ERROR: "!",
                    }.get(result.status, "?")

                    if verbose or result.status != TestStatus.PASSED:
                        click.echo(
                            f"[{status_char}] Test {result.test_id}: {result.status.value}"
                        )
                        if result.message and result.status != TestStatus.PASSED:
                            click.echo(f"  {result.message}")
                except Exception as e:
                    click.echo(f"[!] Test {test_info['id']} failed with exception: {e}")
                    results.append(
                        TestResult(
                            test_id=test_info["id"],
                            test_name=test_info["name"],
                            test_group=test_info["group"],
                            status=TestStatus.ERROR,
                            duration=0.0,
                            message=f"Execution failed: {str(e)}",
                            error=traceback.format_exc(),
                            timestamp=datetime.now().isoformat(),
                        )
                    )
    else:
        # Sequential execution (original code)
        for test_info in tests_to_run:
            if verbose:
                click.echo(
                    f"Running test {test_info['id']}: {test_info['name']} ({test_info['group']})..."
                )

            result = executor.execute_test(test_info)
            results.append(result)

            # Show immediate feedback
            status_char = {
                TestStatus.PASSED: "✓",
                TestStatus.FAILED: "✗",
                TestStatus.SKIPPED: "○",
                TestStatus.ERROR: "!",
            }.get(result.status, "?")

            if verbose or result.status != TestStatus.PASSED:
                click.echo(
                    f"[{status_char}] Test {result.test_id}: {result.status.value}"
                )
                if result.message and result.status != TestStatus.PASSED:
                    click.echo(f"  {result.message}")

    # Format results
    formatter_map = {
        "json": ResultFormatter.format_json,
        "yaml": ResultFormatter.format_yaml,
        "text": ResultFormatter.format_text,
        "junit": ResultFormatter.format_junit,
    }

    formatter = formatter_map[output_format]
    formatted_results = formatter(results)

    # Save results
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    extension_map = {
        "json": ".json",
        "yaml": ".yaml",
        "text": ".txt",
        "junit": ".xml",
    }

    output_file = output_path / f"results{extension_map[output_format]}"
    with open(output_file, "w") as f:
        f.write(formatted_results)

    click.echo(f"\nResults saved to {output_file}")

    # Print summary
    if output_format != "text":
        click.echo("\n" + ResultFormatter.format_text(results))

    # Exit with appropriate code
    failed_count = len(
        [r for r in results if r.status in [TestStatus.FAILED, TestStatus.ERROR]]
    )
    sys.exit(1 if failed_count > 0 else 0)


if __name__ == "__main__":
    main()
