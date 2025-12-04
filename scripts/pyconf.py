#!/usr/bin/env python3
# SPDX-License-Identifier: MIT
"""
Python-based Kconfig configuration tool using kconfiglib.
Replacement for the C-based 'conf' tool.
"""

import sys
import os
import argparse

try:
    import kconfiglib
except ImportError:
    print(
        "Error: kconfiglib not found. Install with: pip install kconfiglib",
        file=sys.stderr,
    )
    sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description="Kconfig configuration tool")
    parser.add_argument("kconfig_file", help="Path to Kconfig file")
    parser.add_argument(
        "--oldconfig",
        action="store_true",
        help="Update config with defaults for new options",
    )
    parser.add_argument(
        "--olddefconfig",
        action="store_true",
        help="Like oldconfig but sets new options to default",
    )
    parser.add_argument(
        "--savedefconfig", metavar="FILE", help="Save minimal defconfig to FILE"
    )
    parser.add_argument(
        "--config",
        default=".config",
        help="Config file to load/save (default: .config)",
    )

    args = parser.parse_args()

    # Set environment variables for kconfiglib
    os.environ.setdefault("CONFIG_", "CONFIG_")
    os.environ.setdefault("KCONFIG_CONFIG", args.config)

    # Load Kconfig
    kconf = kconfiglib.Kconfig(args.kconfig_file)

    # Load existing config if it exists
    if os.path.exists(args.config):
        kconf.load_config(args.config)

    if args.oldconfig or args.olddefconfig:
        # For olddefconfig, just use defaults for new symbols
        # kconfiglib automatically handles this when loading
        kconf.write_config(args.config)
        print(f"Configuration updated: {args.config}")

    elif args.savedefconfig:
        # Save minimal defconfig
        kconf.write_min_config(args.savedefconfig)
        print(f"Minimal defconfig saved: {args.savedefconfig}")

    else:
        # Default: interactive config (but we don't implement this here)
        print("Use --oldconfig, --olddefconfig, or --savedefconfig", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
