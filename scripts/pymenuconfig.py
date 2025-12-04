#!/usr/bin/env python3
# SPDX-License-Identifier: MIT
"""
Python-based menuconfig interface using kconfiglib.
Replacement for the C-based 'mconf' tool.
"""

import sys
import os

try:
    import kconfiglib
    import menuconfig
except ImportError:
    print(
        "Error: kconfiglib not found. Install with: pip install kconfiglib",
        file=sys.stderr,
    )
    sys.exit(1)


def main():
    if len(sys.argv) < 2:
        print("Usage: pymenuconfig.py <Kconfig file>", file=sys.stderr)
        sys.exit(1)

    kconfig_file = sys.argv[1]
    config_file = os.environ.get("KCONFIG_CONFIG", ".config")

    # Set environment variables for kconfiglib
    os.environ.setdefault("CONFIG_", "CONFIG_")
    os.environ["KCONFIG_CONFIG"] = config_file

    # Load Kconfig
    kconf = kconfiglib.Kconfig(kconfig_file)

    # Load existing config if it exists
    if os.path.exists(config_file):
        kconf.load_config(config_file)

    # Run menuconfig interface
    menuconfig.menuconfig(kconf)


if __name__ == "__main__":
    main()
