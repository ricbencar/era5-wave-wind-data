# -*- coding: utf-8 -*-
"""
Dependency checker for download_era5_data.py
===========================================

This script verifies whether the Python packages required by
'download_era5_data.py' are installed and reports their versions.
It distinguishes between:

- Core extraction dependencies (needed to read/process GRIB files)
- Optional download dependency (needed only for CDS API downloads)
- Standard-library modules used by the script

The current ERA5 workflow is based on:
    xarray + cfgrib + eccodes
and no longer on pygrib.
"""

from __future__ import annotations

import importlib
import importlib.metadata
import sys
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class Dependency:
    """Description of one dependency to be checked."""
    import_name: str
    package_name: Optional[str] = None
    install_name: Optional[str] = None
    category: str = "Python package"
    required_for: str = "General use"
    notes: Optional[str] = None


def get_version(package_name: str) -> str:
    """Return installed distribution version when available."""
    try:
        return importlib.metadata.version(package_name)
    except importlib.metadata.PackageNotFoundError:
        return "installed, version metadata not found"
    except Exception as exc:
        return f"installed, but version lookup failed ({exc})"


def check_dependency(dep: Dependency) -> bool:
    """Check whether one dependency can be imported."""
    package_name = dep.package_name or dep.import_name
    install_name = dep.install_name or package_name

    try:
        importlib.import_module(dep.import_name)
        version = get_version(package_name)
        print(f"✅ {dep.import_name:<20} | {version}")
        print(f"   Role: {dep.required_for}")
        if dep.notes:
            print(f"   Note: {dep.notes}")
        return True
    except ImportError:
        print(f"❌ {dep.import_name:<20} | NOT INSTALLED")
        print(f"   Role: {dep.required_for}")
        print(f"   Install: pip install {install_name}")
        if dep.notes:
            print(f"   Note: {dep.notes}")
        return False
    except Exception as exc:
        print(f"⚠️  {dep.import_name:<20} | CHECK FAILED ({exc})")
        print(f"   Role: {dep.required_for}")
        if dep.notes:
            print(f"   Note: {dep.notes}")
        return False


def print_header(title: str) -> None:
    print("\n" + "=" * 78)
    print(title)
    print("=" * 78)


def main() -> None:
    print_header("Checking dependencies for download_era5_data.py")
    print(
        "This script checks the packages used by the current ERA5 workflow based on\n"
        "xarray + cfgrib + eccodes, and reports installed versions when available."
    )

    core_dependencies = [
        Dependency(
            import_name="numpy",
            install_name="numpy",
            category="Core",
            required_for="Numerical operations and interpolation support",
        ),
        Dependency(
            import_name="pandas",
            install_name="pandas",
            category="Core",
            required_for="Datetime handling and CSV output",
        ),
        Dependency(
            import_name="xarray",
            install_name="xarray",
            category="Core",
            required_for="Reading and handling GRIB datasets through cfgrib",
        ),
        Dependency(
            import_name="cfgrib",
            install_name="cfgrib",
            category="Core",
            required_for="Opening GRIB files with xarray",
            notes="Requires ecCodes support to read GRIB files correctly.",
        ),
        Dependency(
            import_name="eccodes",
            install_name="eccodes",
            category="Core",
            required_for="Backend library used by cfgrib for GRIB decoding",
            notes="Depending on platform, you may need the ecCodes binaries/libraries available on the system as well.",
        ),
        Dependency(
            import_name="tqdm",
            install_name="tqdm",
            category="Core",
            required_for="Progress bars during download and extraction",
        ),
    ]

    optional_dependencies = [
        Dependency(
            import_name="cdsapi",
            install_name="cdsapi",
            category="Optional",
            required_for="Option 1 only: downloading ERA5 data from the CDS API",
            notes="Also requires a valid ~/.cdsapirc (or equivalent) CDS API configuration.",
        ),
    ]

    standard_library_dependencies = [
        Dependency(
            import_name="calendar",
            category="Standard library",
            required_for="Monthly day counting",
            notes="Included with Python.",
        ),
        Dependency(
            import_name="logging",
            category="Standard library",
            required_for="Execution logging",
            notes="Included with Python.",
        ),
        Dependency(
            import_name="multiprocessing",
            category="Standard library",
            required_for="Spawn start method / process management",
            notes="Included with Python.",
        ),
        Dependency(
            import_name="concurrent.futures",
            category="Standard library",
            required_for="Parallel GRIB processing in extract-only mode",
            notes="Included with Python.",
        ),
        Dependency(
            import_name="os",
            category="Standard library",
            required_for="Filesystem operations",
            notes="Included with Python.",
        ),
        Dependency(
            import_name="sys",
            category="Standard library",
            required_for="Program termination and interpreter interaction",
            notes="Included with Python.",
        ),
        Dependency(
            import_name="time",
            category="Standard library",
            required_for="Retries, delays and elapsed-time measurement",
            notes="Included with Python.",
        ),
        Dependency(
            import_name="importlib.metadata",
            package_name="importlib-metadata",
            category="Standard library",
            required_for="Version reporting in this checker",
            notes="Built into Python 3.8+; older Python versions may require importlib-metadata.",
        ),
    ]

    missing_core = 0
    missing_optional = 0
    missing_stdlib = 0

    print_header("Core dependencies (needed to process GRIB files)")
    for dep in core_dependencies:
        ok = check_dependency(dep)
        if not ok:
            missing_core += 1

    print_header("Optional dependency (needed only for CDS downloads)")
    for dep in optional_dependencies:
        ok = check_dependency(dep)
        if not ok:
            missing_optional += 1

    print_header("Standard library modules")
    for dep in standard_library_dependencies:
        ok = check_dependency(dep)
        if not ok:
            missing_stdlib += 1

    print_header("Summary")
    print(f"Core missing:      {missing_core}")
    print(f"Optional missing:  {missing_optional}")
    print(f"Stdlib missing:    {missing_stdlib}")

    if missing_core == 0:
        print("Result: GRIB extraction prerequisites appear to be available.")
    else:
        print("Result: one or more core packages required for GRIB extraction are missing.")

    if missing_optional == 0:
        print("CDS download support: available.")
    else:
        print("CDS download support: not fully available (Option 2 may still work).")

    print(
        "\nRecommended installation command for the current workflow:\n"
        "pip install numpy pandas xarray cfgrib eccodes tqdm cdsapi"
    )


if __name__ == "__main__":
    main()
