# -*- coding: utf-8 -*-
"""
Dependency checker for download_era5_data.py
===========================================

This script verifies whether the Python modules required by
'download_era5_data.py' are available and reports their versions when that
information can be retrieved.

It distinguishes between:

- Core extraction dependencies (needed to read/process GRIB files)
- Mode-specific dependencies (needed only for CDS downloads or GUI mode)
- Standard-library modules used by the script

The current ERA5 workflow is based on:
    xarray + cfgrib + eccodes
and no longer on pygrib.
"""

from __future__ import annotations

import importlib
import importlib.metadata
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


def get_version(dep: Dependency) -> str:
    """Return an installed version string when available."""
    if dep.category.lower().startswith("standard"):
        return "included with Python"

    package_name = dep.package_name or dep.import_name
    try:
        return importlib.metadata.version(package_name)
    except importlib.metadata.PackageNotFoundError:
        return "installed, version metadata not found"
    except Exception as exc:
        return f"installed, but version lookup failed ({exc})"


def check_dependency(dep: Dependency) -> bool:
    """Check whether one dependency can be imported."""
    install_name = dep.install_name or dep.package_name or dep.import_name

    try:
        importlib.import_module(dep.import_name)
        version = get_version(dep)
        print(f"✅ {dep.import_name:<20} | {version}")
        print(f"   Role: {dep.required_for}")
        if dep.notes:
            print(f"   Note: {dep.notes}")
        return True
    except ImportError:
        print(f"❌ {dep.import_name:<20} | NOT INSTALLED")
        print(f"   Role: {dep.required_for}")
        if install_name:
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
        "This checker follows the current downloader/extractor implementation and\n"
        "reports whether the modules required by each operating mode are available."
    )

    core_dependencies = [
        Dependency(
            import_name="numpy",
            install_name="numpy",
            category="Core",
            required_for="Numerical operations and inverse-distance interpolation",
        ),
        Dependency(
            import_name="pandas",
            install_name="pandas",
            category="Core",
            required_for="Datetime handling, tabular processing, and CSV output",
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
            notes="Depending on platform, you may also need ecCodes system binaries/libraries available.",
        ),
    ]

    mode_specific_dependencies = [
        Dependency(
            import_name="cdsapi",
            install_name="cdsapi",
            category="Mode-specific",
            required_for="Download mode only: retrieving ERA5 data from the CDS API",
            notes="Also requires a valid ~/.cdsapirc (or equivalent) CDS API configuration.",
        ),
        Dependency(
            import_name="tkinter",
            install_name=None,
            category="Mode-specific",
            required_for="GUI mode only: starting the graphical interface",
            notes="Usually bundled with Python on Windows. On some Linux distributions it must be installed separately via the system package manager.",
        ),
    ]

    standard_library_dependencies = [
        Dependency(
            import_name="argparse",
            category="Standard library",
            required_for="Command-line argument parsing",
        ),
        Dependency(
            import_name="calendar",
            category="Standard library",
            required_for="Monthly day counting",
        ),
        Dependency(
            import_name="logging",
            category="Standard library",
            required_for="Execution logging",
        ),
        Dependency(
            import_name="math",
            category="Standard library",
            required_for="Grid and interpolation calculations",
        ),
        Dependency(
            import_name="multiprocessing",
            category="Standard library",
            required_for="Spawn start method / process management",
        ),
        Dependency(
            import_name="queue",
            category="Standard library",
            required_for="Thread-safe GUI message passing",
        ),
        Dependency(
            import_name="threading",
            category="Standard library",
            required_for="Background execution in GUI mode",
        ),
        Dependency(
            import_name="time",
            category="Standard library",
            required_for="Retries, delays, and elapsed-time measurement",
        ),
        Dependency(
            import_name="concurrent.futures",
            category="Standard library",
            required_for="Parallel GRIB processing in extract-only mode",
        ),
        Dependency(
            import_name="dataclasses",
            category="Standard library",
            required_for="Structured configuration/data containers",
        ),
        Dependency(
            import_name="datetime",
            category="Standard library",
            required_for="Timestamp handling",
        ),
        Dependency(
            import_name="os",
            category="Standard library",
            required_for="Filesystem operations",
        ),
        Dependency(
            import_name="pathlib",
            category="Standard library",
            required_for="Portable path handling",
        ),
        Dependency(
            import_name="sys",
            category="Standard library",
            required_for="Program termination and interpreter interaction",
        ),
        Dependency(
            import_name="typing",
            category="Standard library",
            required_for="Type annotations",
        ),
    ]

    missing_core = 0
    missing_mode_specific = 0
    missing_stdlib = 0

    print_header("Core dependencies (needed to process GRIB files)")
    for dep in core_dependencies:
        ok = check_dependency(dep)
        if not ok:
            missing_core += 1

    print_header("Mode-specific dependencies")
    for dep in mode_specific_dependencies:
        ok = check_dependency(dep)
        if not ok:
            missing_mode_specific += 1

    print_header("Standard library modules used by the script")
    for dep in standard_library_dependencies:
        ok = check_dependency(dep)
        if not ok:
            missing_stdlib += 1

    print_header("Summary")
    print(f"Core missing:            {missing_core}")
    print(f"Mode-specific missing:   {missing_mode_specific}")
    print(f"Stdlib missing:          {missing_stdlib}")

    if missing_core == 0:
        print("Result: GRIB extraction prerequisites appear to be available.")
    else:
        print("Result: one or more core packages required for GRIB extraction are missing.")

    print(
        "\nMode-specific notes:\n"
        "- Download mode additionally needs cdsapi and valid CDS credentials.\n"
        "- GUI mode additionally needs tkinter.\n"
        "- Extract-only CLI runs do not require cdsapi or tkinter."
    )

    print(
        "\nRecommended installation command for the main Python packages used by the current workflow:\n"
        "pip install numpy pandas xarray cfgrib eccodes cdsapi"
    )
    print(
        "\nNote: tkinter is usually not installed with pip. If GUI mode fails, install/enable tkinter\n"
        "in the Python distribution or operating system package set you are using."
    )


if __name__ == "__main__":
    main()
