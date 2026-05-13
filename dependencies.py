#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Practical dependency checker for download_era5_data.py.

The current downloader uses the CDS API, reads CDS CSV/ZIP table payloads,
merges the ERA5 wave and wind tables with pandas, and computes wind speed and
meteorological wind direction with numpy.
"""

from __future__ import annotations

import importlib
import importlib.metadata
import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


SCRIPT_DIR = Path(__file__).resolve().parent
DOWNLOADER = SCRIPT_DIR / "download_era5_data.py"


@dataclass(frozen=True)
class PackageCheck:
    import_name: str
    pip_name: str
    required: bool = True
    note: str = ""


def _version(import_name: str, pip_name: str) -> str:
    for name in (pip_name, import_name):
        try:
            return importlib.metadata.version(name)
        except importlib.metadata.PackageNotFoundError:
            pass
        except Exception:
            pass
    return "installed"


def check_package(pkg: PackageCheck) -> bool:
    try:
        importlib.import_module(pkg.import_name)
        suffix = f" - {pkg.note}" if pkg.note else ""
        print(f"OK      {pkg.import_name:<12} {_version(pkg.import_name, pkg.pip_name)}{suffix}")
        return True
    except ImportError:
        suffix = f" - {pkg.note}" if pkg.note else ""
        print(f"MISSING {pkg.import_name:<12} install with: pip install {pkg.pip_name}{suffix}")
        return False
    except Exception as exc:
        print(f"ERROR   {pkg.import_name:<12} import check failed: {exc}")
        return False


def read_downloader_version() -> str:
    if not DOWNLOADER.exists():
        return "download_era5_data.py not found in this folder"
    try:
        text = DOWNLOADER.read_text(encoding="utf-8", errors="replace")
    except Exception as exc:
        return f"could not read download_era5_data.py: {exc}"

    match = re.search(r"^SCRIPT_VERSION\s*=\s*['\"]([^'\"]+)['\"]", text, flags=re.MULTILINE)
    if match:
        return match.group(1)
    return "SCRIPT_VERSION not found"


def check_cds_credentials() -> bool:
    cdsapirc = Path.home() / ".cdsapirc"
    has_file = cdsapirc.exists()
    has_env = bool(os.environ.get("CDSAPI_URL") and os.environ.get("CDSAPI_KEY"))

    if has_file:
        print(f"OK      CDS credentials found: {cdsapirc}")
        return True
    if has_env:
        print("OK      CDS credentials found in CDSAPI_URL / CDSAPI_KEY")
        return True

    print("MISSING CDS credentials: create %USERPROFILE%\\.cdsapirc or set CDSAPI_URL and CDSAPI_KEY")
    return False


def main() -> int:
    print("ERA5 dependency check")
    print(f"Python: {sys.version.split()[0]}")
    print(f"Folder: {SCRIPT_DIR}")
    print(f"Downloader version: {read_downloader_version()}")
    print()

    required = [
        PackageCheck("numpy", "numpy", True, "numeric calculations"),
        PackageCheck("pandas", "pandas", True, "CSV/table processing"),
        PackageCheck("cdsapi", "cdsapi", True, "CDS download"),
    ]

    optional = [
        PackageCheck("tkinter", "tkinter", False, "GUI mode; normally included with Python"),
        PackageCheck("openpyxl", "openpyxl", False, "only needed if a CDS ZIP contains .xlsx files"),
        PackageCheck("xlrd", "xlrd", False, "only needed if a CDS ZIP contains .xls files"),
    ]

    print("Required packages")
    required_missing = [pkg.pip_name for pkg in required if not check_package(pkg)]

    print("\nOptional checks")
    optional_missing = [pkg.pip_name for pkg in optional if not check_package(pkg)]

    print("\nCDS access")
    cds_ready = check_cds_credentials()

    print("\nInstall commands")
    if required_missing:
        print("Required missing packages:")
        print("  pip install " + " ".join(required_missing))
    else:
        print("Required packages are installed.")

    print("Full practical install command:")
    print("  pip install numpy pandas cdsapi openpyxl xlrd")

    print("\nSummary")
    print(f"Required packages missing: {len(required_missing)}")
    print(f"Optional packages missing: {len(optional_missing)}")
    print(f"CDS credentials: {'found' if cds_ready else 'not found'}")

    if required_missing or not cds_ready:
        print("Result: not ready for CDS download.")
        return 1

    print("Result: ready for CDS download.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
