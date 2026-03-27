#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ERA5 Hourly Data Downloader and Extractor
=========================================

Production GUI application for downloading and extracting hourly ERA5
single-level data at a user-defined target coordinate.

Main capabilities
-----------------
- GUI-first workflow with a clean tabbed interface.
- Download mode for monthly ERA5 retrievals through the CDS API.
- Extract-only mode for processing existing local GRIB files.
- ERA5-grid-aligned spatial request window around the target coordinate.
- Inverse-distance weighting (IDW) interpolation to the exact target point.
- Background execution, integrated progress reporting, and detailed logging.
- Optional CLI arguments for scripted runs and automation.
"""

from __future__ import annotations

import argparse
import calendar
import importlib
import logging
import math
import multiprocessing
import os
import queue
import sys
import threading
import time
from concurrent.futures import ProcessPoolExecutor, TimeoutError, as_completed
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple

try:
    import numpy as np
except Exception:
    np = None

try:
    import pandas as pd
except Exception:
    pd = None

try:
    import cdsapi
except Exception:
    cdsapi = None

try:
    import tkinter as tk
    from tkinter import filedialog, messagebox, scrolledtext, ttk
except Exception:
    tk = None
    filedialog = None
    messagebox = None
    scrolledtext = None
    ttk = None


SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_DATA_DIR = SCRIPT_DIR / "grib"
DEFAULT_RESULTS_DIR = SCRIPT_DIR / "results"
DEFAULT_OUTPUT_CSV_NAME = "download_era5_data.csv"
DEFAULT_LOG_FILE = SCRIPT_DIR / "download_era5_data.log"
DEFAULT_DEFAULTS_FILE = SCRIPT_DIR / "defaults.txt"
DEFAULT_GRID_STEP = 0.25
DEFAULT_REQUEST_DELAY = 60
DEFAULT_MAX_RETRIES = 3
DEFAULT_IDW_POWER = 2
DEFAULT_TIMEOUT_PER_FILE = 180
DEFAULT_WINDOW_WIDTH = 980
DEFAULT_WINDOW_HEIGHT = 760
DEFAULT_WINDOW_MIN_WIDTH = 900
DEFAULT_WINDOW_MIN_HEIGHT = 680
DEFAULT_COORD_ENTRY_WIDTH = 20
DEFAULT_YEAR_ENTRY_WIDTH = 12
DEFAULT_PATH_ENTRY_WIDTH = 52
GRIB_EXTENSIONS = (".grib", ".grib2", ".grb", ".grb2")

VARIABLES = {
    "swh": {
        "request_code": "229.140",
        "aliases": {"swh"},
        "parameter_numbers": {"229"},
        "parameter_ids": {"140229"},
    },
    "mwd": {
        "request_code": "230.140",
        "aliases": {"mwd"},
        "parameter_numbers": {"230"},
        "parameter_ids": {"140230"},
    },
    "pp1d": {
        "request_code": "231.140",
        "aliases": {"pp1d"},
        "parameter_numbers": {"231"},
        "parameter_ids": {"140231"},
    },
    "wind": {
        "request_code": "245.140",
        "aliases": {"wind"},
        "parameter_numbers": {"245"},
        "parameter_ids": {"140245"},
    },
    "dwi": {
        "request_code": "249.140",
        "aliases": {"dwi"},
        "parameter_numbers": {"249"},
        "parameter_ids": {"140249"},
    },
}

INSTRUCTIONS_TEXT = """
ERA5 Downloader / Extractor
===========================

Purpose
-------
This application downloads and/or extracts hourly ERA5 single-level data for a
single target coordinate and writes the final merged time series to CSV.

The program is intended for production use and supports both interactive GUI
operation and command-line execution.

What the application does
-------------------------
- Retrieves monthly ERA5 GRIB files from the Copernicus Climate Data Store.
- Processes GRIB files already stored locally when download is not required.
- Extracts the configured variables from each file.
- Interpolates the ERA5 values to the exact target location using inverse
  distance weighting (IDW).
- Merges all processed timestamps into a single CSV sorted by datetime.
- Writes detailed progress information to the Log tab and to the log file.

Available modes
---------------
1) Download and process
   Use this when you want the application to contact CDS, download monthly
   GRIB files, and process them in the same run.

   Typical workflow:
   - validate the requested configuration;
   - initialize the CDS API client;
   - download one monthly GRIB file per request;
   - process each downloaded file immediately;
   - merge results into the selected CSV output.

2) Extract existing GRIB files only
   Use this when the GRIB files have already been downloaded and are available
   in the selected GRIB folder.

   Typical workflow:
   - scan the selected GRIB folder for supported GRIB file extensions;
   - open and process the files in parallel;
   - merge extracted data into a fresh CSV output;
   - sort the final result by datetime.

Target coordinate entry
-----------------------
The coordinate fields are ordered as:
- Longitude
- Latitude

Enter coordinates in decimal degrees.
Examples:
- Longitude west of Greenwich is negative, e.g. -9.58166667
- Latitude north of the Equator is positive, e.g. 41.14833299

Take care to place longitude in the first box and latitude in the second box.
Reversing them will move the target location to a different place and will lead
to incorrect results. The application stores the last used form values in
defaults.txt and restores them automatically the next time it starts.

Time range
----------
- Start year and End year are used in Download and process mode.
- In Extract existing GRIB files only mode, the application processes the GRIB
  files already present in the selected folder regardless of the year range.

Folders and files
-----------------
GRIB folder
  Folder where downloaded GRIB files are stored and where Extract mode looks
  for existing input files.

Results folder
  Folder where the final CSV file is written.

Output CSV name
  Name of the merged CSV file produced by the run.

Log file
  Path of the execution log written by the application.

Variables extracted
-------------------
The current configuration extracts these ERA5 variables:
- swh  : Significant height of combined wind waves and swell
- mwd  : Mean wave direction
- pp1d : Peak wave period
- wind : 10 metre wind speed
- dwi  : 10 metre wind direction

Spatial extraction method
-------------------------
The request window is aligned to the ERA5 0.25° grid around the target
coordinate. The application then interpolates the value at the exact target
point using inverse-distance weighting (IDW).

This approach preserves a clean, stable extraction workflow while avoiding the
need to treat the target point as if it were located exactly on a model node.

How to use the GUI
------------------
1. Select the desired mode.
2. Enter Longitude first and Latitude second.
3. Enter the start and end years if Download mode is being used.
4. Confirm or change the GRIB folder, Results folder, output CSV name, and log
   file path.
5. Review the request summary shown in the Run tab.
6. Click Start.
7. Monitor execution in the Progress panel and the Log tab.
8. When the run finishes, open the CSV file from the selected results folder.

Status, progress, and logging
-----------------------------
- The Progress panel shows the current stage of the run.
- The Log tab records detailed execution messages.
- The log file stores the same operational information on disk.
- On success, the application reports the full output CSV path.
- On failure, the error message is reported in the status bar and log.

Dependencies
------------
Required Python packages:
- numpy
- pandas
- xarray
- cfgrib
- eccodes
- cdsapi   (required only for Download and process mode)

Typical installation:
    pip install numpy pandas xarray cfgrib eccodes cdsapi

CDS API credentials
-------------------
Download mode requires a valid CDS account and a working CDS API setup.
The usual setup includes a .cdsapirc file in the user home directory.

Typical content:
    url: https://cds.climate.copernicus.eu/api/v2
    key: <YOUR_UID>:<YOUR_API_KEY>

Replace the placeholders with your real CDS credentials.

Command-line usage
------------------
The script starts in GUI mode by default.

Examples:
    python download_era5_data.py
    python download_era5_data.py --gui
    python download_era5_data.py --download --longitude -9.58166667 --latitude 41.14833299
    python download_era5_data.py --extract --data-dir grib --results-dir results

Operational notes
-----------------
- The working directory is set to the folder that contains the script.
- Missing folders are created automatically when needed.
- Extract mode rebuilds the output CSV from the GRIB files found in the input
  folder.
- Final CSV output is sorted by datetime in ascending order.
- Some GRIB files may contain heterogeneous internal groups; the extractor is
  designed to handle that structure.

Troubleshooting
---------------
If Download mode fails:
- confirm that cdsapi is installed;
- confirm that CDS credentials are valid;
- confirm that network access to CDS is available.

If Extract mode fails:
- confirm that the GRIB folder contains supported GRIB files;
- confirm that xarray, cfgrib, and eccodes are installed correctly;
- inspect the Log tab and log file for the failing file name and error message.

If the GUI does not start:
- confirm that tkinter is available in the Python installation being used.
""".strip()


def format_target_point(longitude: float, latitude: float) -> str:
    return f"lon={longitude:.8f}, lat={latitude:.8f}"


def format_lon_lat_pair(longitude: float, latitude: float, decimals: int = 5) -> str:
    return f"({longitude:.{decimals}f}, {latitude:.{decimals}f})"



HARDCODED_DEFAULTS = {
    "mode": "download",
    "longitude": "-9.58166667",
    "latitude": "41.14833299",
    "start_year": "1940",
    "end_year": "2026",
    "data_dir": str(DEFAULT_DATA_DIR),
    "results_dir": str(DEFAULT_RESULTS_DIR),
    "output_csv_name": DEFAULT_OUTPUT_CSV_NAME,
    "log_file": str(DEFAULT_LOG_FILE),
}


def _parse_defaults_file(text: str) -> Dict[str, str]:
    parsed: Dict[str, str] = {}
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if key:
            parsed[key] = value
    return parsed


def load_saved_defaults(file_path: Path = DEFAULT_DEFAULTS_FILE) -> Dict[str, str]:
    defaults = dict(HARDCODED_DEFAULTS)
    try:
        if not file_path.exists():
            return defaults
        parsed = _parse_defaults_file(file_path.read_text(encoding="utf-8"))
        for key in defaults:
            if key in parsed and parsed[key]:
                defaults[key] = parsed[key]
        return defaults
    except Exception:
        return defaults


def save_defaults(values: Dict[str, str], file_path: Path = DEFAULT_DEFAULTS_FILE) -> None:
    merged = dict(HARDCODED_DEFAULTS)
    for key in merged:
        if key in values and values[key] is not None:
            merged[key] = str(values[key]).strip()
    file_path.parent.mkdir(parents=True, exist_ok=True)
    ordered_keys = [
        "mode",
        "longitude",
        "latitude",
        "start_year",
        "end_year",
        "data_dir",
        "results_dir",
        "output_csv_name",
        "log_file",
    ]
    lines = [f"{key}={merged[key]}" for key in ordered_keys]
    file_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _safe_float(value: str, fallback: float) -> float:
    try:
        return float(str(value).strip())
    except Exception:
        return fallback


def _safe_int(value: str, fallback: int) -> int:
    try:
        return int(str(value).strip())
    except Exception:
        return fallback


SAVED_DEFAULTS = load_saved_defaults()
INITIAL_MODE = SAVED_DEFAULTS["mode"] if SAVED_DEFAULTS.get("mode") in {"download", "extract"} else HARDCODED_DEFAULTS["mode"]
INITIAL_LONGITUDE = _safe_float(SAVED_DEFAULTS.get("longitude", HARDCODED_DEFAULTS["longitude"]), float(HARDCODED_DEFAULTS["longitude"]))
INITIAL_LATITUDE = _safe_float(SAVED_DEFAULTS.get("latitude", HARDCODED_DEFAULTS["latitude"]), float(HARDCODED_DEFAULTS["latitude"]))
INITIAL_START_YEAR = _safe_int(SAVED_DEFAULTS.get("start_year", HARDCODED_DEFAULTS["start_year"]), int(HARDCODED_DEFAULTS["start_year"]))
INITIAL_END_YEAR = _safe_int(SAVED_DEFAULTS.get("end_year", HARDCODED_DEFAULTS["end_year"]), int(HARDCODED_DEFAULTS["end_year"]))
INITIAL_DATA_DIR = SAVED_DEFAULTS.get("data_dir", HARDCODED_DEFAULTS["data_dir"]) or HARDCODED_DEFAULTS["data_dir"]
INITIAL_RESULTS_DIR = SAVED_DEFAULTS.get("results_dir", HARDCODED_DEFAULTS["results_dir"]) or HARDCODED_DEFAULTS["results_dir"]
INITIAL_OUTPUT_CSV_NAME = SAVED_DEFAULTS.get("output_csv_name", HARDCODED_DEFAULTS["output_csv_name"]) or HARDCODED_DEFAULTS["output_csv_name"]
INITIAL_LOG_FILE = SAVED_DEFAULTS.get("log_file", HARDCODED_DEFAULTS["log_file"]) or HARDCODED_DEFAULTS["log_file"]


@dataclass
class Era5Config:
    longitude: float = INITIAL_LONGITUDE
    latitude: float = INITIAL_LATITUDE
    start_year: int = INITIAL_START_YEAR
    end_year: int = INITIAL_END_YEAR
    data_dir: Path = field(default_factory=lambda: Path(INITIAL_DATA_DIR).expanduser())
    results_dir: Path = field(default_factory=lambda: Path(INITIAL_RESULTS_DIR).expanduser())
    output_csv_name: str = INITIAL_OUTPUT_CSV_NAME
    log_file: Path = field(default_factory=lambda: Path(INITIAL_LOG_FILE).expanduser())
    grid_step: float = DEFAULT_GRID_STEP
    request_delay: int = DEFAULT_REQUEST_DELAY
    max_retries: int = DEFAULT_MAX_RETRIES
    idw_power: int = DEFAULT_IDW_POWER
    timeout_per_file: int = DEFAULT_TIMEOUT_PER_FILE

    @property
    def years(self) -> List[int]:
        return list(range(self.start_year, self.end_year + 1))

    @property
    def output_csv(self) -> Path:
        return self.results_dir / self.output_csv_name

    @property
    def area(self) -> List[float]:
        north, west, south, east = compute_surrounding_four_point_area(
            target_lat=self.latitude,
            target_lon=self.longitude,
            grid_step=self.grid_step,
        )
        return [north, west, south, east]

    @property
    def grid(self) -> List[float]:
        return [self.grid_step, self.grid_step]

    @property
    def surrounding_points(self) -> List[Tuple[float, float]]:
        north, west, south, east = self.area
        return [
            (north, west),
            (north, east),
            (south, west),
            (south, east),
        ]


def compute_surrounding_four_point_area(target_lat: float, target_lon: float, grid_step: float = 0.25) -> Tuple[float, float, float, float]:
    """
    Return an ERA5 request window that encloses the target with exactly 2x2 grid
    nodes for a regular grid of size `grid_step`.

    The returned tuple is (north, west, south, east).
    """
    if grid_step <= 0:
        raise ValueError("grid_step must be > 0")

    lat_scaled = target_lat / grid_step
    lon_scaled = target_lon / grid_step

    south = math.floor(lat_scaled) * grid_step
    north = math.ceil(lat_scaled) * grid_step
    west = math.floor(lon_scaled) * grid_step
    east = math.ceil(lon_scaled) * grid_step

    # If the target lies exactly on a grid line, widen by one grid cell so the
    # request still contains two nodes in that direction.
    if math.isclose(north, south, rel_tol=0.0, abs_tol=1e-12):
        north = south + grid_step
    if math.isclose(east, west, rel_tol=0.0, abs_tol=1e-12):
        east = west + grid_step

    return (
        round(north, 10),
        round(west, 10),
        round(south, 10),
        round(east, 10),
    )


def setup_logging(log_file: Path) -> None:
    """Configure file logging for the current process."""
    log_file.parent.mkdir(parents=True, exist_ok=True)
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    existing_targets = {
        getattr(handler, "baseFilename", None) for handler in root_logger.handlers
    }
    if str(log_file) not in existing_targets:
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
        root_logger.addHandler(file_handler)


def _dependency_check_line(module_name: str, install_name: Optional[str], purpose: str) -> Optional[str]:
    try:
        importlib.import_module(module_name)
        return None
    except Exception as exc:
        install_hint = f" Install with: pip install {install_name or module_name}."
        return f"Missing required dependency '{module_name}' for {purpose}. Import error: {exc}.{install_hint}"


def validate_runtime_dependencies(mode: str, wants_gui: bool, reporter: Optional["ProgressReporter"] = None) -> None:
    missing_messages: List[str] = []

    if np is None:
        missing_messages.append(
            "Missing required dependency 'numpy' for numerical operations and interpolation. "
            "Install with: pip install numpy."
        )
    if pd is None:
        missing_messages.append(
            "Missing required dependency 'pandas' for datetime handling and CSV output. "
            "Install with: pip install pandas."
        )

    for module_name, install_name, purpose in [
        ("xarray", "xarray", "GRIB dataset handling"),
        ("cfgrib", "cfgrib", "GRIB file reading"),
        ("eccodes", "eccodes", "GRIB decoding backend used by cfgrib"),
    ]:
        message = _dependency_check_line(module_name, install_name, purpose)
        if message is not None:
            missing_messages.append(message)

    if mode == "download" and cdsapi is None:
        missing_messages.append(
            "Missing required dependency 'cdsapi' for CDS downloads. Install with: pip install cdsapi."
        )

    if wants_gui and tk is None:
        missing_messages.append(
            "Missing required dependency 'tkinter' for GUI mode. Install/enable tkinter in this Python distribution."
        )

    if missing_messages:
        header = "Runtime dependency check failed:"
        logging.error(header)
        if reporter is not None:
            reporter.log(header)
        for message in missing_messages:
            logging.error(message)
            if reporter is not None:
                reporter.log(message)
        raise RuntimeError("One or more required dependencies are not installed. See log file for details.")


@dataclass
class GribProcessResult:
    file_path: str
    dataframe: Optional[pd.DataFrame] = None
    open_ok: bool = False
    dataset_count: int = 0
    dataset_without_variables_count: int = 0
    inspected_variable_count: int = 0
    recognized_variable_count: int = 0
    skipped_variable_count: int = 0
    variable_error_count: int = 0
    extracted_timestamp_count: int = 0
    open_error: Optional[str] = None

    @property
    def file_name(self) -> str:
        return Path(self.file_path).name

    @property
    def has_data(self) -> bool:
        return self.dataframe is not None and not self.dataframe.empty and self.extracted_timestamp_count > 0

    def summary_message(self, include_download_context: bool = False) -> str:
        prefix = "Download/lookup completed. " if include_download_context else ""
        if not self.open_ok:
            detail = self.open_error or "cfgrib could not open the file."
            return f"{prefix}Extraction failed for {self.file_name}: unable to open GRIB with cfgrib ({detail})."

        if self.has_data:
            return (
                f"{prefix}Extraction succeeded for {self.file_name}: "
                f"{self.extracted_timestamp_count} timestamps written "
                f"from {self.recognized_variable_count} recognized variable occurrence(s)."
            )

        reasons = []
        if self.recognized_variable_count == 0:
            reasons.append("no GRIB variable matched the configured ERA5 variable map")
        if self.variable_error_count > 0:
            reasons.append(f"{self.variable_error_count} recognized variable(s) failed during interpolation/extraction")
        if self.dataset_without_variables_count > 0:
            reasons.append(f"{self.dataset_without_variables_count} dataset group(s) contained no data variables")
        if not reasons:
            reasons.append("the file opened but produced no usable rows")

        return (
            f"{prefix}Extraction produced no usable rows for {self.file_name}: "
            + "; ".join(reasons)
            + "."
        )


def _report_process_result(result: GribProcessResult, reporter: ProgressReporter, include_download_context: bool = False) -> None:
    reporter.log(result.summary_message(include_download_context=include_download_context))

class ProgressReporter:
    def __init__(self, callback: Optional[Callable[[str, Dict], None]] = None):
        self.callback = callback

    def emit(self, event: str, **payload: Dict) -> None:
        if self.callback is not None:
            self.callback(event, payload)

    def log(self, message: str) -> None:
        logging.info(message)
        self.emit("log", message=message)

    def status(self, message: str) -> None:
        self.emit("status", message=message)

    def progress(self, current: int, total: int, message: str = "") -> None:
        self.emit("progress", current=current, total=total, message=message)


def initialize_cds_client() -> "cdsapi.Client":
    if cdsapi is None:
        raise RuntimeError("cdsapi is not installed. Install it to use download mode.")
    return cdsapi.Client()


def download_monthly_data(
    client: "cdsapi.Client",
    year: int,
    month: int,
    variable_list: List[str],
    area: List[float],
    grid: List[float],
    output_dir: Path,
    request_delay: int,
    max_retries: int,
    reporter: Optional[ProgressReporter] = None,
) -> Optional[Path]:
    file_name = f"ERA5_{year}_{month:02d}.grib"
    file_path = output_dir / file_name

    if file_path.exists():
        if reporter:
            reporter.log(f"Monthly GRIB already exists for {year}-{month:02d}. Reusing local file {file_name}.")
        return file_path

    days_in_month = calendar.monthrange(year, month)[1]
    days = [f"{day:02d}" for day in range(1, days_in_month + 1)]

    for attempt in range(1, max_retries + 1):
        try:
            if reporter:
                reporter.log(
                    f"Downloading {year}-{month:02d} (attempt {attempt}/{max_retries}) using an ERA5-aligned request window."
                )
            client.retrieve(
                "reanalysis-era5-single-levels",
                {
                    "product_type": "reanalysis",
                    "format": "grib",
                    "variable": variable_list,
                    "year": str(year),
                    "month": f"{month:02d}",
                    "day": days,
                    "time": [f"{hour:02d}:00" for hour in range(24)],
                    "area": area,
                    "grid": grid,
                },
                str(file_path),
            )
            if reporter:
                reporter.log(f"Download completed for {year}-{month:02d}: {file_name}")
            return file_path
        except Exception as exc:
            logging.warning("Attempt %d failed for %s-%02d. Error: %s", attempt, year, month, exc)
            if reporter:
                reporter.log(f"Download attempt {attempt} failed for {year}-{month:02d}: {exc}")
            if attempt < max_retries:
                wait_time = request_delay * attempt
                if reporter:
                    reporter.log(f"Waiting {wait_time} s before retrying {year}-{month:02d}.")
                time.sleep(wait_time)
            else:
                if reporter:
                    reporter.log(f"Download failed for {year}-{month:02d} after {max_retries} attempt(s).")
                return None
    return None


def _choose_coord_name(obj, *names: str) -> Optional[str]:
    for name in names:
        if name in getattr(obj, "coords", {}):
            return name
    for name in names:
        if name in getattr(obj, "dims", {}):
            return name
    return None


def _normalize_target_longitude(dataset_or_array, target_lon: float) -> float:
    lon_name = _choose_coord_name(dataset_or_array, "longitude", "lon")
    if lon_name is None:
        return target_lon

    try:
        lon_values = np.asarray(dataset_or_array[lon_name].values, dtype=float)
    except Exception:
        return target_lon

    if lon_values.size == 0:
        return target_lon

    lon_min = np.nanmin(lon_values)
    lon_max = np.nanmax(lon_values)

    if lon_min >= 0.0 and lon_max > 180.0 and target_lon < 0.0:
        return target_lon % 360.0
    if lon_max <= 180.0 and target_lon > 180.0:
        return ((target_lon + 180.0) % 360.0) - 180.0
    return target_lon


def _identify_variable_key(data_var_name: str, data_array) -> Optional[str]:
    attrs = getattr(data_array, "attrs", {}) or {}
    candidate_strings = set()

    def _add(value: object) -> None:
        if value is None:
            return
        text = str(value).strip().lower()
        if text:
            candidate_strings.add(text)

    _add(data_var_name)
    for attr_name in (
        "GRIB_shortName", "shortName",
        "GRIB_cfVarName", "cfVarName",
        "GRIB_name", "long_name", "standard_name",
        "GRIB_parameterNumber", "parameterNumber",
        "GRIB_paramId", "paramId",
    ):
        _add(attrs.get(attr_name))

    param_number = attrs.get("GRIB_parameterNumber", attrs.get("parameterNumber"))
    if param_number is not None:
        try:
            param_number_int = int(param_number)
            candidate_strings.add(str(param_number_int))
            candidate_strings.add(f"{param_number_int}.140")
        except Exception:
            pass

    param_id = attrs.get("GRIB_paramId", attrs.get("paramId"))
    if param_id is not None:
        try:
            candidate_strings.add(str(int(param_id)))
        except Exception:
            candidate_strings.add(str(param_id).strip().lower())

    for output_key, spec in VARIABLES.items():
        accepted = set(spec["aliases"])
        accepted.add(spec["request_code"])
        accepted.update(spec["parameter_numbers"])
        accepted.update(spec["parameter_ids"])
        if candidate_strings & {item.lower() for item in accepted}:
            return output_key

    return None


def _reduce_dataarray_to_time_lat_lon(data_array):
    keep_dim_names = {"time", "valid_time", "latitude", "longitude", "lat", "lon", "y", "x"}
    da = data_array.squeeze(drop=True)

    extra_non_singleton_dims = [
        dim for dim in da.dims
        if dim not in keep_dim_names and da.sizes.get(dim, 1) > 1
    ]
    for dim in extra_non_singleton_dims:
        logging.info(
            "Unexpected non-singleton dimension '%s' in variable '%s'. Selecting first element.",
            dim,
            getattr(da, "name", "unknown"),
        )
        da = da.isel({dim: 0})

    return da.squeeze(drop=True)


def _extract_time_values(data_array, dataset=None) -> List[pd.Timestamp]:
    if "valid_time" in data_array.coords:
        values = np.atleast_1d(data_array["valid_time"].values)
        return pd.to_datetime(values, errors="coerce", format="mixed").tolist()
    if "time" in data_array.coords:
        values = np.atleast_1d(data_array["time"].values)
        return pd.to_datetime(values, errors="coerce", format="mixed").tolist()

    if dataset is not None:
        if "valid_time" in dataset.coords:
            values = np.atleast_1d(dataset["valid_time"].values)
            return pd.to_datetime(values, errors="coerce", format="mixed").tolist()
        if "time" in dataset.coords:
            values = np.atleast_1d(dataset["time"].values)
            return pd.to_datetime(values, errors="coerce", format="mixed").tolist()

    return [pd.NaT]


def _build_lat_lon_grids(data_array):
    lat_name = _choose_coord_name(data_array, "latitude", "lat")
    lon_name = _choose_coord_name(data_array, "longitude", "lon")
    if lat_name is None or lon_name is None:
        raise ValueError("Latitude/longitude coordinates were not found in the GRIB dataset.")

    lat_values = np.asarray(data_array[lat_name].values, dtype=float)
    lon_values = np.asarray(data_array[lon_name].values, dtype=float)

    if lat_values.ndim == 1 and lon_values.ndim == 1:
        lat_grid, lon_grid = np.meshgrid(lat_values, lon_values, indexing="ij")
    else:
        lat_grid, lon_grid = lat_values, lon_values

    return lat_grid, lon_grid, lat_name, lon_name


def _idw_interpolate_dataarray(data_array, target_lat: float, target_lon: float, power: int = 2) -> np.ndarray:
    da = _reduce_dataarray_to_time_lat_lon(data_array)
    lat_grid, lon_grid, lat_name, lon_name = _build_lat_lon_grids(da)

    normalized_lon = _normalize_target_longitude(da, target_lon)
    dist = np.sqrt((lat_grid - target_lat) ** 2 + (lon_grid - normalized_lon) ** 2)
    dist_flat = dist.reshape(-1)

    time_dim = None
    for candidate in ("valid_time", "time"):
        if candidate in da.dims:
            time_dim = candidate
            break

    if time_dim is not None:
        ordered_dims = [time_dim] + [dim for dim in da.dims if dim != time_dim]
        da = da.transpose(*ordered_dims)
        data = np.asarray(da.values, dtype=float)
        data_2d = data.reshape(data.shape[0], -1)
    else:
        data = np.asarray(da.values, dtype=float)
        data_2d = data.reshape(1, -1)

    exact_idx = np.where(dist_flat < 1e-12)[0]
    if exact_idx.size > 0:
        return np.asarray(data_2d[:, exact_idx[0]], dtype=float)

    weights = 1.0 / np.power(dist_flat, power)
    finite_mask = np.isfinite(data_2d)
    weighted_data = np.where(finite_mask, data_2d * weights, 0.0)
    effective_weights = np.where(finite_mask, weights, 0.0)
    numerator = weighted_data.sum(axis=1)
    denominator = effective_weights.sum(axis=1)

    with np.errstate(invalid="ignore", divide="ignore"):
        values = numerator / denominator

    return np.asarray(values, dtype=float)


def _safe_open_cfgrib_datasets(file_path: Path):
    try:
        import cfgrib
    except Exception as exc:
        raise ImportError(
            "cfgrib is not installed or could not be imported. Install cfgrib and eccodes to extract GRIB data."
        ) from exc

    return cfgrib.open_datasets(
        str(file_path),
        backend_kwargs={
            "indexpath": "",
            "cache_geo_coords": True,
            "read_keys": ["shortName", "cfVarName", "paramId", "parameterNumber"],
        },
    )


def process_grib_file_df(file_path: str, latitude: float, longitude: float, idw_power: int, log_file: str) -> GribProcessResult:
    path_obj = Path(file_path)
    setup_logging(Path(log_file))
    result = GribProcessResult(file_path=str(path_obj))

    try:
        datasets = _safe_open_cfgrib_datasets(path_obj)
        result.open_ok = True
        result.dataset_count = len(datasets)
    except Exception as exc:
        result.open_error = str(exc)
        logging.error("Failed to open %s with cfgrib. Error: %s", path_obj, exc)
        return result

    data_records: Dict[str, Dict[str, Optional[float]]] = {}

    for dataset_index, ds in enumerate(datasets):
        try:
            data_var_names = list(ds.data_vars)
        except Exception:
            data_var_names = []

        if not data_var_names:
            result.dataset_without_variables_count += 1
            logging.info("Dataset #%d in %s contains no data variables.", dataset_index, path_obj.name)
            try:
                ds.close()
            except Exception:
                pass
            continue

        for data_var_name in data_var_names:
            result.inspected_variable_count += 1
            try:
                da = ds[data_var_name]
                var_key = _identify_variable_key(data_var_name, da)
                if var_key is None:
                    result.skipped_variable_count += 1
                    logging.info(
                        "Variable skipped in %s: dataset=%d, variable='%s'",
                        path_obj.name,
                        dataset_index,
                        data_var_name,
                    )
                    continue

                result.recognized_variable_count += 1
                interpolated_values = _idw_interpolate_dataarray(
                    da,
                    target_lat=latitude,
                    target_lon=longitude,
                    power=idw_power,
                )
                time_values = _extract_time_values(da, dataset=ds)

                if len(time_values) != len(interpolated_values):
                    if len(time_values) == 1 and len(interpolated_values) > 1:
                        time_values = time_values * len(interpolated_values)
                    elif len(interpolated_values) == 1 and len(time_values) > 1:
                        interpolated_values = np.repeat(interpolated_values[0], len(time_values))
                    else:
                        min_len = min(len(time_values), len(interpolated_values))
                        time_values = time_values[:min_len]
                        interpolated_values = interpolated_values[:min_len]

                for valid_time, value in zip(time_values, interpolated_values):
                    if pd.isna(valid_time):
                        time_key = "NaT"
                    else:
                        time_key = pd.Timestamp(valid_time).strftime("%Y-%m-%d %H:%M:%S")

                    if time_key not in data_records:
                        data_records[time_key] = {}
                    data_records[time_key][var_key] = float(value) if np.isfinite(value) else None
            except Exception as exc:
                result.variable_error_count += 1
                logging.warning("Error processing variable '%s' in %s: %s", data_var_name, path_obj, exc)
                continue

        try:
            ds.close()
        except Exception:
            pass

    if not data_records:
        logging.warning(
            "Extraction produced no usable rows for %s. open_ok=%s, datasets=%d, inspected_variables=%d, recognized_variables=%d, skipped_variables=%d, variable_errors=%d",
            path_obj,
            result.open_ok,
            result.dataset_count,
            result.inspected_variable_count,
            result.recognized_variable_count,
            result.skipped_variable_count,
            result.variable_error_count,
        )
        return result

    records = []
    for dt_value, vars_data in data_records.items():
        row = {"datetime": dt_value}
        for var_name in VARIABLES.keys():
            row[var_name] = vars_data.get(var_name, None)
        records.append(row)

    df = pd.DataFrame(records)
    if not df.empty:
        result.dataframe = df
        result.extracted_timestamp_count = len(df)

    logging.info(
        "Extraction succeeded for %s. datasets=%d, inspected_variables=%d, recognized_variables=%d, extracted_timestamps=%d",
        path_obj,
        result.dataset_count,
        result.inspected_variable_count,
        result.recognized_variable_count,
        result.extracted_timestamp_count,
    )
    return result


def run_extract_only(config: Era5Config, reporter: ProgressReporter) -> Path:
    config.data_dir.mkdir(parents=True, exist_ok=True)
    config.results_dir.mkdir(parents=True, exist_ok=True)

    output_csv = config.output_csv
    if output_csv.exists():
        output_csv.unlink()
        reporter.log(f"Deleted previous CSV output: {output_csv}")

    grib_files = sorted(
        str(config.data_dir / file_name)
        for file_name in os.listdir(config.data_dir)
        if file_name.lower().endswith(GRIB_EXTENSIONS)
    )
    if not grib_files:
        raise FileNotFoundError(f"No GRIB files found in '{config.data_dir}'.")

    reporter.status("Extracting existing GRIB files...")
    reporter.log(f"Found {len(grib_files)} local GRIB files in {config.data_dir}")

    dataframes: List[pd.DataFrame] = []
    completed = 0
    max_workers = max(1, (os.cpu_count() or 1))

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(process_grib_file_df, file_path, config.latitude, config.longitude, config.idw_power, str(config.log_file)): file_path
            for file_path in grib_files
        }

        for future in as_completed(futures):
            file_name = Path(futures[future]).name
            try:
                result = future.result(timeout=config.timeout_per_file)
                if result.has_data:
                    dataframes.append(result.dataframe)
                _report_process_result(result, reporter, include_download_context=False)
            except TimeoutError:
                reporter.log(f"Extraction timed out for {file_name}")
                future.cancel()
            except Exception as exc:
                reporter.log(f"Extraction crashed for {file_name}: {exc}")

            completed += 1
            reporter.progress(completed, len(grib_files), f"Processed {completed}/{len(grib_files)} files")

    if not dataframes:
        raise RuntimeError("No data was extracted from any GRIB file.")

    final_df = pd.concat(dataframes, ignore_index=True)
    final_df["datetime"] = pd.to_datetime(final_df["datetime"], errors="coerce")
    final_df.sort_values(by="datetime", inplace=True)
    final_df.to_csv(output_csv, index=False)
    reporter.log(f"CSV written to {output_csv}")
    return output_csv


def run_download_and_process(config: Era5Config, reporter: ProgressReporter) -> Path:
    config.data_dir.mkdir(parents=True, exist_ok=True)
    config.results_dir.mkdir(parents=True, exist_ok=True)

    reporter.status("Initializing CDS client...")
    client = initialize_cds_client()
    reporter.log("CDS API client initialized successfully.")

    variable_list = [spec["request_code"] for spec in VARIABLES.values()]
    output_csv = config.output_csv
    area = config.area

    reporter.log(
        "Using ERA5-aligned request window: "
        f"north={area[0]}, west={area[1]}, south={area[2]}, east={area[3]}"
    )
    reporter.log(
        "Grid points (lon, lat): " + ", ".join(format_lon_lat_pair(lon, lat) for lat, lon in config.surrounding_points)
    )

    rows_accumulated: List[pd.DataFrame] = []
    total_requests = len(config.years) * 12
    done = 0

    for year in config.years:
        for month in range(1, 13):
            reporter.status(f"Downloading {year}-{month:02d}...")
            file_path = download_monthly_data(
                client=client,
                year=year,
                month=month,
                variable_list=variable_list,
                area=area,
                grid=config.grid,
                output_dir=config.data_dir,
                request_delay=config.request_delay,
                max_retries=config.max_retries,
                reporter=reporter,
            )

            if file_path:
                result = process_grib_file_df(str(file_path), config.latitude, config.longitude, config.idw_power, str(config.log_file))
                if result.has_data:
                    rows_accumulated.append(result.dataframe)
                _report_process_result(result, reporter, include_download_context=True)
            else:
                reporter.log(f"Download failed for {year}-{month:02d}. Monthly step skipped before extraction.")

            done += 1
            reporter.progress(done, total_requests, f"Completed {done}/{total_requests} monthly steps")
            time.sleep(config.request_delay)

    if not rows_accumulated:
        raise RuntimeError("No data was downloaded/extracted successfully.")

    final_df = pd.concat(rows_accumulated, ignore_index=True)
    final_df["datetime"] = pd.to_datetime(final_df["datetime"], errors="coerce")
    final_df.sort_values(by="datetime", inplace=True)
    final_df.to_csv(output_csv, index=False)
    reporter.log(f"CSV written to {output_csv}")
    return output_csv


def execute_pipeline(mode: str, config: Era5Config, callback: Optional[Callable[[str, Dict], None]] = None) -> Path:
    setup_logging(config.log_file)
    reporter = ProgressReporter(callback)
    reporter.log("Starting ERA5 workflow.")
    validate_runtime_dependencies(mode=mode, wants_gui=False, reporter=reporter)
    target_point_text = format_target_point(config.longitude, config.latitude)
    reporter.log(f"Target point: {target_point_text}")

    if mode == "extract":
        return run_extract_only(config, reporter)
    if mode == "download":
        return run_download_and_process(config, reporter)
    raise ValueError(f"Unsupported mode: {mode}")


class Era5DownloaderGUI:
    def __init__(self, root: "tk.Tk") -> None:
        self.root = root
        self.root.title("ERA5 Hourly Downloader and Extractor")
        self.root.geometry(f"{DEFAULT_WINDOW_WIDTH}x{DEFAULT_WINDOW_HEIGHT}")
        self.root.minsize(DEFAULT_WINDOW_MIN_WIDTH, DEFAULT_WINDOW_MIN_HEIGHT)

        self.message_queue: "queue.Queue[Tuple[str, Dict]]" = queue.Queue()
        self.worker_thread: Optional[threading.Thread] = None

        self.mode_var = tk.StringVar(value=INITIAL_MODE)
        self.longitude_var = tk.StringVar(value=f"{INITIAL_LONGITUDE:.8f}")
        self.latitude_var = tk.StringVar(value=f"{INITIAL_LATITUDE:.8f}")
        self.start_year_var = tk.StringVar(value=str(INITIAL_START_YEAR))
        self.end_year_var = tk.StringVar(value=str(INITIAL_END_YEAR))
        self.data_dir_var = tk.StringVar(value=INITIAL_DATA_DIR)
        self.results_dir_var = tk.StringVar(value=INITIAL_RESULTS_DIR)
        self.output_csv_var = tk.StringVar(value=INITIAL_OUTPUT_CSV_NAME)
        self.log_file_var = tk.StringVar(value=INITIAL_LOG_FILE)
        self.status_var = tk.StringVar(value="Ready. Configure the run and click Start.")
        self.grid_info_var = tk.StringVar(value="ERA5 request window will be shown after validation.")
        self.progress_label_var = tk.StringVar(value="Idle")
        self.eta_label_var = tk.StringVar(value="Estimated completion time: not available yet.")
        self.run_start_monotonic: Optional[float] = None
        self.last_progress_current = 0

        self._configure_style()
        self._build_header()
        self._build_body()
        self._build_footer()
        self._freeze_initial_geometry()
        self._refresh_grid_summary()
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)
        self.root.after(150, self._poll_messages)

    def _freeze_initial_geometry(self) -> None:
        """Lock the initial geometry so widget content does not resize the window."""
        self.root.update_idletasks()
        width = max(self.root.winfo_width(), self.root.winfo_reqwidth(), DEFAULT_WINDOW_WIDTH)
        height = max(self.root.winfo_height(), self.root.winfo_reqheight(), DEFAULT_WINDOW_HEIGHT)
        self.root.geometry(f"{width}x{height}")
        self.root.minsize(width, height)

    def _configure_style(self) -> None:
        style = ttk.Style(self.root)
        for theme_name in ("vista", "xpnative", "clam", "alt", "default"):
            if theme_name in style.theme_names():
                try:
                    style.theme_use(theme_name)
                    break
                except Exception:
                    pass

        self.root.configure(bg="#eef3f8")
        style.configure("TNotebook", background="#eef3f8", borderwidth=0)
        style.configure("TNotebook.Tab", padding=(16, 8), font=("Segoe UI", 10, "bold"))
        style.configure("Card.TLabelframe", padding=10)
        style.configure("Card.TLabelframe.Label", font=("Segoe UI", 10, "bold"))
        style.configure("Primary.TButton", font=("Segoe UI", 10, "bold"))
        style.configure("TButton", padding=(10, 6), font=("Segoe UI", 10))
        style.configure("TLabel", font=("Segoe UI", 10))
        style.configure("Small.TLabel", font=("Segoe UI", 9))
        style.configure("Status.TLabel", font=("Segoe UI", 10))

    def _build_header(self) -> None:
        header = tk.Frame(self.root, bg="#1f3b5b", padx=16, pady=14)
        header.pack(fill="x")

        tk.Label(
            header,
            text="ERA5 Hourly Data Downloader and Extractor",
            bg="#1f3b5b",
            fg="white",
            font=("Segoe UI", 18, "bold"),
            anchor="w",
        ).pack(fill="x")

        tk.Label(
            header,
            text=(
                "Production GUI for ERA5 download and extraction with integrated "
                "progress reporting, logging, and request validation."
            ),
            bg="#1f3b5b",
            fg="#d7e6f5",
            font=("Segoe UI", 10),
            anchor="w",
            justify="left",
        ).pack(fill="x", pady=(4, 0))

    def _build_body(self) -> None:
        outer = ttk.Frame(self.root, padding=12)
        outer.pack(fill="both", expand=True)

        self.notebook = ttk.Notebook(outer)
        self.notebook.pack(fill="both", expand=True)

        self.run_tab = ttk.Frame(self.notebook, padding=14)
        self.log_tab = ttk.Frame(self.notebook, padding=14)
        self.instructions_tab = ttk.Frame(self.notebook, padding=14)

        self.notebook.add(self.run_tab, text="Run")
        self.notebook.add(self.log_tab, text="Log")
        self.notebook.add(self.instructions_tab, text="Instructions")

        self._build_run_tab()
        self._build_log_tab()
        self._build_instructions_tab()

    def _build_run_tab(self) -> None:
        self.run_tab.columnconfigure(0, weight=3)
        self.run_tab.columnconfigure(1, weight=2)

        left = ttk.Frame(self.run_tab)
        right = ttk.Frame(self.run_tab)
        left.grid(row=0, column=0, sticky="nsew", padx=(0, 8))
        right.grid(row=0, column=1, sticky="nsew", padx=(8, 0))
        left.columnconfigure(0, weight=1)
        right.columnconfigure(0, weight=1)

        mode_card = ttk.LabelFrame(left, text="Mode", style="Card.TLabelframe")
        mode_card.grid(row=0, column=0, sticky="ew")
        ttk.Radiobutton(mode_card, text="Download from CDS and process", value="download", variable=self.mode_var).grid(row=0, column=0, sticky="w", pady=4)
        ttk.Radiobutton(mode_card, text="Extract existing GRIB files only", value="extract", variable=self.mode_var).grid(row=1, column=0, sticky="w", pady=4)

        point_card = ttk.LabelFrame(left, text="Target point and time range", style="Card.TLabelframe")
        point_card.grid(row=1, column=0, sticky="ew", pady=(12, 0))
        point_card.columnconfigure(1, weight=1, minsize=220)

        ttk.Label(point_card, text="Longitude").grid(row=0, column=0, sticky="w", padx=(0, 8), pady=6)
        lon_entry = ttk.Entry(point_card, textvariable=self.longitude_var, width=DEFAULT_COORD_ENTRY_WIDTH)
        lon_entry.grid(row=0, column=1, sticky="ew", pady=6)
        lon_entry.bind("<KeyRelease>", lambda _event: self._refresh_grid_summary())

        ttk.Label(point_card, text="Latitude").grid(row=1, column=0, sticky="w", padx=(0, 8), pady=6)
        lat_entry = ttk.Entry(point_card, textvariable=self.latitude_var, width=DEFAULT_COORD_ENTRY_WIDTH)
        lat_entry.grid(row=1, column=1, sticky="ew", pady=6)
        lat_entry.bind("<KeyRelease>", lambda _event: self._refresh_grid_summary())

        ttk.Label(point_card, text="Start year").grid(row=2, column=0, sticky="w", padx=(0, 8), pady=6)
        ttk.Entry(point_card, textvariable=self.start_year_var, width=DEFAULT_YEAR_ENTRY_WIDTH).grid(row=2, column=1, sticky="ew", pady=6)

        ttk.Label(point_card, text="End year").grid(row=3, column=0, sticky="w", padx=(0, 8), pady=6)
        ttk.Entry(point_card, textvariable=self.end_year_var, width=DEFAULT_YEAR_ENTRY_WIDTH).grid(row=3, column=1, sticky="ew", pady=6)

        ttk.Label(
            point_card,
            textvariable=self.grid_info_var,
            style="Small.TLabel",
            wraplength=520,
            justify="left",
        ).grid(row=4, column=0, columnspan=2, sticky="w", pady=(8, 2))

        paths_card = ttk.LabelFrame(left, text="Folders and output", style="Card.TLabelframe")
        paths_card.grid(row=2, column=0, sticky="ew", pady=(12, 0))
        paths_card.columnconfigure(1, weight=1, minsize=360)

        ttk.Label(paths_card, text="GRIB folder").grid(row=0, column=0, sticky="w", padx=(0, 8), pady=6)
        ttk.Entry(paths_card, textvariable=self.data_dir_var, width=DEFAULT_PATH_ENTRY_WIDTH).grid(row=0, column=1, sticky="ew", pady=6)
        ttk.Button(paths_card, text="Browse", command=self._browse_data_dir).grid(row=0, column=2, sticky="ew", padx=(8, 0), pady=6)

        ttk.Label(paths_card, text="Results folder").grid(row=1, column=0, sticky="w", padx=(0, 8), pady=6)
        ttk.Entry(paths_card, textvariable=self.results_dir_var, width=DEFAULT_PATH_ENTRY_WIDTH).grid(row=1, column=1, sticky="ew", pady=6)
        ttk.Button(paths_card, text="Browse", command=self._browse_results_dir).grid(row=1, column=2, sticky="ew", padx=(8, 0), pady=6)

        ttk.Label(paths_card, text="Output CSV name").grid(row=2, column=0, sticky="w", padx=(0, 8), pady=6)
        ttk.Entry(paths_card, textvariable=self.output_csv_var, width=DEFAULT_PATH_ENTRY_WIDTH).grid(row=2, column=1, columnspan=2, sticky="ew", pady=6)

        ttk.Label(paths_card, text="Log file").grid(row=3, column=0, sticky="w", padx=(0, 8), pady=6)
        ttk.Entry(paths_card, textvariable=self.log_file_var, width=DEFAULT_PATH_ENTRY_WIDTH).grid(row=3, column=1, sticky="ew", pady=6)
        ttk.Button(paths_card, text="Browse", command=self._browse_log_file).grid(row=3, column=2, sticky="ew", padx=(8, 0), pady=6)

        action_card = ttk.LabelFrame(right, text="Actions", style="Card.TLabelframe")
        action_card.grid(row=0, column=0, sticky="new")
        action_card.columnconfigure(0, weight=1)

        self.start_button = ttk.Button(action_card, text="Start", style="Primary.TButton", command=self.start_run)
        self.start_button.grid(row=0, column=0, sticky="ew", pady=(0, 8))
        ttk.Button(action_card, text="Open Log tab", command=lambda: self.notebook.select(self.log_tab)).grid(row=1, column=0, sticky="ew", pady=4)
        ttk.Button(action_card, text="Refresh request summary", command=self._refresh_grid_summary).grid(row=2, column=0, sticky="ew", pady=4)
        ttk.Button(action_card, text="Quit", command=self.root.destroy).grid(row=3, column=0, sticky="ew", pady=4)

        summary_card = ttk.LabelFrame(right, text="Current request profile", style="Card.TLabelframe")
        summary_card.grid(row=1, column=0, sticky="new", pady=(12, 0))
        ttk.Label(
            summary_card,
            text=(
                "Spatial request:\n"
                "- ERA5 grid step: 0.25°\n"
                "- request mode: 4 surrounding points (2x2)\n"
                "- interpolation: inverse distance weighting (IDW)\n\n"
                "Operational notes:\n"
                "- download mode handles one month per request\n"
                "- extract mode scans the selected GRIB folder\n"
                "- output CSV is sorted by datetime"
            ),
            justify="left",
        ).pack(fill="x")

        progress_card = ttk.LabelFrame(right, text="Progress", style="Card.TLabelframe")
        progress_card.grid(row=2, column=0, sticky="new", pady=(12, 0))
        self.progress_bar = ttk.Progressbar(progress_card, mode="determinate", maximum=100)
        self.progress_bar.pack(fill="x", pady=(0, 8))
        ttk.Label(progress_card, textvariable=self.progress_label_var, justify="left", wraplength=300).pack(fill="x")
        ttk.Label(progress_card, textvariable=self.eta_label_var, justify="left", wraplength=300).pack(fill="x", pady=(6, 0))

    def _format_eta_text(self, current: int, total: int) -> str:
        if self.run_start_monotonic is None or current <= 0 or total <= 0:
            return "Estimated completion time: not available yet."

        elapsed_seconds = max(0.0, time.monotonic() - self.run_start_monotonic)
        average_seconds_per_step = elapsed_seconds / float(current)
        remaining_steps = max(0, total - current)
        remaining_seconds = average_seconds_per_step * remaining_steps
        eta_timestamp = time.time() + remaining_seconds
        eta_dt = datetime.fromtimestamp(eta_timestamp)
        eta_clock = eta_dt.strftime("%H:%M:%S")

        if eta_dt.date() == datetime.now().date():
            day_suffix = ""
        else:
            day_suffix = eta_dt.strftime(" (%Y-%m-%d)")

        return (
            f"Estimated completion time: {eta_clock}{day_suffix} "
            f"(average {average_seconds_per_step:.1f} s per completed file)"
        )

    def _build_log_tab(self) -> None:
        log_frame = ttk.LabelFrame(self.log_tab, text="Execution log", style="Card.TLabelframe")
        log_frame.pack(fill="both", expand=True)

        self.log_box = scrolledtext.ScrolledText(
            log_frame,
            wrap="word",
            font=("Consolas", 13),
            padx=12,
            pady=12,
            borderwidth=0,
            relief="flat",
            background="white",
        )
        self.log_box.pack(fill="both", expand=True)
        self.log_box.configure(state="disabled")

    def _build_instructions_tab(self) -> None:
        info_frame = ttk.LabelFrame(self.instructions_tab, text="Usage", style="Card.TLabelframe")
        info_frame.pack(fill="both", expand=True)
        text_box = scrolledtext.ScrolledText(
            info_frame,
            wrap="word",
            font=("Consolas", 13),
            padx=12,
            pady=12,
            borderwidth=0,
            relief="flat",
            background="white",
        )
        text_box.pack(fill="both", expand=True)
        text_box.insert("1.0", INSTRUCTIONS_TEXT)
        text_box.configure(state="disabled")

    def _build_footer(self) -> None:
        footer = ttk.Frame(self.root, padding=(12, 0, 12, 12))
        footer.pack(fill="x")
        ttk.Label(
            footer,
            textvariable=self.status_var,
            style="Status.TLabel",
            wraplength=920,
            justify="left",
        ).pack(fill="x")

    def _append_log(self, message: str) -> None:
        self.log_box.configure(state="normal")
        self.log_box.insert("end", message.rstrip() + "\n")
        self.log_box.see("end")
        self.log_box.configure(state="disabled")
        self.root.update_idletasks()

    def _browse_data_dir(self) -> None:
        path = filedialog.askdirectory(initialdir=self.data_dir_var.get() or str(SCRIPT_DIR))
        if path:
            self.data_dir_var.set(path)

    def _browse_results_dir(self) -> None:
        path = filedialog.askdirectory(initialdir=self.results_dir_var.get() or str(SCRIPT_DIR))
        if path:
            self.results_dir_var.set(path)

    def _browse_log_file(self) -> None:
        path = filedialog.asksaveasfilename(
            title="Select log file",
            initialdir=str(SCRIPT_DIR),
            initialfile=Path(self.log_file_var.get()).name,
            defaultextension=".log",
            filetypes=[("Log files", "*.log"), ("All files", "*.*")],
        )
        if path:
            self.log_file_var.set(path)

    def _collect_current_defaults(self) -> Dict[str, str]:
        return {
            "mode": self.mode_var.get().strip() or HARDCODED_DEFAULTS["mode"],
            "longitude": self.longitude_var.get().strip() or HARDCODED_DEFAULTS["longitude"],
            "latitude": self.latitude_var.get().strip() or HARDCODED_DEFAULTS["latitude"],
            "start_year": self.start_year_var.get().strip() or HARDCODED_DEFAULTS["start_year"],
            "end_year": self.end_year_var.get().strip() or HARDCODED_DEFAULTS["end_year"],
            "data_dir": self.data_dir_var.get().strip() or HARDCODED_DEFAULTS["data_dir"],
            "results_dir": self.results_dir_var.get().strip() or HARDCODED_DEFAULTS["results_dir"],
            "output_csv_name": self.output_csv_var.get().strip() or HARDCODED_DEFAULTS["output_csv_name"],
            "log_file": self.log_file_var.get().strip() or HARDCODED_DEFAULTS["log_file"],
        }

    def _persist_current_defaults(self) -> None:
        try:
            save_defaults(self._collect_current_defaults())
        except Exception:
            pass

    def _on_close(self) -> None:
        self._persist_current_defaults()
        self.root.destroy()

    def _refresh_grid_summary(self) -> None:
        try:
            latitude = float(self.latitude_var.get().strip())
            longitude = float(self.longitude_var.get().strip())
            north, west, south, east = compute_surrounding_four_point_area(latitude, longitude)
            points = [(north, west), (north, east), (south, west), (south, east)]
            points_text = "; ".join(format_lon_lat_pair(lon, lat) for lat, lon in points)
            self.grid_info_var.set(
                f"Request area: north={north:.5f}, west={west:.5f}, south={south:.5f}, east={east:.5f}\n"
                f"Grid nodes (lon, lat): {points_text}"
            )
        except Exception:
            self.grid_info_var.set("Enter valid longitude/latitude values to compute the ERA5 request window.")

    def _build_config(self) -> Era5Config:
        latitude = float(self.latitude_var.get().strip())
        longitude = float(self.longitude_var.get().strip())
        start_year = int(self.start_year_var.get().strip())
        end_year = int(self.end_year_var.get().strip())
        output_csv_name = self.output_csv_var.get().strip() or DEFAULT_OUTPUT_CSV_NAME
        if start_year > end_year:
            raise ValueError("Start year must be less than or equal to end year.")

        save_defaults(self._collect_current_defaults())

        return Era5Config(
            longitude=longitude,
            latitude=latitude,
            start_year=start_year,
            end_year=end_year,
            data_dir=Path(self.data_dir_var.get().strip()).expanduser(),
            results_dir=Path(self.results_dir_var.get().strip()).expanduser(),
            output_csv_name=output_csv_name,
            log_file=Path(self.log_file_var.get().strip()).expanduser(),
        )

    def start_run(self) -> None:
        if self.worker_thread is not None and self.worker_thread.is_alive():
            messagebox.showinfo("ERA5", "A run is already in progress.")
            return

        try:
            config = self._build_config()
        except Exception as exc:
            messagebox.showerror("Invalid configuration", str(exc))
            return

        self._append_log("=" * 72)
        self._append_log("Preparing new run...")
        self._append_log(
            "Mode: {} | Target (lon, lat): ({:.8f}, {:.8f}) | Years: {}-{}".format(
                self.mode_var.get(),
                config.longitude,
                config.latitude,
                config.start_year,
                config.end_year,
            )
        )
        self.progress_bar["value"] = 0
        self.progress_label_var.set("Starting...")
        self.eta_label_var.set("Estimated completion time: calculating after the first completed file...")
        self.run_start_monotonic = time.monotonic()
        self.last_progress_current = 0
        self.status_var.set("Running. Progress and detailed messages will appear below.")
        self.start_button.state(["disabled"])

        def worker() -> None:
            try:
                output_path = execute_pipeline(
                    mode=self.mode_var.get(),
                    config=config,
                    callback=lambda event, payload: self.message_queue.put((event, payload)),
                )
                self.message_queue.put(("done", {"output_path": str(output_path)}))
            except Exception as exc:
                self.message_queue.put(("error", {"message": str(exc)}))

        self.worker_thread = threading.Thread(target=worker, daemon=True)
        self.worker_thread.start()

    def _poll_messages(self) -> None:
        try:
            while True:
                event, payload = self.message_queue.get_nowait()
                if event == "log":
                    self._append_log(payload.get("message", ""))
                elif event == "status":
                    self.status_var.set(payload.get("message", ""))
                elif event == "progress":
                    total = max(1, int(payload.get("total", 1)))
                    current = max(0, int(payload.get("current", 0)))
                    if current > self.last_progress_current:
                        self.last_progress_current = current
                    self.progress_bar["value"] = max(0.0, min(100.0, current * 100.0 / total))
                    self.progress_label_var.set(payload.get("message", f"{current}/{total}"))
                    self.eta_label_var.set(self._format_eta_text(current, total))
                elif event == "done":
                    output_path = payload.get("output_path", "")
                    self.status_var.set(f"Completed successfully. CSV written to: {output_path}")
                    self.progress_bar["value"] = 100
                    self.progress_label_var.set("Completed")
                    self.eta_label_var.set("Estimated completion time: completed.")
                    self.start_button.state(["!disabled"])
                    self._append_log(f"Completed successfully. CSV written to: {output_path}")
                    self.notebook.select(self.log_tab)
                elif event == "error":
                    message = payload.get("message", "Unknown error.")
                    self.status_var.set(f"Run failed: {message}")
                    self.progress_label_var.set("Failed")
                    self.eta_label_var.set("Estimated completion time: unavailable due to failure.")
                    self.start_button.state(["!disabled"])
                    self._append_log(f"ERROR: {message}")
                    self.notebook.select(self.log_tab)
        except queue.Empty:
            pass
        finally:
            self.root.after(150, self._poll_messages)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="ERA5 downloader/extractor with GUI and CLI support.")
    parser.add_argument("--gui", action="store_true", help="Launch the GUI.")
    parser.add_argument("--download", action="store_true", help="Run download + process from CLI.")
    parser.add_argument("--extract", action="store_true", help="Run extract-only mode from CLI.")
    parser.add_argument("--longitude", type=float, default=INITIAL_LONGITUDE)
    parser.add_argument("--latitude", type=float, default=INITIAL_LATITUDE)
    parser.add_argument("--start-year", type=int, default=INITIAL_START_YEAR)
    parser.add_argument("--end-year", type=int, default=INITIAL_END_YEAR)
    parser.add_argument("--data-dir", default=INITIAL_DATA_DIR)
    parser.add_argument("--results-dir", default=INITIAL_RESULTS_DIR)
    parser.add_argument("--output-csv", default=INITIAL_OUTPUT_CSV_NAME)
    parser.add_argument("--log-file", default=INITIAL_LOG_FILE)
    return parser


def run_cli(args: argparse.Namespace) -> int:
    mode = "download" if args.download else "extract"
    config = Era5Config(
        longitude=args.longitude,
        latitude=args.latitude,
        start_year=args.start_year,
        end_year=args.end_year,
        data_dir=Path(args.data_dir),
        results_dir=Path(args.results_dir),
        output_csv_name=args.output_csv,
        log_file=Path(args.log_file),
    )

    save_defaults(
        {
            "mode": mode,
            "longitude": f"{config.longitude:.8f}",
            "latitude": f"{config.latitude:.8f}",
            "start_year": str(config.start_year),
            "end_year": str(config.end_year),
            "data_dir": str(config.data_dir),
            "results_dir": str(config.results_dir),
            "output_csv_name": config.output_csv_name,
            "log_file": str(config.log_file),
        }
    )

    def cli_callback(event: str, payload: Dict) -> None:
        if event == "log":
            print(payload.get("message", ""))
        elif event == "status":
            print(payload.get("message", ""))
        elif event == "progress":
            current = payload.get("current", 0)
            total = payload.get("total", 0)
            message = payload.get("message", "")
            print(f"[{current}/{total}] {message}")

    output_path = execute_pipeline(mode=mode, config=config, callback=cli_callback)
    print(f"Completed successfully. CSV written to: {output_path}")
    return 0


def main() -> int:
    os.chdir(SCRIPT_DIR)
    parser = build_parser()
    args = parser.parse_args()

    wants_cli = args.download or args.extract
    wants_gui = args.gui or not wants_cli
    mode = "download" if args.download or not args.extract else "extract"

    if wants_gui:
        log_file = Path(getattr(args, "log_file", INITIAL_LOG_FILE)).expanduser()
        setup_logging(log_file)
        validate_runtime_dependencies(mode=mode, wants_gui=True, reporter=None)
        root = tk.Tk()
        Era5DownloaderGUI(root)
        root.mainloop()
        return 0

    return run_cli(args)


if __name__ == "__main__":
    multiprocessing.freeze_support()
    try:
        multiprocessing.set_start_method("spawn", force=True)
    except RuntimeError:
        pass
    raise SystemExit(main())
