#!/usr/bin/env python3
# -*- coding: utf-8 -*-
r"""
ERA5 single-point wave and wind downloader.

Purpose
-------
Download ERA5 single-point time series from the Copernicus Climate Data Store
(CDS) dataset ``reanalysis-era5-single-levels-timeseries`` and create two CSV
files in the same directory as this script:

    - ``era5_data.csv``: merged raw ERA5 table, normalised to a compact set of
      columns used by the post-processing stage.
    - ``output.csv``: final engineering table with wave parameters, wind speed
      and meteorological wind direction.

The final ``output.csv`` columns are fixed:

    datetime,swh,mwp,mwd,wind,dwi,u10,v10

The intermediate merged ``era5_data.csv`` columns are fixed:

    datetime,swh,mwp,mwd,u10,v10

Data requested from CDS
-----------------------
The script performs one CDS request with ``data_format="csv"`` for these ERA5
single-level variables:

    - significant_height_of_combined_wind_waves_and_swell  -> swh
    - mean_wave_period                                     -> mwp
    - mean_wave_direction                                  -> mwd
    - 10m_u_component_of_wind                              -> u10
    - 10m_v_component_of_wind                              -> v10

Wind gust is not requested. The processed wind speed is computed from ``u10``
and ``v10``. The processed wind direction ``dwi`` is computed as a
meteorological direction in degrees, with North = 0 degrees, clockwise positive,
and values in the interval [0, 360).

CDS payload handling
--------------------
Although the request asks for CSV, CDS may return either a plain CSV file or a
ZIP archive containing separate CSV or Excel tables. This script reads all
recognised table members in the payload, maps ERA5 long variable names to short
column names, merges tables by ``datetime`` and writes a single ``era5_data.csv``.
No separate wave or wind CSV files are retained.

Execution modes
---------------
Run without arguments to open the graphical interface:

    python download_era5_data.py

Run from the command line with explicit coordinates and dates:

    python download_era5_data.py --longitude -9.58166667 --latitude 41.14833299 ^
        --start-date 1940-01-01 --end-date 2026-05-08

Run the graphical interface explicitly:

    python download_era5_data.py --gui

Python virtual environment and dependencies
-------------------------------------------
Create and activate a local Python virtual environment on Windows:

    python -m venv venv
    venv\Scripts\activate

Upgrade pip and install the runtime dependencies:

    python -m pip install --upgrade pip
    python -m pip install cdsapi numpy pandas openpyxl pyinstaller

Dependency notes:

    - ``cdsapi`` is required for CDS downloads.
    - ``numpy`` and ``pandas`` are required for table processing.
    - ``openpyxl`` is required only when CDS returns Excel table members.
    - ``pyinstaller`` is required only to build the standalone Windows EXE.
    - ``tkinter`` is used by the GUI and is normally included with standard
      Windows Python installations from python.org.

Syntax check
------------
Before building the executable, check that the script compiles as Python source:

    python -m py_compile download_era5_data.py

Standalone Windows executable
-----------------------------
Build a one-file standalone Windows GUI executable with PyInstaller and the
provided spec file:

    python -m PyInstaller --clean --noconfirm download_era5_data.spec

The executable will be created in:

    dist\download_era5_data.exe

The spec file must use ``console=False`` in the ``EXE(...)`` block. This builds
a GUI executable and prevents the background command-prompt window from opening
before the Tkinter window. The program also redirects missing standard streams
to ``os.devnull`` so third-party download libraries cannot fail when stdout or
stderr are unavailable in the GUI executable.

Run the executable by double-clicking it:

    dist\download_era5_data.exe

Configuration and outputs
-------------------------
The following files are written next to this script or next to the compiled
executable, depending on how the program is launched:

    - ``defaults.json``: last coordinates and date range used by the GUI/CLI.
    - ``download_era5_data.log``: execution log.
    - ``era5_data.csv``: merged raw table.
    - ``output.csv``: processed table for downstream use.

Operational notes
-----------------
- A configured CDS API key is required by ``cdsapi.Client``.
- Existing output files are checked before the download starts. If a CSV is open
  in Excel or another program, the script stops early with a practical message.
- CSV files are written through temporary files and then atomically replaced, so
  failed writes do not leave partially written final outputs.
- The script is intentionally dependency-light and does not require xarray,
  cfgrib, eccodes or pygrib.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import logging
import os
import queue
import sys
import threading
import time
import zipfile
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Callable, Iterable, Optional


# -----------------------------------------------------------------------------
# Windows GUI executable compatibility
# -----------------------------------------------------------------------------

_OPEN_STANDARD_STREAMS: list[object] = []


def _ensure_standard_streams() -> None:
    """Provide stdout/stderr/stdin replacements in --windowed PyInstaller builds.

    In a Windows GUI executable built with ``console=False`` / ``--windowed``,
    ``sys.stdout`` and ``sys.stderr`` may be ``None``. Some third-party
    packages used during HTTP transfers still expect file-like streams and call
    methods such as ``write()``, ``flush()`` or ``isatty()``. Assigning these
    streams to ``os.devnull`` keeps the GUI executable silent while preventing
    ``'NoneType' object has no attribute 'write'`` errors.
    """

    for stream_name, mode in (("stdout", "w"), ("stderr", "w"), ("stdin", "r")):
        if getattr(sys, stream_name, None) is None:
            stream = open(os.devnull, mode, encoding="utf-8", errors="replace")
            setattr(sys, stream_name, stream)
            _OPEN_STANDARD_STREAMS.append(stream)


def _application_directory() -> Path:
    """Return the directory where outputs and settings must be written.

    For normal Python execution this is the script folder. For a PyInstaller
    one-file executable, ``__file__`` points to a temporary extraction directory
    such as ``_MEIxxxxx``; therefore the correct persistent location is the
    directory containing ``sys.executable``.
    """

    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent


_ensure_standard_streams()

try:
    import numpy as np
except Exception:  # pragma: no cover - reported by validate_runtime_dependencies
    np = None

try:
    import pandas as pd
except Exception:  # pragma: no cover - reported by validate_runtime_dependencies
    pd = None

try:
    import cdsapi
except Exception:  # pragma: no cover - reported by validate_runtime_dependencies
    cdsapi = None

try:
    import tkinter as tk
    from tkinter import messagebox, scrolledtext, ttk
except Exception:  # pragma: no cover - GUI is optional
    tk = None
    messagebox = None
    scrolledtext = None
    ttk = None


# -----------------------------------------------------------------------------
# Paths and fixed CDS settings
# -----------------------------------------------------------------------------

SCRIPT_DIR = _application_directory()
DEFAULTS_JSON = SCRIPT_DIR / "defaults.json"
LOG_FILE = SCRIPT_DIR / "download_era5_data.log"
RAW_CSV = SCRIPT_DIR / "era5_data.csv"
OUTPUT_CSV = SCRIPT_DIR / "output.csv"
TEMP_PAYLOAD = SCRIPT_DIR / "_era5_cds_download_payload.zip"

DATASET_NAME = "reanalysis-era5-single-levels-timeseries"
TOTAL_PROGRESS_STEPS = 4

LONG_SWH = "significant_height_of_combined_wind_waves_and_swell"
LONG_MWP = "mean_wave_period"
LONG_MWD = "mean_wave_direction"
LONG_U10 = "10m_u_component_of_wind"
LONG_V10 = "10m_v_component_of_wind"

ERA5_VARIABLES = [LONG_U10, LONG_V10, LONG_MWD, LONG_MWP, LONG_SWH]
OUTPUT_COLUMNS = ["datetime", "swh", "mwp", "mwd", "wind", "dwi", "u10", "v10"]
MERGED_COLUMNS = ["datetime", "swh", "mwp", "mwd", "u10", "v10"]

HARDCODED_DEFAULTS: dict[str, str] = {
    "longitude": "-9.58166667",
    "latitude": "41.14833299",
    "start_date": "1940-01-01",
    "end_date": (date.today() - timedelta(days=5)).isoformat(),
}

RAW_TO_SHORT_CANDIDATES: dict[str, list[str]] = {
    "datetime": ["datetime", "valid_time", "time", "timestamp", "date"],
    "swh": [LONG_SWH, "swh"],
    "mwd": [LONG_MWD, "mwd"],
    "mwp": [LONG_MWP, "mwp", "pp1d"],
    "u10": [LONG_U10, "u10"],
    "v10": [LONG_V10, "v10"],
}

INSTRUCTIONS_TEXT = r"""
ERA5 single-point downloader
============================

The program performs one CDS request for wave and wind data. CDS can return a
plain CSV or a ZIP archive containing separate wave and atmospheric tables. The
program reads the available tables, merges them by datetime and writes one
merged era5_data.csv plus one processed output.csv.

Files written in the script or executable directory
---------------------------------------------------
- era5_data.csv
- output.csv
- download_era5_data.log
- defaults.json

Processed output format
-----------------------
output.csv uses these columns:
    datetime,swh,mwp,mwd,wind,dwi,u10,v10

era5_data.csv uses these columns:
    datetime,swh,mwp,mwd,u10,v10

Python virtual environment and dependencies
-------------------------------------------
Run these commands in Windows cmd from the folder containing the script:

    python -m venv venv
    venv\Scripts\activate
    python -m pip install --upgrade pip
    python -m pip install cdsapi numpy pandas openpyxl pyinstaller

Syntax check
------------

    python -m py_compile download_era5_data.py

Build one-file standalone Windows GUI EXE
-----------------------------------------

Compile with the provided spec file:

    python -m PyInstaller --clean --noconfirm download_era5_data.spec

The executable is created as:

    dist\download_era5_data.exe

The spec file must contain console=False in the EXE(...) block. This prevents
the background command-prompt window from appearing before the Tkinter GUI.

Usage notes
-----------
- Longitude and latitude must be entered in decimal degrees.
- Dates must use YYYY-MM-DD.
- Outputs are always written to the directory containing the script or EXE.
- Close existing CSV outputs before running the program again.
- A configured CDS API key is required by cdsapi.Client.
""".strip()

DEFAULT_WINDOW_WIDTH = 920
DEFAULT_WINDOW_HEIGHT = 560
DEFAULT_WINDOW_MIN_WIDTH = DEFAULT_WINDOW_WIDTH
DEFAULT_WINDOW_MIN_HEIGHT = DEFAULT_WINDOW_HEIGHT

HEADER_HEIGHT = 84
BODY_FRAME_WIDTH = 896
BODY_FRAME_HEIGHT = 386
FOOTER_HEIGHT = 54

RUN_LEFT_WIDTH = 536
RUN_RIGHT_WIDTH = 318
RUN_PANEL_HEIGHT = 300
POINT_CARD_WIDTH = RUN_LEFT_WIDTH
POINT_CARD_HEIGHT = 190
ACTION_CARD_WIDTH = RUN_RIGHT_WIDTH
ACTION_CARD_HEIGHT = 150
PROGRESS_CARD_WIDTH = RUN_RIGHT_WIDTH
PROGRESS_CARD_HEIGHT = 150

LOG_FRAME_WIDTH = BODY_FRAME_WIDTH - 56
LOG_FRAME_HEIGHT = BODY_FRAME_HEIGHT - 78
LOG_BOX_WIDTH_CHARS = 104
LOG_BOX_HEIGHT_LINES = 17
INSTRUCTIONS_FRAME_WIDTH = BODY_FRAME_WIDTH - 56
INSTRUCTIONS_FRAME_HEIGHT = BODY_FRAME_HEIGHT - 78
INSTRUCTIONS_BOX_WIDTH_CHARS = 104
INSTRUCTIONS_BOX_HEIGHT_LINES = 17
FOOTER_WRAP_LENGTH = 880


# -----------------------------------------------------------------------------
# Data model and defaults
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class Era5Config:
    """Runtime configuration for one ERA5 extraction."""

    longitude: float
    latitude: float
    start_date: str
    end_date: str

    @property
    def raw_csv_path(self) -> Path:
        return RAW_CSV

    @property
    def output_csv_path(self) -> Path:
        return OUTPUT_CSV

    @property
    def log_file(self) -> Path:
        return LOG_FILE


def _safe_float(value: object, fallback: float) -> float:
    try:
        return float(str(value).strip())
    except (TypeError, ValueError):
        return fallback


def load_defaults() -> dict[str, str]:
    """Load persisted GUI/CLI defaults and merge them with hardcoded defaults."""

    defaults = dict(HARDCODED_DEFAULTS)
    try:
        if DEFAULTS_JSON.exists():
            loaded = json.loads(DEFAULTS_JSON.read_text(encoding="utf-8"))
            if isinstance(loaded, dict):
                for key in defaults:
                    value = loaded.get(key)
                    if value is not None and str(value).strip():
                        defaults[key] = str(value).strip()
    except (OSError, json.JSONDecodeError):
        pass
    return defaults


def save_defaults(values: dict[str, str]) -> None:
    """Persist GUI/CLI defaults used for the next run."""

    merged = dict(HARDCODED_DEFAULTS)
    for key in merged:
        if key in values and values[key] is not None:
            merged[key] = str(values[key]).strip()
    DEFAULTS_JSON.write_text(
        json.dumps(merged, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


SAVED_DEFAULTS = load_defaults()
INITIAL_LONGITUDE = _safe_float(
    SAVED_DEFAULTS["longitude"],
    float(HARDCODED_DEFAULTS["longitude"]),
)
INITIAL_LATITUDE = _safe_float(
    SAVED_DEFAULTS["latitude"],
    float(HARDCODED_DEFAULTS["latitude"]),
)
INITIAL_START_DATE = SAVED_DEFAULTS["start_date"]
INITIAL_END_DATE = SAVED_DEFAULTS["end_date"]


# -----------------------------------------------------------------------------
# Logging and progress reporting
# -----------------------------------------------------------------------------

ProgressCallback = Callable[[str, dict[str, object]], None]


def setup_logging(log_file: Path) -> None:
    """Create a fresh file logger for the current run."""

    log_file.parent.mkdir(parents=True, exist_ok=True)
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    for handler in list(root_logger.handlers):
        root_logger.removeHandler(handler)
        try:
            handler.close()
        except Exception:
            pass

    handler = logging.FileHandler(log_file, encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    root_logger.addHandler(handler)


class ProgressReporter:
    """Small adapter used by CLI and GUI to receive pipeline messages."""

    def __init__(self, callback: Optional[ProgressCallback] = None) -> None:
        self.callback = callback

    def emit(self, event: str, **payload: object) -> None:
        if self.callback is not None:
            self.callback(event, payload)

    def log(self, message: str) -> None:
        logging.info(message)
        self.emit("log", message=message)

    def status(self, message: str) -> None:
        self.emit("status", message=message)

    def progress(self, current: int, total: int, message: str) -> None:
        self.emit("progress", current=current, total=total, message=message)


# -----------------------------------------------------------------------------
# Dependency and CDS client setup
# -----------------------------------------------------------------------------

def validate_runtime_dependencies(wants_gui: bool) -> None:
    """Fail early with a direct dependency message."""

    missing: list[str] = []
    if np is None:
        missing.append("numpy")
    if pd is None:
        missing.append("pandas")
    if cdsapi is None:
        missing.append("cdsapi")
    if wants_gui and tk is None:
        missing.append("tkinter")

    if missing:
        raise RuntimeError(
            "Missing required dependencies: "
            + ", ".join(missing)
            + ". Install them and run the script again."
        )


def initialize_cds_client() -> "cdsapi.Client":
    if cdsapi is None:
        raise RuntimeError("cdsapi is not installed.")
    return cdsapi.Client()


# -----------------------------------------------------------------------------
# CSV and payload parsing utilities
# -----------------------------------------------------------------------------

def normalize_name(value: object) -> str:
    """Return a stable lower-case token for matching ERA5 column names."""

    text = str(value).strip().lower()
    for char in (" ", "-", "/", "(", ")", "[", "]", ".", '"', "'"):
        text = text.replace(char, "_")
    while "__" in text:
        text = text.replace("__", "_")
    return text.strip("_")


def _decode_bytes_with_fallbacks(data: bytes, source_name: str) -> str:
    last_error: Optional[Exception] = None
    for encoding in ("utf-8-sig", "utf-8", "cp1252", "latin1"):
        try:
            return data.decode(encoding)
        except UnicodeDecodeError as exc:
            last_error = exc
    raise RuntimeError(f"Could not decode text payload '{source_name}': {last_error}")


def _detect_delimiter_from_header(header_line: str) -> str:
    return max((",", ";", "\t"), key=header_line.count)


def _parse_header_line(line: str, delimiter: str) -> list[str]:
    """Parse one header line while tolerating malformed metadata text."""

    try:
        tokens = next(csv.reader([line], delimiter=delimiter))
    except csv.Error:
        tokens = line.split(delimiter)
    return [str(token).strip().strip('"').strip("'") for token in tokens]


def _looks_like_table_header(line: str) -> bool:
    stripped = line.strip()
    if not stripped or stripped.startswith("#"):
        return False

    delimiter = _detect_delimiter_from_header(stripped)
    tokens = [normalize_name(token) for token in _parse_header_line(stripped, delimiter)]
    token_set = set(tokens)
    datetime_markers = {normalize_name(item) for item in RAW_TO_SHORT_CANDIDATES["datetime"]}
    variable_markers = {
        normalize_name(LONG_SWH),
        normalize_name(LONG_MWD),
        normalize_name(LONG_MWP),
        normalize_name(LONG_U10),
        normalize_name(LONG_V10),
        "swh",
        "mwd",
        "mwp",
        "u10",
        "v10",
    }
    return bool(token_set & datetime_markers) and bool(token_set & variable_markers)


def _detect_header_index(lines: list[str]) -> int:
    """Find the first row that appears to be a CDS table header."""

    for index, line in enumerate(lines):
        if _looks_like_table_header(line):
            return index
    return 0


def _read_csv_text_robust(text: str, source_name: str) -> "pd.DataFrame":
    """Read a CDS CSV table, ignoring metadata lines before the real header."""

    lines = text.replace("\r\n", "\n").replace("\r", "\n").split("\n")
    header_index = _detect_header_index(lines)
    useful_lines = lines[header_index:]

    while useful_lines and not useful_lines[-1].strip():
        useful_lines.pop()

    if not useful_lines:
        raise RuntimeError(f"CSV '{source_name}' is empty.")

    header_line = useful_lines[0].strip()
    delimiter = _detect_delimiter_from_header(header_line)
    header_tokens = _parse_header_line(header_line, delimiter)
    expected_fields = len(header_tokens)

    if expected_fields < 2:
        raise RuntimeError(f"CSV '{source_name}' does not contain a valid table header.")

    filtered_lines = [delimiter.join(header_tokens)]
    for raw_line in useful_lines[1:]:
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.count(delimiter) == expected_fields - 1:
            filtered_lines.append(line)

    if len(filtered_lines) <= 1:
        raise RuntimeError(f"CSV '{source_name}' contains a header but no usable data rows.")

    cleaned_text = "\n".join(filtered_lines)
    try:
        dataframe = pd.read_csv(io.StringIO(cleaned_text), sep=delimiter, engine="python")
    except Exception:
        dataframe = pd.read_csv(
            io.StringIO(cleaned_text),
            sep=delimiter,
            engine="python",
            quoting=csv.QUOTE_NONE,
            on_bad_lines="skip",
        )

    dataframe.columns = [str(column).strip().strip('"').strip("'") for column in dataframe.columns]
    if len(dataframe.columns) < 2:
        raise RuntimeError(f"CSV '{source_name}' does not contain a valid table.")
    return dataframe


def _is_probably_text_csv(data: bytes) -> bool:
    """Reject common binary containers before trying to parse as text CSV."""

    if not data:
        return False
    if data[:2] in {b"PK", b"\x1f\x8b"}:
        return False
    if data[:3] == b"CDF" or data[:8] == b"\x89HDF\r\n\x1a\n":
        return False
    if b"\x00" in data[:4096]:
        return False
    return True


def read_csv_robust(csv_path: Path) -> "pd.DataFrame":
    data = csv_path.read_bytes()
    if not _is_probably_text_csv(data):
        raise RuntimeError(f"CSV '{csv_path.name}' is not a plain text CSV file.")
    text = _decode_bytes_with_fallbacks(data, csv_path.name)
    return _read_csv_text_robust(text, csv_path.name)


def _read_excel_member(data: bytes, member_name: str) -> list["pd.DataFrame"]:
    try:
        sheets = pd.read_excel(io.BytesIO(data), sheet_name=None)
    except Exception as exc:
        raise RuntimeError(f"Could not read Excel member '{member_name}': {exc}") from exc

    frames: list[pd.DataFrame] = []
    for dataframe in sheets.values():
        if isinstance(dataframe, pd.DataFrame) and not dataframe.empty:
            frame = dataframe.copy()
            frame.columns = [str(column).strip() for column in frame.columns]
            frames.append(frame)

    if not frames:
        raise RuntimeError(f"Excel member '{member_name}' contains no usable sheets.")
    return frames


def _read_payload_member_tables(member_name: str, data: bytes) -> list["pd.DataFrame"]:
    suffix = Path(member_name).suffix.lower()

    if suffix == ".csv" or (not suffix and _is_probably_text_csv(data)):
        text = _decode_bytes_with_fallbacks(data, member_name)
        return [_read_csv_text_robust(text, member_name)]

    if suffix in {".xlsx", ".xls"}:
        return _read_excel_member(data, member_name)

    raise RuntimeError(f"Unsupported table member '{member_name}'.")


def _read_tables_from_cds_payload(payload_path: Path, reporter: ProgressReporter) -> list["pd.DataFrame"]:
    data = payload_path.read_bytes()

    if zipfile.is_zipfile(payload_path):
        return _read_tables_from_zip_payload(payload_path, reporter)

    if _is_probably_text_csv(data):
        reporter.log("Plain CSV payload detected.")
        text = _decode_bytes_with_fallbacks(data, payload_path.name)
        return [_read_csv_text_robust(text, payload_path.name)]

    if data[:2] == b"PK":
        reporter.log("Office Open XML payload detected; attempting Excel read.")
        return _read_excel_member(data, payload_path.name)

    raise RuntimeError(
        "CDS returned a payload that is neither a ZIP archive nor a plain CSV/Excel table."
    )


def _read_tables_from_zip_payload(payload_path: Path, reporter: ProgressReporter) -> list["pd.DataFrame"]:
    frames: list[pd.DataFrame] = []

    with zipfile.ZipFile(payload_path, "r") as archive:
        member_names = [
            name
            for name in archive.namelist()
            if not name.endswith("/") and Path(name).suffix.lower() in {".csv", ".xlsx", ".xls"}
        ]

        if not member_names:
            raise RuntimeError("The CDS ZIP payload does not contain any CSV/XLSX/XLS table members.")

        reporter.log("ZIP payload detected. Table members: " + ", ".join(member_names))
        for name in member_names:
            member_frames = _read_payload_member_tables(name, archive.read(name))
            for index, frame in enumerate(member_frames, start=1):
                sheet_text = f" sheet {index}" if len(member_frames) > 1 else ""
                reporter.log(
                    f"Read member '{name}'{sheet_text}: "
                    f"{len(frame):,} rows; columns: {', '.join(str(c) for c in frame.columns)}"
                )
            frames.extend(member_frames)

    return frames


# -----------------------------------------------------------------------------
# Column mapping and data standardisation
# -----------------------------------------------------------------------------

def _find_datetime_column(dataframe: "pd.DataFrame") -> Optional[str]:
    datetime_candidates = {normalize_name(item) for item in RAW_TO_SHORT_CANDIDATES["datetime"]}
    for column in dataframe.columns:
        if normalize_name(column) in datetime_candidates:
            return str(column)
    return None


def _find_normalized_column(dataframe: "pd.DataFrame", candidates: Iterable[str]) -> Optional[str]:
    normalized_to_actual = {normalize_name(column): str(column) for column in dataframe.columns}
    for candidate in candidates:
        actual = normalized_to_actual.get(normalize_name(candidate))
        if actual is not None:
            return actual
    return None


def _map_wide_columns(dataframe: "pd.DataFrame") -> "pd.DataFrame":
    working = dataframe.copy()
    datetime_column = _find_datetime_column(working)
    if datetime_column is None:
        raise RuntimeError("Could not identify a datetime column.")

    rename_map = {datetime_column: "datetime"}
    normalized_to_actual = {normalize_name(column): column for column in working.columns}

    for short_name, candidates in RAW_TO_SHORT_CANDIDATES.items():
        if short_name == "datetime":
            continue
        for candidate in candidates:
            actual = normalized_to_actual.get(normalize_name(candidate))
            if actual is not None:
                rename_map[actual] = short_name
                break

    working = working.rename(columns=rename_map)
    keep = [column for column in MERGED_COLUMNS if column in working.columns]
    if "datetime" not in keep:
        raise RuntimeError("Wide-format CSV could not be mapped to datetime.")
    return working[keep].copy()


def _map_long_format(dataframe: "pd.DataFrame") -> "pd.DataFrame":
    datetime_col = _find_normalized_column(dataframe, ["datetime", "valid_time", "time", "timestamp", "date"])
    variable_col = _find_normalized_column(dataframe, ["variable", "parameter", "var", "name"])
    value_col = _find_normalized_column(dataframe, ["value", "observation", "data"])

    if datetime_col is None or variable_col is None or value_col is None:
        raise RuntimeError("CSV is not in a recognised long format.")

    alias_map: dict[str, str] = {}
    for short_name, names in RAW_TO_SHORT_CANDIDATES.items():
        if short_name == "datetime":
            continue
        for name in names:
            alias_map[name] = short_name
            alias_map[normalize_name(name)] = short_name

    working = dataframe[[datetime_col, variable_col, value_col]].copy()
    working.columns = ["datetime", "variable", "value"]
    working["variable"] = working["variable"].map(
        lambda value: alias_map.get(str(value), alias_map.get(normalize_name(value), normalize_name(value)))
    )
    working = working[working["variable"].isin(["swh", "mwd", "mwp", "u10", "v10"])]

    wide = working.pivot_table(
        index="datetime",
        columns="variable",
        values="value",
        aggfunc="first",
    ).reset_index()
    wide.columns.name = None
    return wide


def standardize_dataframe(raw_dataframe: "pd.DataFrame") -> "pd.DataFrame":
    """Map CDS table columns to the internal compact column names."""

    try:
        wide = _map_wide_columns(raw_dataframe)
        if any(column in wide.columns for column in ["swh", "mwd", "mwp", "u10", "v10"]):
            return wide
    except Exception:
        pass
    return _map_long_format(raw_dataframe)


def _parse_datetime_series(series: "pd.Series") -> "pd.Series":
    try:
        parsed = pd.to_datetime(series, errors="coerce", format="mixed")
    except TypeError:
        parsed = pd.to_datetime(series, errors="coerce")

    try:
        if getattr(parsed.dt, "tz", None) is not None:
            parsed = parsed.dt.tz_convert(None)
    except Exception:
        pass
    return parsed


def _ensure_numeric(dataframe: "pd.DataFrame", columns: Iterable[str]) -> None:
    for column in columns:
        if column in dataframe.columns:
            dataframe[column] = pd.to_numeric(dataframe[column], errors="coerce")


def _meteorological_direction_from_uv(u10: "pd.Series", v10: "pd.Series") -> "pd.Series":
    return (180.0 + np.degrees(np.arctan2(u10, v10))) % 360.0


def _coalesce_duplicate_columns(dataframe: "pd.DataFrame") -> "pd.DataFrame":
    """Collapse duplicate columns by taking the first non-empty value per row."""

    result = pd.DataFrame(index=dataframe.index)
    for column in dataframe.columns:
        if column in result.columns:
            continue

        duplicate_block = dataframe.loc[:, dataframe.columns == column]
        if isinstance(duplicate_block, pd.Series):
            result[column] = duplicate_block
        elif duplicate_block.shape[1] == 1:
            result[column] = duplicate_block.iloc[:, 0]
        else:
            result[column] = duplicate_block.bfill(axis=1).iloc[:, 0]
    return result


def _standardize_and_index_member(
    raw_dataframe: "pd.DataFrame",
    source_label: str,
    reporter: ProgressReporter,
) -> "pd.DataFrame":
    standardized = standardize_dataframe(raw_dataframe)
    reporter.log(f"Mapped columns for processing ({source_label}): {', '.join(standardized.columns)}")

    standardized["datetime"] = _parse_datetime_series(standardized["datetime"])
    standardized = standardized.dropna(subset=["datetime"]).copy()
    _ensure_numeric(standardized, ["swh", "mwd", "mwp", "u10", "v10"])

    keep = [column for column in MERGED_COLUMNS if column in standardized.columns]
    if len(keep) <= 1:
        raise RuntimeError(f"Table member '{source_label}' does not contain recognised ERA5 variable columns.")

    standardized = standardized[keep].copy()
    standardized = standardized.sort_values("datetime").drop_duplicates(subset=["datetime"], keep="first")
    return standardized.set_index("datetime")


def _prepare_standardized_dataframe(
    raw_dataframe: "pd.DataFrame",
    source_label: str,
    reporter: ProgressReporter,
) -> "pd.DataFrame":
    standardized = standardize_dataframe(raw_dataframe)
    reporter.log(f"Mapped columns for processing ({source_label}): {', '.join(standardized.columns)}")

    standardized["datetime"] = _parse_datetime_series(standardized["datetime"])
    standardized = standardized.dropna(subset=["datetime"]).copy()
    _ensure_numeric(standardized, ["swh", "mwd", "mwp", "u10", "v10"])
    return standardized


def _require_non_empty_columns(dataframe: "pd.DataFrame", columns: Iterable[str], source_label: str) -> None:
    missing = [column for column in columns if column not in dataframe.columns]
    if missing:
        raise RuntimeError(
            f"The {source_label} CSV does not contain the required columns: {', '.join(missing)}. "
            "The CDS response did not include all expected wave/wind variables."
        )

    empty = [column for column in columns if int(dataframe[column].notna().sum()) == 0]
    if empty:
        raise RuntimeError(
            f"The {source_label} CSV contains only empty values for: {', '.join(empty)}. "
            "The run has stopped to avoid creating an incomplete output.csv."
        )


# -----------------------------------------------------------------------------
# File writing and lock checks
# -----------------------------------------------------------------------------

def _format_lock_message(path: Path, label: str) -> str:
    return (
        f"Permission denied while writing {label}: {path}. "
        "Close the file if it is open in Excel, LibreOffice, a text editor, "
        "Windows Preview pane or another process, then run the downloader again."
    )


def _check_output_path_is_writable(path: Path, label: str) -> None:
    """Detect locked output files before starting the CDS download."""

    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        test_path = path.parent / f".__write_test_{path.name}.tmp"
        with test_path.open("w", encoding="utf-8", newline="") as handle:
            handle.write("test\n")
        try:
            test_path.unlink()
        except FileNotFoundError:
            pass

        if path.exists():
            with path.open("a", encoding="utf-8", newline=""):
                pass
    except PermissionError as exc:
        raise RuntimeError(_format_lock_message(path, label)) from exc
    except OSError as exc:
        raise RuntimeError(f"Cannot write {label} at {path}: {exc}") from exc


def _preflight_output_files(config: Era5Config, reporter: ProgressReporter) -> None:
    reporter.status("Checking output files...")
    _check_output_path_is_writable(config.raw_csv_path, "era5_data.csv")
    _check_output_path_is_writable(config.output_csv_path, "output.csv")
    reporter.log("Output files are writable.")


def _write_dataframe_csv_safely(dataframe: "pd.DataFrame", path: Path, label: str) -> None:
    """Write CSV via a temporary file, then replace the final file."""

    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_name(f".{path.stem}.writing{path.suffix}")

    try:
        if temporary_path.exists():
            temporary_path.unlink()
        dataframe.to_csv(temporary_path, index=False, lineterminator="\n")
        temporary_path.replace(path)
    except PermissionError as exc:
        try:
            temporary_path.unlink(missing_ok=True)
        except Exception:
            pass
        raise RuntimeError(_format_lock_message(path, label)) from exc
    except OSError as exc:
        try:
            temporary_path.unlink(missing_ok=True)
        except Exception:
            pass
        raise RuntimeError(f"Could not write {label} at {path}: {exc}") from exc


# -----------------------------------------------------------------------------
# CDS request and processing pipeline
# -----------------------------------------------------------------------------

def _build_cds_request(config: Era5Config) -> dict[str, object]:
    return {
        "variable": ERA5_VARIABLES,
        "location": {"longitude": config.longitude, "latitude": config.latitude},
        "date": [f"{config.start_date}/{config.end_date}"],
        "data_format": "csv",
    }


def _download_cds_payload(
    client: "cdsapi.Client",
    config: Era5Config,
    payload_path: Path,
    reporter: ProgressReporter,
) -> Path:
    request = _build_cds_request(config)
    reporter.status("Downloading ERA5 data from CDS...")
    reporter.progress(1, TOTAL_PROGRESS_STEPS, "Downloading CDS payload")
    reporter.log("Requested variables: " + ", ".join(ERA5_VARIABLES))
    reporter.log(f"Target point: lon={config.longitude:.8f}, lat={config.latitude:.8f}")
    reporter.log(f"Date range: {config.start_date} to {config.end_date}")

    if payload_path.exists():
        payload_path.unlink()

    result = client.retrieve(DATASET_NAME, request)
    result.download(str(payload_path))

    if not payload_path.exists() or payload_path.stat().st_size == 0:
        raise RuntimeError(f"{payload_path.name} was not created on disk.")

    reporter.log(f"Downloaded CDS response to: {payload_path}")
    return payload_path


def _normalize_cds_payload_to_merged_csv(
    payload_path: Path,
    merged_csv_path: Path,
    reporter: ProgressReporter,
) -> Path:
    reporter.status("Reading and merging CDS payload...")
    reporter.progress(2, TOTAL_PROGRESS_STEPS, "Reading CDS tables and merging by datetime")

    raw_frames = _read_tables_from_cds_payload(payload_path, reporter)
    if not raw_frames:
        raise RuntimeError("No usable tables were found in the CDS payload.")

    merged_indexed: Optional[pd.DataFrame] = None
    used_members = 0
    skipped_messages: list[str] = []

    for index, raw_dataframe in enumerate(raw_frames, start=1):
        label = f"CDS table {index}"
        try:
            part = _standardize_and_index_member(raw_dataframe, label, reporter)
        except Exception as exc:
            skipped_messages.append(f"{label}: {exc}")
            continue

        used_members += 1
        if merged_indexed is None:
            merged_indexed = part
        else:
            merged_indexed = _coalesce_duplicate_columns(merged_indexed.combine_first(part))

    for message in skipped_messages:
        reporter.log("Skipped payload table: " + message)

    if merged_indexed is None or merged_indexed.empty:
        raise RuntimeError("No recognised ERA5 variables could be extracted from the CDS payload.")

    merged_dataframe = merged_indexed.reset_index().sort_values("datetime").reset_index(drop=True)
    for column in ["swh", "mwd", "mwp", "u10", "v10"]:
        if column not in merged_dataframe.columns:
            merged_dataframe[column] = np.nan

    merged_dataframe = merged_dataframe[MERGED_COLUMNS].copy()
    merged_dataframe["datetime"] = pd.to_datetime(
        merged_dataframe["datetime"],
        errors="coerce",
    ).dt.strftime("%Y-%m-%d %H:%M:%S")

    _write_dataframe_csv_safely(merged_dataframe, merged_csv_path, "era5_data.csv")
    reporter.log(f"Merged {used_members} CDS table(s) into: {merged_csv_path}")
    reporter.log("Non-empty values in era5_data.csv: " + _non_null_summary(merged_dataframe))
    return merged_csv_path


def download_raw_csv(config: Era5Config, reporter: ProgressReporter) -> Path:
    reporter.status("Initializing CDS client...")
    client = initialize_cds_client()
    reporter.log("CDS API client initialized.")
    program_path = Path(sys.executable).resolve() if getattr(sys, "frozen", False) else Path(__file__).resolve()
    reporter.log(f"Program file: {program_path}")
    reporter.log(f"Application/output directory: {SCRIPT_DIR}")
    reporter.log(f"Dataset: {DATASET_NAME}")

    try:
        _download_cds_payload(client, config, TEMP_PAYLOAD, reporter)
        _normalize_cds_payload_to_merged_csv(TEMP_PAYLOAD, config.raw_csv_path, reporter)
    finally:
        try:
            if TEMP_PAYLOAD.exists():
                TEMP_PAYLOAD.unlink()
                reporter.log(f"Removed temporary CDS payload: {TEMP_PAYLOAD}")
        except OSError as exc:
            reporter.log(f"Could not remove temporary CDS payload {TEMP_PAYLOAD}: {exc}")

    if not config.raw_csv_path.exists() or config.raw_csv_path.stat().st_size == 0:
        raise RuntimeError("era5_data.csv was not created on disk.")

    return config.raw_csv_path


def _non_null_summary(dataframe: "pd.DataFrame") -> str:
    columns = [column for column in ["swh", "mwd", "mwp", "u10", "v10"] if column in dataframe.columns]
    return ", ".join(f"{column}={int(dataframe[column].notna().sum())}" for column in columns)


def build_output_csv(config: Era5Config, reporter: ProgressReporter) -> Path:
    reporter.status("Building output.csv...")
    reporter.progress(3, TOTAL_PROGRESS_STEPS, "Reading era5_data.csv and writing output.csv")
    reporter.log(f"Reading merged CSV: {config.raw_csv_path}")

    raw_dataframe = read_csv_robust(config.raw_csv_path)
    standardized = _prepare_standardized_dataframe(raw_dataframe, "merged ERA5 CSV", reporter)
    _require_non_empty_columns(standardized, ["swh", "mwd", "mwp", "u10", "v10"], "merged ERA5")

    standardized["wind"] = np.sqrt(standardized["u10"] ** 2 + standardized["v10"] ** 2)
    standardized["dwi"] = _meteorological_direction_from_uv(standardized["u10"], standardized["v10"])

    for column in OUTPUT_COLUMNS:
        if column not in standardized.columns:
            standardized[column] = np.nan

    output_dataframe = standardized[OUTPUT_COLUMNS].copy()
    output_dataframe = output_dataframe.sort_values("datetime").drop_duplicates(
        subset=["datetime"],
        keep="first",
    ).reset_index(drop=True)
    output_dataframe["datetime"] = output_dataframe["datetime"].dt.strftime("%Y-%m-%d %H:%M:%S")

    _write_dataframe_csv_safely(output_dataframe, config.output_csv_path, "output.csv")

    if not config.output_csv_path.exists() or config.output_csv_path.stat().st_size == 0:
        raise RuntimeError("output.csv was not created on disk.")

    reporter.log("Non-empty values in output.csv: " + _non_null_summary(output_dataframe))
    reporter.log(f"Saved processed CSV to: {config.output_csv_path}")
    return config.output_csv_path


def execute_pipeline(
    config: Era5Config,
    callback: Optional[ProgressCallback] = None,
) -> Path:
    setup_logging(config.log_file)
    reporter = ProgressReporter(callback)
    reporter.log("Starting ERA5 single-point workflow.")
    _preflight_output_files(config, reporter)
    download_raw_csv(config, reporter)
    output_path = build_output_csv(config, reporter)
    reporter.progress(TOTAL_PROGRESS_STEPS, TOTAL_PROGRESS_STEPS, "Completed")
    return output_path


# -----------------------------------------------------------------------------
# Graphical interface
# -----------------------------------------------------------------------------

class TkTextWriter:
    """Small file-like adapter for writing text into a Tkinter text widget."""

    def __init__(self, text_widget: "tk.Text") -> None:
        self.text_widget = text_widget

    def write(self, text: object) -> int:
        message = str(text)
        if not message:
            return 0
        try:
            self.text_widget.configure(state="normal")
            self.text_widget.insert("end", message)
            self.text_widget.see("end")
            self.text_widget.configure(state="disabled")
        except Exception:
            return 0
        return len(message)

    def flush(self) -> None:
        try:
            self.text_widget.update_idletasks()
        except Exception:
            pass


class Era5DownloaderGUI:
    """Tkinter GUI wrapper around the ERA5 processing pipeline."""

    def __init__(self, root: "tk.Tk") -> None:
        self.root = root
        self.root.title("ERA5 Single-Point CSV Downloader")
        self.root.geometry(f"{DEFAULT_WINDOW_WIDTH}x{DEFAULT_WINDOW_HEIGHT}")
        self.root.minsize(DEFAULT_WINDOW_MIN_WIDTH, DEFAULT_WINDOW_MIN_HEIGHT)
        self.root.maxsize(DEFAULT_WINDOW_WIDTH, DEFAULT_WINDOW_HEIGHT)
        self.root.resizable(False, False)

        self.message_queue: queue.Queue[tuple[str, dict[str, object]]] = queue.Queue()
        self.worker_thread: Optional[threading.Thread] = None
        self.run_start_monotonic: Optional[float] = None

        self.longitude_var = tk.StringVar(value=f"{INITIAL_LONGITUDE:.8f}")
        self.latitude_var = tk.StringVar(value=f"{INITIAL_LATITUDE:.8f}")
        self.start_date_var = tk.StringVar(value=INITIAL_START_DATE)
        self.end_date_var = tk.StringVar(value=INITIAL_END_DATE)
        self.status_var = tk.StringVar(value="Ready.")
        self.progress_label_var = tk.StringVar(value="Idle")
        self.eta_label_var = tk.StringVar(value="Estimated completion time: not available yet.")

        self._configure_style()
        self._build_header()
        self._build_body()
        self._build_footer()
        self._append_log("Log window ready.")
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)
        self.root.after(150, self._poll_messages)

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

    @staticmethod
    def _freeze_widget_size(widget: object) -> None:
        """Prevent child widgets from changing the allocated widget size."""

        for method_name in ("pack_propagate", "grid_propagate"):
            method = getattr(widget, method_name, None)
            if callable(method):
                try:
                    method(False)
                except Exception:
                    pass

    def _build_header(self) -> None:
        header = tk.Frame(self.root, bg="#1f3b5b", padx=16, pady=12, height=HEADER_HEIGHT)
        header.pack(fill="x")
        self._freeze_widget_size(header)

        tk.Label(
            header,
            text="ERA5 Single-Point CSV Downloader",
            bg="#1f3b5b",
            fg="white",
            font=("Segoe UI", 18, "bold"),
            anchor="w",
        ).pack(fill="x")
        tk.Label(
            header,
            text=(
                "Single CDS request; ZIP/CSV responses are merged automatically. "
                "Outputs are written in the script directory."
            ),
            bg="#1f3b5b",
            fg="#d7e6f5",
            font=("Segoe UI", 10),
            anchor="w",
            justify="left",
        ).pack(fill="x", pady=(4, 0))

    def _build_body(self) -> None:
        outer = ttk.Frame(
            self.root,
            padding=12,
            width=BODY_FRAME_WIDTH,
            height=BODY_FRAME_HEIGHT,
        )
        outer.pack(fill="both", expand=False)
        self._freeze_widget_size(outer)

        self.notebook = ttk.Notebook(
            outer,
            width=BODY_FRAME_WIDTH - 24,
            height=BODY_FRAME_HEIGHT - 24,
        )
        self.notebook.pack(fill="both", expand=False)
        self._freeze_widget_size(self.notebook)

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
        self.run_tab.columnconfigure(0, minsize=RUN_LEFT_WIDTH, weight=0)
        self.run_tab.columnconfigure(1, minsize=RUN_RIGHT_WIDTH, weight=0)
        self.run_tab.rowconfigure(0, minsize=RUN_PANEL_HEIGHT, weight=0)

        left = ttk.Frame(self.run_tab, width=RUN_LEFT_WIDTH, height=RUN_PANEL_HEIGHT)
        right = ttk.Frame(self.run_tab, width=RUN_RIGHT_WIDTH, height=RUN_PANEL_HEIGHT)
        left.grid(row=0, column=0, sticky="nw", padx=(0, 8))
        right.grid(row=0, column=1, sticky="nw", padx=(8, 0))
        self._freeze_widget_size(left)
        self._freeze_widget_size(right)
        left.columnconfigure(0, minsize=RUN_LEFT_WIDTH, weight=0)
        right.columnconfigure(0, minsize=RUN_RIGHT_WIDTH, weight=0)

        self._build_point_card(left)
        self._build_action_card(right)
        self._build_progress_card(right)

    def _build_point_card(self, parent: "ttk.Frame") -> None:
        point_card = ttk.LabelFrame(
            parent,
            text="Target point and date range",
            style="Card.TLabelframe",
            width=POINT_CARD_WIDTH,
            height=POINT_CARD_HEIGHT,
        )
        point_card.grid(row=0, column=0, sticky="nw")
        self._freeze_widget_size(point_card)
        point_card.columnconfigure(1, minsize=300, weight=0)

        fields = [
            ("Longitude", self.longitude_var),
            ("Latitude", self.latitude_var),
            ("Start date", self.start_date_var),
            ("End date", self.end_date_var),
        ]
        for row_index, (label, variable) in enumerate(fields):
            ttk.Label(point_card, text=label).grid(row=row_index, column=0, sticky="w", padx=(0, 8), pady=6)
            ttk.Entry(point_card, textvariable=variable, width=24).grid(
                row=row_index,
                column=1,
                sticky="w",
                pady=6,
            )

        ttk.Label(
            point_card,
            text="Files are fixed as era5_data.csv, output.csv, download_era5_data.log and defaults.json.",
            style="Small.TLabel",
            wraplength=500,
            justify="left",
        ).grid(row=4, column=0, columnspan=2, sticky="w", pady=(8, 2))

    def _build_action_card(self, parent: "ttk.Frame") -> None:
        action_card = ttk.LabelFrame(
            parent,
            text="Actions",
            style="Card.TLabelframe",
            width=ACTION_CARD_WIDTH,
            height=ACTION_CARD_HEIGHT,
        )
        action_card.grid(row=0, column=0, sticky="nw")
        self._freeze_widget_size(action_card)
        action_card.columnconfigure(0, minsize=ACTION_CARD_WIDTH - 28, weight=0)

        self.start_button = ttk.Button(
            action_card,
            text="Start",
            style="Primary.TButton",
            command=self.start_run,
        )
        self.start_button.grid(row=0, column=0, sticky="ew", pady=(0, 8))
        ttk.Button(
            action_card,
            text="Open Log tab",
            command=lambda: self.notebook.select(self.log_tab),
        ).grid(row=1, column=0, sticky="ew", pady=4)
        ttk.Button(action_card, text="Quit", command=self.root.destroy).grid(
            row=2,
            column=0,
            sticky="ew",
            pady=4,
        )

    def _build_progress_card(self, parent: "ttk.Frame") -> None:
        progress_card = ttk.LabelFrame(
            parent,
            text="Progress",
            style="Card.TLabelframe",
            width=PROGRESS_CARD_WIDTH,
            height=PROGRESS_CARD_HEIGHT,
        )
        progress_card.grid(row=1, column=0, sticky="nw", pady=(12, 0))
        self._freeze_widget_size(progress_card)

        self.progress_bar = ttk.Progressbar(
            progress_card,
            mode="determinate",
            maximum=100,
            length=PROGRESS_CARD_WIDTH - 34,
        )
        self.progress_bar.pack(fill="x", pady=(0, 8))
        ttk.Label(
            progress_card,
            textvariable=self.progress_label_var,
            justify="left",
            wraplength=PROGRESS_CARD_WIDTH - 34,
        ).pack(fill="x")
        ttk.Label(
            progress_card,
            textvariable=self.eta_label_var,
            justify="left",
            wraplength=PROGRESS_CARD_WIDTH - 34,
        ).pack(fill="x", pady=(6, 0))

    def _build_log_tab(self) -> None:
        frame = ttk.LabelFrame(
            self.log_tab,
            text="Execution log",
            style="Card.TLabelframe",
            width=LOG_FRAME_WIDTH,
            height=LOG_FRAME_HEIGHT,
        )
        frame.pack(fill="both", expand=False)
        self._freeze_widget_size(frame)

        self.log_box = scrolledtext.ScrolledText(
            frame,
            wrap="word",
            font=("Consolas", 11),
            padx=12,
            pady=12,
            borderwidth=0,
            relief="flat",
            background="white",
            width=LOG_BOX_WIDTH_CHARS,
            height=LOG_BOX_HEIGHT_LINES,
        )
        self.log_box.pack(fill="both", expand=True)
        self.log_box.configure(state="disabled")
        self.log_writer = TkTextWriter(self.log_box)

    def _build_instructions_tab(self) -> None:
        frame = ttk.LabelFrame(
            self.instructions_tab,
            text="Usage",
            style="Card.TLabelframe",
            width=INSTRUCTIONS_FRAME_WIDTH,
            height=INSTRUCTIONS_FRAME_HEIGHT,
        )
        frame.pack(fill="both", expand=False)
        self._freeze_widget_size(frame)

        box = scrolledtext.ScrolledText(
            frame,
            wrap="word",
            font=("Consolas", 11),
            padx=12,
            pady=12,
            borderwidth=0,
            relief="flat",
            background="white",
            width=INSTRUCTIONS_BOX_WIDTH_CHARS,
            height=INSTRUCTIONS_BOX_HEIGHT_LINES,
        )
        box.pack(fill="both", expand=True)
        box.insert("1.0", INSTRUCTIONS_TEXT)
        box.configure(state="disabled")

    def _build_footer(self) -> None:
        footer = ttk.Frame(self.root, padding=(12, 0, 12, 12), height=FOOTER_HEIGHT)
        footer.pack(fill="x", expand=False)
        self._freeze_widget_size(footer)
        ttk.Label(
            footer,
            textvariable=self.status_var,
            wraplength=FOOTER_WRAP_LENGTH,
            justify="left",
        ).pack(fill="x")

    def _append_log(self, message: str) -> None:
        if not hasattr(self, "log_box"):
            return
        writer = getattr(self, "log_writer", None)
        if writer is not None and hasattr(writer, "write"):
            writer.write(message.rstrip() + "\n")
            writer.flush()
            return

        self.log_box.configure(state="normal")
        self.log_box.insert("end", message.rstrip() + "\n")
        self.log_box.see("end")
        self.log_box.configure(state="disabled")

    def _collect_defaults(self) -> dict[str, str]:
        return {
            "longitude": self.longitude_var.get().strip(),
            "latitude": self.latitude_var.get().strip(),
            "start_date": self.start_date_var.get().strip(),
            "end_date": self.end_date_var.get().strip(),
        }

    def _on_close(self) -> None:
        try:
            save_defaults(self._collect_defaults())
        except OSError:
            pass
        self.root.destroy()

    def _build_config(self) -> Era5Config:
        longitude = float(self.longitude_var.get().strip())
        latitude = float(self.latitude_var.get().strip())
        start_date = self.start_date_var.get().strip()
        end_date = self.end_date_var.get().strip()

        datetime.fromisoformat(start_date)
        datetime.fromisoformat(end_date)
        if start_date > end_date:
            raise ValueError("Start date must be less than or equal to end date.")

        save_defaults(self._collect_defaults())
        return Era5Config(
            longitude=longitude,
            latitude=latitude,
            start_date=start_date,
            end_date=end_date,
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

        self._prepare_gui_for_run(config)

        def worker() -> None:
            try:
                output_path = execute_pipeline(
                    config,
                    callback=lambda event, payload: self.message_queue.put((event, payload)),
                )
                self.message_queue.put(("done", {"output_path": str(output_path)}))
            except Exception as exc:
                self.message_queue.put(("error", {"message": str(exc)}))

        self.worker_thread = threading.Thread(target=worker, daemon=True)
        self.worker_thread.start()

    def _prepare_gui_for_run(self, config: Era5Config) -> None:
        self._append_log("=" * 72)
        self._append_log("Preparing new run...")
        self._append_log(
            f"Target (lon, lat): ({config.longitude:.8f}, {config.latitude:.8f}) | "
            f"Dates: {config.start_date} to {config.end_date}"
        )
        self.progress_bar["value"] = 0
        self.progress_label_var.set("Starting...")
        self.eta_label_var.set("Estimated completion time: calculating after the first completed step...")
        self.status_var.set("Running.")
        self.start_button.state(["disabled"])
        self.run_start_monotonic = time.monotonic()

    def _format_eta_text(self, current: int, total: int) -> str:
        if self.run_start_monotonic is None or current <= 0 or total <= 0:
            return "Estimated completion time: not available yet."

        elapsed = max(0.0, time.monotonic() - self.run_start_monotonic)
        average_step_time = elapsed / float(current)
        remaining = average_step_time * max(0, total - current)
        eta = datetime.fromtimestamp(time.time() + remaining).strftime("%H:%M:%S")
        return f"Estimated completion time: {eta}"

    def _poll_messages(self) -> None:
        try:
            while True:
                event, payload = self.message_queue.get_nowait()
                self._handle_worker_message(event, payload)
        except queue.Empty:
            pass
        finally:
            self.root.after(150, self._poll_messages)

    def _handle_worker_message(self, event: str, payload: dict[str, object]) -> None:
        if event == "log":
            self._append_log(str(payload.get("message", "")))
        elif event == "status":
            self.status_var.set(str(payload.get("message", "")))
        elif event == "progress":
            self._handle_progress_message(payload)
        elif event == "done":
            self._handle_done_message(payload)
        elif event == "error":
            self._handle_error_message(payload)

    def _handle_progress_message(self, payload: dict[str, object]) -> None:
        current = max(0, int(payload.get("current", 0)))
        total = max(1, int(payload.get("total", 1)))
        self.progress_bar["value"] = max(0.0, min(100.0, current * 100.0 / total))
        self.progress_label_var.set(str(payload.get("message", f"{current}/{total}")))
        self.eta_label_var.set(self._format_eta_text(current, total))

    def _handle_done_message(self, payload: dict[str, object]) -> None:
        self.progress_bar["value"] = 100
        self.progress_label_var.set("Completed")
        self.eta_label_var.set("Estimated completion time: completed.")
        self.status_var.set(f"Completed successfully. output.csv written to: {payload.get('output_path', '')}")
        self.start_button.state(["!disabled"])
        self.notebook.select(self.log_tab)

    def _handle_error_message(self, payload: dict[str, object]) -> None:
        message = str(payload.get("message", "Unknown error."))
        self.status_var.set(f"Run failed: {message}")
        self.progress_label_var.set("Failed")
        self.eta_label_var.set("Estimated completion time: unavailable due to failure.")
        self.start_button.state(["!disabled"])
        self._append_log("ERROR: " + message)
        self.notebook.select(self.log_tab)


# -----------------------------------------------------------------------------
# Command-line interface
# -----------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="ERA5 single-point time-series downloader")
    parser.add_argument("--gui", action="store_true", help="Launch GUI mode.")
    parser.add_argument("--longitude", type=float, default=INITIAL_LONGITUDE)
    parser.add_argument("--latitude", type=float, default=INITIAL_LATITUDE)
    parser.add_argument("--start-date", default=INITIAL_START_DATE)
    parser.add_argument("--end-date", default=INITIAL_END_DATE)
    return parser


def _validate_cli_dates(start_date: str, end_date: str) -> None:
    datetime.fromisoformat(start_date)
    datetime.fromisoformat(end_date)
    if start_date > end_date:
        raise ValueError("Start date must be less than or equal to end date.")


def run_cli(args: argparse.Namespace) -> int:
    start_date = str(args.start_date)
    end_date = str(args.end_date)
    _validate_cli_dates(start_date, end_date)

    config = Era5Config(
        longitude=float(args.longitude),
        latitude=float(args.latitude),
        start_date=start_date,
        end_date=end_date,
    )
    save_defaults(
        {
            "longitude": f"{config.longitude:.8f}",
            "latitude": f"{config.latitude:.8f}",
            "start_date": config.start_date,
            "end_date": config.end_date,
        }
    )

    def cli_callback(event: str, payload: dict[str, object]) -> None:
        if event in {"log", "status"}:
            print(payload.get("message", ""))
        elif event == "progress":
            print(f"[{payload.get('current', 0)}/{payload.get('total', 0)}] {payload.get('message', '')}")

    output_path = execute_pipeline(config, callback=cli_callback)
    print(f"Completed successfully. output.csv written to: {output_path}")
    return 0


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    wants_gui = args.gui or len(sys.argv) == 1

    validate_runtime_dependencies(wants_gui=wants_gui)

    if wants_gui:
        root = tk.Tk()
        Era5DownloaderGUI(root)
        root.mainloop()
        return 0

    return run_cli(args)


if __name__ == "__main__":
    raise SystemExit(main())
