# ERA5 Hourly Data Downloader and Extractor

## Overview

`download_era5_data.py` downloads and/or extracts hourly ERA5 single-level data from ECMWF GRIB files for a fixed target point offshore Leixões, Porto, Portugal.

The current implementation is based on **`xarray` + `cfgrib`**, not `pygrib`. It supports two operating modes:

1. **Download & Process**
   - Downloads ERA5 data from the CDS API in **monthly GRIB files**.
   - Processes each downloaded GRIB file immediately after download.
   - Appends the extracted point time series to a CSV file.

2. **Extract Only**
   - Skips downloading.
   - Reads all GRIB files already present in the local `grib/` directory.
   - Processes files in parallel using `ProcessPoolExecutor` with a progress bar.

In both modes, the script extracts a predefined set of oceanographic and meteorological variables, interpolates them to the exact target coordinate using **Inverse Distance Weighting (IDW)**, and writes the merged time series to:

```text
results/download_era5_data.csv
```

The output is always sorted by `datetime` in ascending order.

---

## Current Implementation Summary

This README reflects the current code behavior implemented in `download_era5_data.py`.

### Target point

The script is configured for the following fixed location:

- **Location:** Leixões Oceanic Buoy, Porto, Portugal
- **Latitude:** `41.14833299`
- **Longitude:** `-9.581666670`

### Time span used for downloads

When running in **Option 1**, the script downloads data for:

- **Start year:** `2000`
- **End year:** `1940`
- **Frequency:** hourly
- **Chunking:** monthly GRIB files

When running in **Option 2**, the script **ignores** `START_YEAR` and `END_YEAR` and simply processes every GRIB file found in the `grib/` directory.

---

## Variables Extracted

The script uses a central `VARIABLES` dictionary containing request codes plus matching metadata for extraction.

| Output key | Description | Stored request code | Matching aliases / identifiers |
|---|---|---:|---|
| `swh` | Significant height of combined wind waves and swell | `229.140` | alias `swh`, parameter number `229`, parameter id `140229` |
| `mwd` | Mean wave direction | `230.140` | alias `mwd`, parameter number `230`, parameter id `140230` |
| `pp1d` | Peak wave period | `231.140` | alias `pp1d`, parameter number `231`, parameter id `140231` |
| `wind` | 10 metre wind speed | `245.140` | alias `wind`, parameter number `245`, parameter id `140245` |
| `dwi` | 10 metre wind direction | `249.140` | alias `dwi`, parameter number `249`, parameter id `140249` |

The final CSV columns are:

```text
datetime,swh,mwd,pp1d,wind,dwi
```

---

## Directory Structure

The script automatically creates the following directories if they do not already exist:

```text
grib/
results/
```

### Files produced by the script

- **Downloaded monthly GRIB files:**
  ```text
  grib/ERA5_YYYY_MM.grib
  ```
- **Merged CSV output:**
  ```text
  results/download_era5_data.csv
  ```
- **Log file:**
  ```text
  download_era5_data.log
  ```

---

## Core Workflow

### 1. Download stage

In **Option 1**, the script:

- initializes a CDS API client,
- loops through all years and months in the configured range,
- downloads one GRIB file per month,
- retries failed downloads up to `MAX_RETRIES = 3`,
- waits between attempts using an increasing delay,
- and processes each file immediately after a successful download.

### Download request settings

The current code uses:

- dataset: `reanalysis-era5-single-levels`
- `product_type = reanalysis`
- `format = grib`
- `variable = [spec['request_code'] for spec in VARIABLES.values()]`
- full list of days for the month
- all 24 hourly timestamps from `00:00` to `23:00`
- spatial subsetting with `area`
- regridding with `grid`

### Spatial request window

The script builds a small extraction window around the target point using:

- `BUFFER = 0.25` degrees
- `AREA = [NORTH, WEST, SOUTH, EAST]`
- `GRID = [0.25, 0.25]`

With the configured latitude and longitude, the requested bounding box is:

| Parameter | Value |
|---|---:|
| North | `41.39833299` |
| South | `40.89833299` |
| East | `-9.33166667` |
| West | `-9.83166667` |

This means the script requests a very small local ERA5 grid around the point of interest and then interpolates to the exact buoy coordinate.

---

### 2. GRIB reading stage

The extraction stage uses **`cfgrib.open_datasets(...)`** rather than assuming that a GRIB file contains a single homogeneous data cube.

This is important because heterogeneous GRIB files often contain multiple internal groups with different metadata or dimensional structure.

### Current `cfgrib` behavior used by the script

The script opens GRIB files with:

- `indexpath=''` to disable `.idx` sidecar files,
- `cache_geo_coords=True`,
- `read_keys=['shortName', 'cfVarName', 'paramId', 'parameterNumber']`.

This keeps the extraction logic close to pure file-based processing without leaving cfgrib index files behind.

---

### 3. Variable identification logic

The script does **not** rely on only one GRIB key. Instead, it tries to identify each variable robustly by building a set of candidate strings from:

- xarray data variable name,
- `GRIB_shortName` / `shortName`,
- `GRIB_cfVarName` / `cfVarName`,
- `GRIB_name`, `long_name`, `standard_name`,
- `GRIB_parameterNumber` / `parameterNumber`,
- `GRIB_paramId` / `paramId`.

It then compares those candidates to the accepted identifiers defined in `VARIABLES`:

- alias,
- request code,
- parameter number,
- parameter id.

This makes the extraction more tolerant to differences in how `cfgrib` exposes metadata across files.

---

### 4. Coordinate handling

The script includes several defensive routines to make extraction more robust.

### Coordinate name detection

It looks for latitude and longitude using common names such as:

- `latitude`, `lat`
- `longitude`, `lon`

It can also deal with dimensions such as `x` and `y` where needed.

### Longitude normalization

Some GRIB datasets use longitudes in:

- `-180 .. 180`, while others use
- `0 .. 360`.

The script automatically normalizes the target longitude to match the dataset convention before interpolation.

### Dimensional reduction

The function `_reduce_dataarray_to_time_lat_lon(...)` reduces each xarray `DataArray` to the expected structure:

- `[time, latitude, longitude]`, or
- `[latitude, longitude]`

Singleton dimensions are squeezed out. Unexpected non-singleton dimensions are reduced by selecting the first index, with that event recorded in the log.

---

### 5. Time handling

Time extraction is also defensive.

The script prefers:

1. `valid_time`
2. `time`

If the variable itself does not expose those coordinates, it falls back to the parent dataset.

Returned timestamps are converted with:

```python
pd.to_datetime(..., errors='coerce', format='mixed')
```

When the number of timestamps and the number of interpolated values do not match exactly, the script attempts to realign them by:

- repeating a single timestamp if needed,
- repeating a single value if needed,
- or truncating both arrays to the minimum common length.

---

### 6. IDW interpolation method

The script interpolates from the ERA5 grid to the exact target point using **Inverse Distance Weighting (IDW)** with:

```text
IDW_POWER = 2
```

### IDW behavior implemented in the code

1. The script builds latitude/longitude grids from the dataset coordinates.
2. It computes planar distance in degree space from each grid point to the target point.
3. If the target point coincides exactly with a grid point (`distance < 1e-12`), the script returns that grid-point value directly.
4. Otherwise, it computes standard IDW weights:

```text
weight = 1 / distance^power
```

5. Only finite values contribute to the interpolation.
6. The resulting interpolated value is stored for each time step.

This is a pragmatic point-extraction approach for the small local grid requested by the script.

---

### 7. Parallel processing in Extract Only mode

In **Option 2**, the script processes all local GRIB files in parallel using:

- `ProcessPoolExecutor`
- `max_workers = os.cpu_count() or 1`
- a progress bar from `tqdm`

Each file is processed with a timeout of:

```text
TIMEOUT_PER_FILE = 180 seconds
```

If a file exceeds that limit, the future is cancelled and the timeout event is written to the log.

The script also forces the multiprocessing start method to:

```python
spawn
```

when run as the main program. This is explicitly intended to improve compatibility with C-library-based packages on Windows.

---

### 8. CSV generation logic

### Option 1: Download & Process

In Option 1, the script:

- creates the CSV header if the file does not yet exist,
- appends each monthly processed DataFrame to the CSV,
- and at the end re-reads the full CSV, sorts by `datetime`, and writes it back.

### Option 2: Extract Only

In Option 2, the script:

- deletes any existing output CSV before processing,
- processes all GRIB files found locally,
- concatenates all extracted DataFrames,
- converts `datetime` to pandas datetime,
- sorts the merged result,
- and writes the final CSV once.

---

## Configuration Parameters

The key configurable constants in the current code are:

| Name | Value | Meaning |
|---|---:|---|
| `LONGITUDE` | `-9.581666670` | Target longitude |
| `LATITUDE` | `41.14833299` | Target latitude |
| `START_YEAR` | `1940` | First year used in Option 1 |
| `END_YEAR` | `2025` | Last year used in Option 1 |
| `BUFFER` | `0.25` | Bounding-box half-size in degrees |
| `GRID` | `[0.25, 0.25]` | Requested grid resolution |
| `REQUEST_DELAY` | `60` | Delay between download attempts / monthly requests |
| `MAX_RETRIES` | `3` | Maximum number of download attempts |
| `IDW_POWER` | `2` | Inverse distance weighting exponent |
| `TIMEOUT_PER_FILE` | `180` | Timeout per GRIB file in Option 2 |
| `GRIB_EXTENSIONS` | `.grib`, `.grib2`, `.grb`, `.grb2` | File extensions accepted in local processing |
| `LOG_FILE` | `download_era5_data.log` | Log file path |

---

## Requirements

### Python packages

The current script requires these Python packages:

- `numpy`
- `pandas`
- `tqdm`
- `xarray`
- `cfgrib`
- `eccodes`
- `cdsapi` *(only required if you use Option 1)*

A straightforward installation is:

```bash
pip install numpy pandas tqdm xarray cfgrib eccodes cdsapi
```

If you only want to process local GRIB files and do not need downloading, the script can still run without `cdsapi`, because that import is optional in the code. However, `xarray`, `cfgrib`, and `eccodes` remain essential for GRIB extraction.

---

## CDS API setup

To use **Option 1**, you need CDS API credentials.

1. Create an account at the Copernicus Climate Data Store.
2. Obtain your API credentials.
3. Create a `.cdsapirc` file in your home directory.

Typical content:

```text
url: https://cds.climate.copernicus.eu/api/v2
key: <YOUR_UID>:<YOUR_API_KEY>
```

Replace `<YOUR_UID>` and `<YOUR_API_KEY>` with your own credentials.

---

## How to Run

Run the script from the project directory:

```bash
python download_era5_data.py
```

At runtime, the script prompts:

```text
SELECT YOUR OPTION:
1) Download ERA5 data from CDS API and process GRIB files;
2) Only extract data from existing GRIB files.
Choose (1 or 2):
```

### Option 1 — Download and process

Choose `1` when you want the script to:

- download monthly ERA5 GRIB files,
- process each downloaded file,
- and write the final CSV output.

### Option 2 — Process existing GRIB files only

Choose `2` when you already have GRIB files in `grib/` and only want extraction.

This mode:

- ignores the configured year range,
- scans the `grib/` directory for supported file extensions,
- processes files in parallel,
- and rebuilds the output CSV from scratch.

---

## Output Format

The resulting CSV contains one row per timestamp and the following fields:

| Column | Meaning |
|---|---|
| `datetime` | Timestamp of the extracted ERA5 data |
| `swh` | Significant wave height |
| `mwd` | Mean wave direction |
| `pp1d` | Peak wave period |
| `wind` | 10 metre wind speed |
| `dwi` | 10 metre wind direction |

Missing or non-finite values are written as empty cells / null-equivalent CSV entries.

---

## Logging and Diagnostics

The script writes a log file named:

```text
download_era5_data.log
```

The log records:

- CDS client initialization issues,
- download attempts and retries,
- skipped or unrecognized variables,
- dimension-reduction events,
- file-processing timeouts,
- GRIB-opening errors,
- CSV sorting issues,
- and total processing time.

At the end of execution, the script prints total runtime to the console.

---

## Important Notes

### 1. This is no longer a `pygrib` workflow

The current script is based on:

- `xarray`
- `cfgrib`
- `eccodes`

Any older documentation referring to `pygrib` is outdated for this codebase.

### 2. The script is designed for a fixed point

The target latitude and longitude are hard-coded. To use another site, change:

- `LATITUDE`
- `LONGITUDE`

and, if desired, also adjust:

- `BUFFER`
- `START_YEAR`
- `END_YEAR`
- output directory names

### 3. Option 2 replaces previous CSV output

In Extract Only mode, the script deletes any existing `results/download_era5_data.csv` before rebuilding it.

### 4. GRIB heterogeneity is handled explicitly

Using `cfgrib.open_datasets(...)` allows the script to process files that contain multiple internal GRIB groups instead of assuming a single cube.

---

## Minimal Example Workflow

### First-time setup

```bash
pip install numpy pandas tqdm xarray cfgrib eccodes cdsapi
```

Create folders if needed:

```bash
mkdir grib
mkdir results
```

### Download and process ERA5 data

```bash
python download_era5_data.py
```

Then choose:

```text
1
```

### Process GRIB files already stored locally

```bash
python download_era5_data.py
```

Then choose:

```text
2
```

---

## Suggested Future README Extensions

If you want this documentation to become even more complete, the next logical additions would be:

- a sample excerpt of the output CSV,
- a section on typical `cfgrib` / `eccodes` installation issues,
- notes on CDS API quotas and rate limits,
- and a section describing how to adapt the script for multiple stations instead of a single fixed point.
