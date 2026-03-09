#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ERA5 Hourly Data Downloader and Extractor (xarray + cfgrib version)
===================================================================

Overview:
---------
This script works with ERA5 reanalysis data from ECMWF. It can either download hourly
ERA5 data via the CDS API and process the resulting GRIB files, or it can solely extract
data from existing GRIB files. The extraction process reads GRIB files with xarray +
cfgrib, selects a set of pre-defined meteorological and oceanographic variables using
IDW (Inverse Distance Weighting) interpolation to estimate data at an exact point, and
then saves the combined data into a CSV file for analysis.

Key Features:
-------------
- Dual Mode Operation:
  - Option 1 (Download & Process): Downloads ERA5 data via the CDS API (monthly chunks)
    and then extracts specified variables from the resulting GRIB files.
  - Option 2 (Extract Only): Skips downloading and processes all GRIB files already
    available locally (ignoring the START_YEAR and END_YEAR range), using parallel
    processing with a progress bar.
- Selected Variable Extraction with IDW Interpolation:
    - swh  : Significant height of combined wind waves and swell
    - mwd  : Mean wave direction
    - pp1d : Peak wave period
    - wind : 10 metre wind speed
    - dwi  : 10 metre wind direction
- Robust Error Handling & Retry Mechanism: Uses retries with exponential back-off for downloads.
- Detailed Logging: Logs every major step and potential issues to aid debugging.
- Sorted Output: The final CSV file is sorted in ascending order by the datetime column.
- Performance Metrics: Computes overall processing time.

Dependencies:
-------------
- Python 3.x
- Libraries:
    - cdsapi
    - xarray
    - cfgrib
    - eccodes
    - pandas
    - numpy
    - tqdm

Typical installation:
    pip install cdsapi xarray cfgrib eccodes pandas numpy tqdm

Notes:
------
- cfgrib can read heterogeneous GRIB files via cfgrib.open_datasets(...), which is why
  this script uses that API instead of assuming the whole file is a single hypercube.
- By default cfgrib writes .idx sidecar files. This script disables them with
  backend_kwargs={'indexpath': ''} to keep behaviour closer to the previous pygrib-based version.
"""

import calendar
import logging
import multiprocessing
import os
import sys
import time
from concurrent.futures import ProcessPoolExecutor, TimeoutError, as_completed

import numpy as np
import pandas as pd
from tqdm import tqdm

try:
    import cdsapi
except Exception:
    cdsapi = None


# Use "spawn" start method to help with C-library based packages on Windows.
if __name__ == '__main__':
    multiprocessing.set_start_method("spawn", force=True)


# ----------------------------- Configuration -----------------------------
# Target location: LEIXOES OCEANIC BUOY, Porto/Portugal
LONGITUDE = -9.581666670
LATITUDE = 41.14833299

# Process years from given years (used only in Option 1: Download & Process)
START_YEAR = 1940
END_YEAR = 2025
YEARS = list(range(START_YEAR, END_YEAR + 1))

# Variable definitions.
# request_code is kept exactly as in the original script for download requests.
# Matching during extraction is made robust via aliases / parameter numbers / parameter ids.
VARIABLES = {
    'swh': {
        'request_code': '229.140',
        'aliases': {'swh'},
        'parameter_numbers': {'229'},
        'parameter_ids': {'140229'},
    },
    'mwd': {
        'request_code': '230.140',
        'aliases': {'mwd'},
        'parameter_numbers': {'230'},
        'parameter_ids': {'140230'},
    },
    'pp1d': {
        'request_code': '231.140',
        'aliases': {'pp1d'},
        'parameter_numbers': {'231'},
        'parameter_ids': {'140231'},
    },
    'wind': {
        'request_code': '245.140',
        'aliases': {'wind'},
        'parameter_numbers': {'245'},
        'parameter_ids': {'140245'},
    },
    'dwi': {
        'request_code': '249.140',
        'aliases': {'dwi'},
        'parameter_numbers': {'249'},
        'parameter_ids': {'140249'},
    },
}

# Directories for GRIB files and output CSV
DATA_DIR = 'grib'
RESULTS_DIR = 'results'
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(RESULTS_DIR, exist_ok=True)

# Bounding box buffer (degrees)
BUFFER = 0.25
NORTH = LATITUDE + BUFFER
SOUTH = LATITUDE - BUFFER
EAST = LONGITUDE + BUFFER
WEST = LONGITUDE - BUFFER
AREA = [NORTH, WEST, SOUTH, EAST]

# Grid resolution for data extraction
GRID = [0.25, 0.25]

# API request delay and retry configuration
REQUEST_DELAY = 60  # seconds
MAX_RETRIES = 3

# Extraction settings
IDW_POWER = 2
TIMEOUT_PER_FILE = 180  # seconds
GRIB_EXTENSIONS = ('.grib', '.grib2', '.grb', '.grb2')

# Logging configuration
LOG_FILE = 'download_era5_data.log'
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


# ----------------------------- Utility functions -----------------------------
def initialize_cds_client():
    """Initialize and return the CDS API client."""
    if cdsapi is None:
        logging.error("cdsapi is not installed. Install it to use option 1.")
        sys.exit("Error: cdsapi is not installed. Install it to use option 1.")

    try:
        client = cdsapi.Client()
        logging.info("CDS API client initialized successfully.")
        return client
    except Exception as exc:
        logging.error("Failed to initialize CDS API client. Error: %s", exc)
        sys.exit(1)


def download_monthly_data(client, year, month, variable_list, area, grid, output_dir):
    """
    Download ERA5 monthly data for a specific year and month.
    Returns the file path if successful, else None.
    """
    file_name = f"ERA5_{year}_{month:02d}.grib"
    file_path = os.path.join(output_dir, file_name)

    if os.path.exists(file_path):
        logging.info("Data for %s-%02d exists. Skipping download.", year, month)
        return file_path

    days_in_month = calendar.monthrange(year, month)[1]
    days = [f"{day:02d}" for day in range(1, days_in_month + 1)]

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logging.info("Attempt %d: Downloading data for %s-%02d...", attempt, year, month)
            client.retrieve(
                'reanalysis-era5-single-levels',
                {
                    'product_type': 'reanalysis',
                    'format': 'grib',
                    'variable': variable_list,
                    'year': str(year),
                    'month': f"{month:02d}",
                    'day': days,
                    'time': [f"{hour:02d}:00" for hour in range(24)],
                    'area': area,
                    'grid': grid,
                },
                file_path,
            )
            logging.info("Successfully downloaded data for %s-%02d.", year, month)
            return file_path
        except Exception as exc:
            logging.warning("Attempt %d failed for %s-%02d. Error: %s", attempt, year, month, exc)
            if attempt < MAX_RETRIES:
                wait_time = REQUEST_DELAY * attempt
                logging.info("Retrying after %d seconds...", wait_time)
                time.sleep(wait_time)
            else:
                logging.error("All %d attempts failed for %s-%02d.", MAX_RETRIES, year, month)
                return None


def _choose_coord_name(obj, *names):
    """Return the first coordinate/dimension name found in obj."""
    for name in names:
        if name in getattr(obj, 'coords', {}):
            return name
    for name in names:
        if name in getattr(obj, 'dims', {}):
            return name
    return None


def _normalize_target_longitude(dataset_or_array, target_lon):
    """Match target longitude convention to the dataset (e.g. -180..180 or 0..360)."""
    lon_name = _choose_coord_name(dataset_or_array, 'longitude', 'lon')
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
        normalized = ((target_lon + 180.0) % 360.0) - 180.0
        return normalized
    return target_lon


def _identify_variable_key(data_var_name, data_array):
    """Map a cfgrib/xarray variable to one of the output keys in VARIABLES."""
    attrs = getattr(data_array, 'attrs', {}) or {}

    candidate_strings = set()

    def _add(value):
        if value is None:
            return
        text = str(value).strip().lower()
        if text:
            candidate_strings.add(text)

    _add(data_var_name)
    for attr_name in (
        'GRIB_shortName', 'shortName',
        'GRIB_cfVarName', 'cfVarName',
        'GRIB_name', 'long_name', 'standard_name',
        'GRIB_parameterNumber', 'parameterNumber',
        'GRIB_paramId', 'paramId',
    ):
        _add(attrs.get(attr_name))

    # Also add a "parameterNumber.140" style candidate when parameter number exists.
    param_number = attrs.get('GRIB_parameterNumber', attrs.get('parameterNumber'))
    if param_number is not None:
        try:
            param_number_int = int(param_number)
            candidate_strings.add(str(param_number_int))
            candidate_strings.add(f"{param_number_int}.140")
        except Exception:
            pass

    param_id = attrs.get('GRIB_paramId', attrs.get('paramId'))
    if param_id is not None:
        try:
            candidate_strings.add(str(int(param_id)))
        except Exception:
            candidate_strings.add(str(param_id).strip().lower())

    for output_key, spec in VARIABLES.items():
        accepted = set(spec['aliases'])
        accepted.add(spec['request_code'])
        accepted.update(spec['parameter_numbers'])
        accepted.update(spec['parameter_ids'])
        if candidate_strings & {item.lower() for item in accepted}:
            return output_key

    return None


def _reduce_dataarray_to_time_lat_lon(data_array):
    """
    Reduce a DataArray to at most [time, latitude, longitude] or [latitude, longitude].
    Any extra singleton dimensions are squeezed out. Any unexpected non-singleton
    dimensions are indexed at 0 to keep behaviour practical for mixed GRIB metadata.
    """
    keep_dim_names = {'time', 'valid_time', 'latitude', 'longitude', 'lat', 'lon', 'y', 'x'}
    da = data_array.squeeze(drop=True)

    extra_non_singleton_dims = [dim for dim in da.dims if dim not in keep_dim_names and da.sizes.get(dim, 1) > 1]
    for dim in extra_non_singleton_dims:
        logging.info(
            "Unexpected non-singleton dimension '%s' in variable '%s'. Selecting first element.",
            dim,
            getattr(da, 'name', 'unknown'),
        )
        da = da.isel({dim: 0})

    da = da.squeeze(drop=True)
    return da


def _extract_time_values(data_array, dataset=None):
    """Return list of timestamps aligned with the data array values."""
    # Prefer valid_time because cfgrib exposes it explicitly when available.
    if 'valid_time' in data_array.coords:
        values = np.atleast_1d(data_array['valid_time'].values)
        return pd.to_datetime(values, errors='coerce', format='mixed').tolist()
    if 'time' in data_array.coords:
        values = np.atleast_1d(data_array['time'].values)
        return pd.to_datetime(values, errors='coerce', format='mixed').tolist()

    if dataset is not None:
        if 'valid_time' in dataset.coords:
            values = np.atleast_1d(dataset['valid_time'].values)
            return pd.to_datetime(values, errors='coerce', format='mixed').tolist()
        if 'time' in dataset.coords:
            values = np.atleast_1d(dataset['time'].values)
            return pd.to_datetime(values, errors='coerce', format='mixed').tolist()

    return [pd.NaT]


def _build_lat_lon_grids(data_array):
    """Create 2D latitude and longitude grids for either regular or curvilinear grids."""
    lat_name = _choose_coord_name(data_array, 'latitude', 'lat')
    lon_name = _choose_coord_name(data_array, 'longitude', 'lon')

    if lat_name is None or lon_name is None:
        raise ValueError("Latitude/longitude coordinates were not found in the GRIB dataset.")

    lat_values = np.asarray(data_array[lat_name].values, dtype=float)
    lon_values = np.asarray(data_array[lon_name].values, dtype=float)

    if lat_values.ndim == 1 and lon_values.ndim == 1:
        lat_grid, lon_grid = np.meshgrid(lat_values, lon_values, indexing='ij')
    else:
        lat_grid, lon_grid = lat_values, lon_values

    return lat_grid, lon_grid, lat_name, lon_name


def _idw_interpolate_dataarray(data_array, target_lat, target_lon, power=2):
    """
    Interpolate an xarray DataArray to the exact target coordinate using IDW.
    Returns a 1D numpy array of values aligned with the time coordinate, or a
    one-element array when the variable has no time dimension.
    """
    da = _reduce_dataarray_to_time_lat_lon(data_array)
    lat_grid, lon_grid, lat_name, lon_name = _build_lat_lon_grids(da)

    normalized_lon = _normalize_target_longitude(da, target_lon)
    dist = np.sqrt((lat_grid - target_lat) ** 2 + (lon_grid - normalized_lon) ** 2)
    dist_flat = dist.reshape(-1)

    spatial_dims = []
    for dim in da.dims:
        if dim == lat_name or dim == lon_name:
            spatial_dims.append(dim)
        elif dim in {'y', 'x'}:
            spatial_dims.append(dim)

    time_dim = None
    for candidate in ('valid_time', 'time'):
        if candidate in da.dims:
            time_dim = candidate
            break

    # Ensure the array has time as the leading dimension when it exists.
    if time_dim is not None:
        ordered_dims = [time_dim] + [dim for dim in da.dims if dim != time_dim]
        da = da.transpose(*ordered_dims)
        data = np.asarray(da.values, dtype=float)
        data_2d = data.reshape(data.shape[0], -1)
    else:
        data = np.asarray(da.values, dtype=float)
        data_2d = data.reshape(1, -1)

    # Exact grid point match.
    exact_idx = np.where(dist_flat < 1e-12)[0]
    if exact_idx.size > 0:
        values = data_2d[:, exact_idx[0]]
        return np.asarray(values, dtype=float)

    # Standard IDW over all finite points in the small extraction grid.
    weights = 1.0 / np.power(dist_flat, power)
    finite_mask = np.isfinite(data_2d)
    weighted_data = np.where(finite_mask, data_2d * weights, 0.0)
    effective_weights = np.where(finite_mask, weights, 0.0)
    numerator = weighted_data.sum(axis=1)
    denominator = effective_weights.sum(axis=1)

    with np.errstate(invalid='ignore', divide='ignore'):
        values = numerator / denominator

    return np.asarray(values, dtype=float)


def _safe_open_cfgrib_datasets(file_path):
    """Open all readable datasets from a GRIB file using cfgrib."""
    try:
        import cfgrib
    except Exception as exc:
        raise ImportError(
            "cfgrib is not installed or could not be imported. "
            "Install cfgrib and eccodes to extract GRIB data."
        ) from exc

    # open_datasets is preferred for heterogeneous GRIB files.
    datasets = cfgrib.open_datasets(
        file_path,
        backend_kwargs={
            'indexpath': '',
            'cache_geo_coords': True,
            'read_keys': ['shortName', 'cfVarName', 'paramId', 'parameterNumber'],
        },
    )
    return datasets


def process_grib_file_df(file_path):
    """
    Process a GRIB file to extract selected data and return a pandas DataFrame.
    This version uses xarray + cfgrib instead of pygrib.
    """
    try:
        datasets = _safe_open_cfgrib_datasets(file_path)
    except Exception as exc:
        logging.error("Failed to open %s with cfgrib. Error: %s", file_path, exc)
        return None

    data_records = {}

    for dataset_index, ds in enumerate(datasets):
        try:
            data_var_names = list(ds.data_vars)
        except Exception:
            data_var_names = []

        if not data_var_names:
            logging.info("Dataset #%d in %s contains no data variables.", dataset_index, os.path.basename(file_path))
            try:
                ds.close()
            except Exception:
                pass
            continue

        for data_var_name in data_var_names:
            try:
                da = ds[data_var_name]
                var_key = _identify_variable_key(data_var_name, da)
                if var_key is None:
                    logging.info(
                        "Variable skipped in %s: dataset=%d, variable='%s', attrs=%s",
                        os.path.basename(file_path),
                        dataset_index,
                        data_var_name,
                        dict(da.attrs),
                    )
                    continue

                interpolated_values = _idw_interpolate_dataarray(
                    da,
                    target_lat=LATITUDE,
                    target_lon=LONGITUDE,
                    power=IDW_POWER,
                )
                time_values = _extract_time_values(da, dataset=ds)

                # Align time length to data length as defensively as possible.
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
                        time_key = 'NaT'
                    else:
                        time_key = pd.Timestamp(valid_time).strftime('%Y-%m-%d %H:%M:%S')

                    if time_key not in data_records:
                        data_records[time_key] = {}
                    data_records[time_key][var_key] = float(value) if np.isfinite(value) else None

            except Exception as exc:
                logging.warning(
                    "Error processing variable '%s' in %s: %s",
                    data_var_name,
                    file_path,
                    exc,
                )
                continue

        try:
            ds.close()
        except Exception:
            pass

    if not data_records:
        logging.warning("No data extracted from %s.", file_path)
        return None

    records = []
    for dt, vars_data in data_records.items():
        row = {'datetime': dt}
        for var in VARIABLES.keys():
            row[var] = vars_data.get(var, None)
        records.append(row)

    df = pd.DataFrame(records)
    if df.empty:
        return None
    return df


# ----------------------------- Main execution -----------------------------
def main():
    """Main function to orchestrate data retrieval and processing."""
    user_option = input(
        "SELECT YOUR OPTION:\n"
        "1) Download ERA5 data from CDS API and process GRIB files;\n"
        "2) Only extract data from existing GRIB files.\n"
        "Choose (1 or 2): "
    ).strip()

    if user_option not in {'1', '2'}:
        print("Invalid option selected. Exiting.")
        return

    overall_start_time = time.time()
    output_csv = os.path.join(RESULTS_DIR, 'download_era5_data.csv')

    if user_option == '2':
        # Option 2: Process all existing GRIB files in parallel, ignoring START_YEAR and END_YEAR.
        if os.path.exists(output_csv):
            os.remove(output_csv)
            logging.info("Deleted existing CSV file at %s.", output_csv)

        grib_files = sorted(
            file_name for file_name in os.listdir(DATA_DIR)
            if file_name.lower().endswith(GRIB_EXTENSIONS)
        )

        if not grib_files:
            print(f"No GRIB files found in '{DATA_DIR}'.")
            logging.warning("No GRIB files found in '%s'.", DATA_DIR)
            return

        dataframes = []
        max_workers = os.cpu_count() or 1

        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(process_grib_file_df, os.path.join(DATA_DIR, file_name)): file_name
                for file_name in grib_files
            }

            for future in tqdm(as_completed(futures), total=len(futures), desc="Processing GRIB files"):
                file_name = futures[future]
                try:
                    df = future.result(timeout=TIMEOUT_PER_FILE)
                    if df is not None and not df.empty:
                        dataframes.append(df)
                        logging.info("Processed data from %s.", file_name)
                    else:
                        logging.error("No data extracted from %s.", file_name)
                except TimeoutError:
                    logging.error("Processing %s timed out.", file_name)
                    future.cancel()
                except Exception as exc:
                    logging.error("Exception while processing %s: %s", file_name, exc)

        if dataframes:
            final_df = pd.concat(dataframes, ignore_index=True)
            final_df['datetime'] = pd.to_datetime(final_df['datetime'], errors='coerce')
            final_df.sort_values(by='datetime', inplace=True)
            final_df.to_csv(output_csv, index=False)
            print("Data processing completed (Option 2).")
        else:
            print("No data was extracted from any GRIB file.")

    else:
        # Option 1: Download data and process each GRIB file sequentially.
        client = initialize_cds_client()
        variable_list = [spec['request_code'] for spec in VARIABLES.values()]
        total_requests = len(YEARS) * 12
        pbar = tqdm(total=total_requests, desc="Downloading ERA5 Data")

        if not os.path.exists(output_csv):
            header_df = pd.DataFrame(columns=['datetime'] + list(VARIABLES.keys()))
            header_df.to_csv(output_csv, index=False)
            logging.info("Created new CSV file at %s with headers.", output_csv)

        for year in YEARS:
            for month in range(1, 13):
                file_path = download_monthly_data(
                    client=client,
                    year=year,
                    month=month,
                    variable_list=variable_list,
                    area=AREA,
                    grid=GRID,
                    output_dir=DATA_DIR,
                )

                if file_path:
                    df = process_grib_file_df(file_path)
                    if df is not None and not df.empty:
                        df.to_csv(output_csv, mode='a', header=False, index=False)
                        logging.info("Processed data for %s-%02d appended to CSV.", year, month)
                    else:
                        logging.error("Failed to process data for %s-%02d.", year, month)
                else:
                    logging.error("Skipping %s-%02d due to download failure.", year, month)

                pbar.update(1)
                time.sleep(REQUEST_DELAY)

        pbar.close()

        # Read the CSV file, sort by datetime, and write it back.
        try:
            final_df = pd.read_csv(output_csv)
            final_df['datetime'] = pd.to_datetime(final_df['datetime'], errors='coerce')
            final_df.sort_values(by='datetime', inplace=True)
            final_df.to_csv(output_csv, index=False)
            logging.info("CSV file sorted by datetime column.")
        except Exception as exc:
            logging.error("Error sorting CSV file: %s", exc)

    overall_end_time = time.time()
    total_time = overall_end_time - overall_start_time

    logging.info("Data processing completed.")
    logging.info("Total time: %.2f seconds", total_time)

    print("Data processing completed.")
    print(f"Total time: {total_time:.2f} seconds")


if __name__ == "__main__":
    main()
