# ERA5 Hourly Data Downloader and Extractor

## 1. Purpose and engineering scope

Python script `download_era5_data.py` is a production-oriented application for the retrieval and extraction of hourly ERA5 single-level time series at a user-defined offshore or coastal target point. The tool is intended for engineering workflows that require a reproducible and auditable offshore metocean forcing series, typically as input to hindcast screening, design-state compilation, extreme-value pre-processing, offshore boundary specification for nearshore models, or operational environmental assessment.

The application supports two execution modes:

1. **Download and process**  
   Monthly ERA5 GRIB files are retrieved from the Copernicus Climate Data Store (CDS) and processed immediately.

2. **Extract existing GRIB files only**  
   Previously downloaded GRIB files are scanned locally and converted into a merged point time series without contacting CDS.

The script is **GUI-first**, but also supports CLI execution. Internally it is based on:

- `xarray` for labeled multidimensional data handling;
- `cfgrib` as the GRIB-to-xarray backend;
- `ecCodes` as the GRIB decoding engine;
- `pandas` for time handling and CSV generation;
- `numpy` for interpolation and numerical operations;
- `cdsapi` for ERA5 retrievals in download mode.

This is **not** a `pygrib` workflow.

---

## 2. Scientific background: what ERA5 is and why it matters for engineers

ERA5 is the fifth-generation ECMWF global atmospheric reanalysis produced under the Copernicus Climate Change Service (C3S). It combines observations and numerical weather prediction through **4D-Var data assimilation** within the ECMWF Integrated Forecast System (IFS, CY41R2). ERA5 provides a dynamically consistent reconstruction of the atmosphere, land surface and ocean waves, making it appropriate for engineering analyses that require long, homogeneous hourly records rather than sparse in-situ measurements alone.

Key ERA5 characteristics relevant to engineering use are:

- coverage from **1940 to present**, with continuing near-real-time extension;
- **hourly** output for the high-resolution realization;
- atmospheric high-resolution realization (HRES) of about **31 km**;
- a 10-member ensemble data-assimilation product used to quantify relative random uncertainty;
- inclusion of **ocean-wave parameters** as single-level products alongside atmospheric and land parameters.

From an engineering perspective, ERA5 is especially useful when:

- local buoy records are too short for climatological interpretation;
- directional wave and wind histories are required over many decades;
- a consistent offshore boundary condition is needed before nearshore transformation with tools such as SWAN, TOMAWAC, MIKE 21 SW, or equivalent;
- preliminary screening is required before site-specific hindcast, nested modelling, or physical model testing.

ERA5 should nevertheless be understood for what it is: a high-quality **global reanalysis**, not a harbour-scale wave transformation model. Offshore point extraction from ERA5 does **not** resolve local bathymetric refraction, diffraction, harbour resonance, breakwater shadowing, surf-zone breaking, or site-specific current-wave interaction at engineering design scale.

---

## 3. ERA5 data architecture relevant to this application

According to the supplied ECMWF/Copernicus documentation, ERA5 is produced with 4D-Var data assimilation in IFS CY41R2 and 137 hybrid sigma/pressure model levels. Surface and single-level parameters include atmospheric 2D quantities and ocean-wave outputs from the coupled wave model. ERA5 contains an hourly HRES realization and a reduced-resolution 10-member ensemble. The original public ERA5 release started in 1979, but the back extension to 1940 is now available in the documented product family. Daily ERA5T updates are typically available about 5 days behind real time, and final ERA5 normally replaces ERA5T about 2 to 3 months later. The supplied documentation also records recent cases where final ERA5 differed from ERA5T after later corrections, which is important when engineering studies require strict data traceability.

For data delivery, ECMWF distinguishes between native archived products and CDS-accessible products interpolated to regular latitude/longitude grids. The documentation notes that wave data are produced on a wave-model grid distinct from the atmospheric model grid, while CDS-accessible ERA5 data are supplied on regular latitude/longitude grids via ECMWF interpolation software. For engineering workflows, this means the retrieved grid is already a delivered analysis product, not the native internal model grid.

The current application uses the CDS dataset:

- **Dataset**: `reanalysis-era5-single-levels`
- **Product type**: `reanalysis`
- **Format**: `grib`
- **Temporal resolution**: hourly
- **Request chunking**: one GRIB file per month

---

## 4. Engineering interpretation of the current workflow

The present code is a **point-extraction workflow** built around a very small surrounding ERA5 request window. The application does not download a large regional domain and then sub-sample it. Instead, it computes the **four surrounding ERA5 grid nodes** that enclose the target point and uses those nodes for interpolation to the exact requested coordinate.

### 4.1 Target point

The current default target is:

- **Location**: offshore Leixões / Porto, Portugal
- **Longitude**: `-9.58166667`
- **Latitude**: `41.14833299`

These values are preloaded in the GUI and CLI defaults and are persisted between sessions in `defaults.txt`.

### 4.2 Request geometry actually implemented in the code

The code computes an ERA5-aligned 2×2 stencil using `compute_surrounding_four_point_area(...)`. For a regular grid spacing `Δ = 0.25°`, the request window is:

- `north = ceil(lat / Δ) * Δ`
- `south = floor(lat / Δ) * Δ`
- `east  = ceil(lon / Δ) * Δ`
- `west  = floor(lon / Δ) * Δ`

If the target lies exactly on a grid line in either direction, the code widens the request by one grid step to ensure a true **2-node by 2-node** stencil in that direction.

For the shipped default point, the resulting request area is:

| Bound | Value |
|---|---:|
| North | `41.25` |
| South | `41.00` |
| West  | `-9.75` |
| East  | `-9.50` |

The four surrounding nodes are therefore:

- `(-9.75, 41.25)`
- `(-9.50, 41.25)`
- `(-9.75, 41.00)`
- `(-9.50, 41.00)`

This is the actual implementation in the current code base; it supersedes older documentation that described a simple ±0.25° buffer box.

### 4.3 Why this matters for engineering use

This design has several practical consequences:

- request volumes remain very small, which is operationally efficient;
- interpolation is performed from the nearest enclosing stencil rather than from a wide neighbourhood;
- extracted values remain directly traceable to the local ERA5 grid support;
- the result is appropriate as an **offshore reference time series**, but should not be confused with a site-specific nearshore transformation.

---

## 5. Numerical method implemented by the script

## 5.1 GRIB ingestion strategy

The extractor uses `cfgrib.open_datasets(...)` rather than assuming each GRIB file is a single homogeneous cube. This is a robust choice because ERA5 GRIB files can contain multiple internal groups with distinct metadata or dimensional structure.

The current backend configuration is:

- `indexpath=''` so no `.idx` sidecar files are left behind;
- `cache_geo_coords=True`;
- `read_keys=['shortName', 'cfVarName', 'paramId', 'parameterNumber']`.

## 5.2 Variable-identification logic

The code does not depend on a single metadata key. Each `DataArray` is matched against the configured variable map using a candidate set assembled from:

- xarray variable name;
- `GRIB_shortName` / `shortName`;
- `GRIB_cfVarName` / `cfVarName`;
- `GRIB_name`, `long_name`, `standard_name`;
- `GRIB_parameterNumber` / `parameterNumber`;
- `GRIB_paramId` / `paramId`.

This defensive matching is important when engineering archives include GRIB files generated under slightly different cfgrib/ecCodes exposures.

## 5.3 Coordinate handling

The script automatically:

- detects latitude and longitude coordinate names from common alternatives;
- supports both `-180..180` and `0..360` longitude conventions;
- squeezes singleton dimensions;
- reduces unexpected non-singleton extra dimensions by selecting the first index and logging the event.

This improves robustness when the GRIB payload contains auxiliary dimensions not directly relevant to point extraction.

## 5.4 Time handling

Time coordinates are read in the following priority:

1. `valid_time`
2. `time`
3. fallback to the parent dataset if the variable itself does not expose those coordinates

The code converts timestamps using `pandas.to_datetime(..., errors='coerce', format='mixed')`. If the number of timestamps and interpolated values differs, the script attempts deterministic realignment by repetition or truncation to a common length.

## 5.5 Inverse Distance Weighting (IDW)

Interpolation to the exact target point is performed with **Inverse Distance Weighting** using power `p = 2`.

The implemented equations are:

```text
Di = sqrt((lati - lat0)^2 + (loni - lon0)^2)
wi = Di^(-p)
Xhat(t) = sum[wi * Xi(t)] / sum[wi]
```

where:

- `lat0`, `lon0` are the target coordinates;
- `lati`, `loni` are the coordinates of the four surrounding ERA5 nodes;
- `Xi(t)` is the value at node `i` and time `t`;
- `p = 2` in the shipped code.

Implementation details:

- distances are computed in **degree space** rather than on a great-circle metric;
- if the target coincides with a grid point (`D < 1e-12`), the grid value is returned directly;
- only finite values contribute to the weighted average;
- missing values reduce the effective weight set automatically.

For the small local stencil used by this tool, this is a pragmatic and numerically stable point-extraction method. Users should nevertheless remember that it is a local interpolation on an already gridded global reanalysis product, not a spectral wave transformation model.

---

## 6. ERA5 wave and wind parameters available in the supplied parameter catalogue

The supplied `parameters.xlsx` file contains a much broader ERA5 wave/wind catalogue than the five variables currently extracted by the script. For engineering use, the most relevant families are summarized below.

### 6.1 Total sea-state parameters

| Short name | Description | Unit | Param. No. |
|---|---|---:|---:|
| `swh` | Significant height of combined wind waves and swell | m | 140229 |
| `mwd` | Mean wave direction | degree true | 140230 |
| `pp1d` | Peak wave period | s | 140231 |
| `mwp` | Mean wave period | s | 140232 |
| `mp1` | Mean wave period based on first moment | s | 140220 |
| `mp2` | Mean zero-crossing wave period | s | 140221 |

### 6.2 Wind-wave and swell partition parameters

| Short name | Description | Unit | Param. No. |
|---|---|---:|---:|
| `shww` | Significant height of wind waves | m | 140234 |
| `mdww` | Mean direction of wind waves | degrees | 140235 |
| `mpww` | Mean period of wind waves | s | 140236 |
| `swh1` | Significant wave height of first swell partition | m | 140121 |
| `mwd1` | Mean wave direction of first swell partition | degrees | 140122 |
| `mwp1` | Mean wave period of first swell partition | s | 140123 |
| `swh2` | Significant wave height of second swell partition | m | 140124 |
| `mwd2` | Mean wave direction of second swell partition | degrees | 140125 |
| `mwp2` | Mean wave period of second swell partition | s | 140126 |
| `swh3` | Significant wave height of third swell partition | m | 140127 |
| `mwd3` | Mean wave direction of third swell partition | degrees | 140128 |
| `mwp3` | Mean wave period of third swell partition | s | 140129 |

### 6.3 Extreme, spectral and energy-related wave diagnostics

| Short name | Description | Unit | Param. No. |
|---|---|---:|---:|
| `hmax` | Envelop-maximum individual wave height | m | 140218 |
| `tmax` | Period corresponding to maximum individual wave height | s | 140217 |
| `wefxm` | Wave energy flux magnitude | W m-1 | 140112 |
| `wefxd` | Wave energy flux mean direction | degree true | 140113 |
| `2dfd` | 2D wave spectra (single) | m2 s radian-1 | 140251 |
| `wsp` | Wave spectral peakedness | dimensionless | 140254 |
| `wsk` | Wave spectral kurtosis | dimensionless | 140252 |
| `wss` | Wave spectral skewness | numeric | 140207 |

### 6.4 Probabilistic and threshold-type wave indicators

| Short name | Description | Unit | Param. No. |
|---|---|---:|---:|
| `swhg2` | Significant wave height of at least 2 m | % | 131074 |
| `swhg4` | Significant wave height of at least 4 m | % | 131075 |
| `swhg6` | Significant wave height of at least 6 m | % | 131076 |
| `swhg8` | Significant wave height of at least 8 m | % | 131077 |
| `mwpg8` | Mean wave period of at least 8 s | % | 131078 |
| `mwpg10` | Mean wave period of at least 10 s | % | 131079 |
| `mwpg12` | Mean wave period of at least 12 s | % | 131080 |
| `mwpg15` | Mean wave period of at least 15 s | % | 131081 |
| `swhp` | Significant wave height probability | % | 131229 |
| `mwpp` | Mean wave period probability | % | 131232 |

### 6.5 Wind parameters relevant to offshore engineering

| Short name | Description | Unit | Param. No. |
|---|---|---:|---:|
| `10u` | 10 metre U wind component | m s-1 | 165 |
| `10v` | 10 metre V wind component | m s-1 | 166 |
| `10si` | 10 metre wind speed | m s-1 | 207 |
| `wind` | 10 metre wind speed | m s-1 | 140245 |
| `dwi` | 10 metre wind direction | degrees | 140249 |
| `10fg` | Maximum 10 metre wind gust since previous post-processing | m s-1 | 49 |
| `10fg6` | Maximum 10 metre wind gust in the last 6 hours | m s-1 | 123 |
| `10spg10` | 10 metre wind speed of at least 10 m/s | % | 131068 |
| `10spg15` | 10 metre wind speed of at least 15 m/s | % | 131069 |

The parameter catalogue therefore supports a much richer metocean extraction workflow than the present script currently implements.

---

## 7. Parameters actually downloaded by the current script

Although ERA5 exposes many wave and wind parameters, the current code downloads **only five** variables. These are defined in the central `VARIABLES` map in `download_era5_data.py`.

| Output column | Request code in script | Parameter mapping used for extraction | Unit | Engineering meaning |
|---|---|---|---:|---|
| `swh` | `229.140` | paramId `140229` / alias `swh` | m | Total significant wave height of combined wind sea and swell |
| `mwd` | `230.140` | paramId `140230` / alias `mwd` | degree true | Mean wave direction of the total sea state |
| `pp1d` | `231.140` | paramId `140231` / alias `pp1d` | s | Spectral peak period |
| `wind` | `245.140` | paramId `140245` / alias `wind` | m s-1 | 10 m wind speed as mapped by the code |
| `dwi` | `249.140` | paramId `140249` / alias `dwi` | degrees | 10 m wind direction |

The final CSV schema is therefore:

```text
datetime,swh,mwd,pp1d,wind,dwi
```

### Important engineering note on the wind-speed field

The script is currently configured to request the parameter mapped by code `245.140` and paramId `140245`, which the supplied catalogue identifies as `wind` / **10 metre wind speed**. This is not the same identifier as `10si` / param number `207`, even though both represent 10 m wind speed products in the catalogue. If strict consistency with a prior atmospheric-wind workflow is required, engineers should verify whether `140245` is the intended wind product for their application before using the output in downstream design calculations.

---

## 8. Copernicus / CDS access and download procedure

## 8.1 Required account and credentials

Download mode requires a valid Copernicus Climate Data Store account and a working CDS API configuration. The usual configuration file is `.cdsapirc` in the user home directory, for example:

```text
url: https://cds.climate.copernicus.eu/api/v2
key: <YOUR_UID>:<YOUR_API_KEY>
```

A valid CDS account, the applicable dataset licence acceptance, and network access are required before the Python script can retrieve any ERA5 data.

## 8.2 What one monthly request contains

For each `(year, month)` combination, the script requests:

- the dataset `reanalysis-era5-single-levels`;
- the five configured variables only;
- every calendar day in the month;
- all 24 hourly slots from `00:00` to `23:00`;
- a very small local area around the target point;
- a regular grid request defined by `[0.25, 0.25]`.

Each successful request is saved as:

```text
grib/ERA5_YYYY_MM.grib
```

## 8.3 Retry logic and pacing

The application includes explicit retry logic:

- `MAX_RETRIES = 3`
- `REQUEST_DELAY = 60 s`

If attempt `n` fails, the script waits `REQUEST_DELAY * n` seconds before retrying. This yields progressively longer waits across successive failures.

## 8.4 ERA5T versus final ERA5

For operational workflows, engineers should distinguish between:

- **ERA5T**: preliminary near-real-time data;
- **final ERA5**: later consolidated and quality-checked product.

The supplied ECMWF documentation notes that final ERA5 normally replaces ERA5T after about two to three months, and also documents specific periods when later corrections changed the final product. For bankable studies, litigation-sensitive work, or contractual design documentation, it is good practice to archive the data acquisition date and to avoid treating recent ERA5T data as fully frozen.

---

## 9. Installation and dependency strategy

## 9.1 Core runtime dependencies

Required Python packages for the full workflow are:

- `numpy`
- `pandas`
- `xarray`
- `cfgrib`
- `eccodes`
- `cdsapi` *(download mode only)*
- `tkinter` *(GUI mode only; usually bundled on Windows)*

Typical installation:

```bash
pip install numpy pandas xarray cfgrib eccodes cdsapi
```

If only local extraction is required, `cdsapi` may be omitted. However, `xarray`, `cfgrib`, and `ecCodes` remain mandatory for GRIB decoding.

## 9.2 Dependency checker

The repository also includes `dependencies.py`, which checks imports and reports missing modules, their roles, and suggested installation commands. It distinguishes between:

- core extraction dependencies;
- mode-specific dependencies;
- standard-library modules.

Recommended pre-flight check:

```bash
python dependencies.py
```

---

## 10. Project structure and generated files

The application creates and uses the following working structure:

```text
project/
├─ download_era5_data.py
├─ dependencies.py
├─ defaults.txt
├─ download_era5_data.log
├─ grib/
│  ├─ ERA5_1940_01.grib
│  ├─ ERA5_1940_02.grib
│  └─ ...
└─ results/
   └─ download_era5_data.csv
```

### Generated artifacts

- **Monthly GRIB inputs**: `grib/ERA5_YYYY_MM.grib`
- **Merged time series**: `results/download_era5_data.csv`
- **Execution log**: `download_era5_data.log` (or a user-selected path)
- **Persistent user defaults**: `defaults.txt`

The GUI persists the last-used values for:

- mode;
- longitude;
- latitude;
- start year;
- end year;
- GRIB folder;
- results folder;
- output CSV name;
- log file path.

This is useful in repetitive engineering production workflows where a fixed site is processed repeatedly.

---

## 11. Detailed GUI usage for engineering production runs

The script launches the GUI by default when executed without `--download` or `--extract`.

```bash
python download_era5_data.py
```

### 11.1 GUI layout

The GUI contains three tabs:

1. **Run**  
   Main operational interface for configuration and execution.

2. **Log**  
   Full textual execution log shown in a scrollable console-style panel.

3. **Instructions**  
   Built-in operator guidance embedded directly in the application.

### 11.2 Run tab blocks

#### Mode

Choose between:

- **Download from CDS and process**
- **Extract existing GRIB files only**

#### Target point and time range

Fields:

- Longitude
- Latitude
- Start year
- End year

The GUI expects **longitude first, latitude second**. This is operationally important because inadvertent reversal will shift the request to a completely different geographic region.

The GUI also displays a computed request summary showing:

- north/west/south/east bounds;
- the four surrounding grid nodes used for interpolation.

#### Folders and output

Fields:

- GRIB folder
- Results folder
- Output CSV name
- Log file

Each path field can be edited manually or selected through a browse dialog.

#### Actions

Buttons:

- **Start**
- **Open Log tab**
- **Refresh request summary**
- **Quit**

#### Current request profile

The GUI displays a static operational summary reminding the user that:

- the ERA5 grid step is 0.25° in the current request logic;
- the request mode is a 4-node surrounding stencil;
- interpolation is IDW;
- download mode works month by month;
- extract mode scans local GRIB files;
- the output CSV is sorted by datetime.

#### Progress

The progress panel contains:

- a percentage progress bar;
- a textual step counter;
- an estimated completion time (ETA) computed from the average time per completed file.

This ETA functionality is especially useful when processing many years of monthly GRIB files or a large local GRIB archive.

### 11.3 Logging behaviour in the GUI

The GUI writes messages both:

- to the **Log** tab, and
- to the configured log file on disk.

Messages include:

- CDS initialization;
- request geometry;
- monthly download attempts;
- retries and wait times;
- extraction success/failure summaries;
- timeout events;
- final CSV write confirmation.

### 11.4 Threading model

The GUI uses a worker thread so the interface remains responsive while the ERA5 workflow runs in the background. Progress and log events are passed back to the UI through a queue and polled periodically by the main thread.

This avoids the common engineering-GUI failure mode where long data retrievals make the application appear frozen.

---

## 12. CLI usage

Although the application is GUI-first, it can also be executed from the command line.

### 12.1 Launch GUI explicitly

```bash
python download_era5_data.py --gui
```

### 12.2 Download and process from CLI

```bash
python download_era5_data.py --download \
    --longitude -9.58166667 \
    --latitude 41.14833299 \
    --start-year 1940 \
    --end-year 2026 \
    --data-dir grib \
    --results-dir results \
    --output-csv download_era5_data.csv \
    --log-file download_era5_data.log
```

### 12.3 Extract only from local GRIB files

```bash
python download_era5_data.py --extract \
    --longitude -9.58166667 \
    --latitude 41.14833299 \
    --data-dir grib \
    --results-dir results \
    --output-csv download_era5_data.csv \
    --log-file download_era5_data.log
```

### 12.4 CLI execution notes

- if `--download` is supplied, the workflow runs in download mode;
- if `--extract` is supplied, the workflow runs in extract-only mode;
- if neither is supplied, the program launches the GUI;
- CLI runs also update `defaults.txt`, which means GUI defaults remain synchronized with the most recent CLI execution.

---

## 13. Extract-only mode, multiprocessing and Windows robustness

When running in extract-only mode, the application:

1. scans the selected GRIB directory for files ending in:
   - `.grib`
   - `.grib2`
   - `.grb`
   - `.grb2`
2. processes the files in parallel using `ProcessPoolExecutor`;
3. collects the extracted `DataFrame`s;
4. concatenates them into a single output table;
5. sorts the result by `datetime`;
6. writes the final CSV once.

The current code sets:

- `max_workers = os.cpu_count() or 1`
- `TIMEOUT_PER_FILE = 180 s`
- multiprocessing start method to `spawn`
- `multiprocessing.freeze_support()` in the executable entry point

The `spawn` start method is particularly relevant on Windows when working with compiled extensions and C-backed packages such as `cfgrib` and `ecCodes`.

---

## 14. Output data structure and interpretation

The final CSV contains one row per timestamp and the following fields:

| Column | Unit | Interpretation |
|---|---:|---|
| `datetime` | UTC timestamp | Valid time of the ERA5 sample as reconstructed by the script |
| `swh` | m | Total significant wave height |
| `mwd` | degree true | Mean wave direction |
| `pp1d` | s | Peak period |
| `wind` | m s-1 | 10 m wind speed as mapped by paramId 140245 |
| `dwi` | degrees | 10 m wind direction |

Missing or non-finite values are written as empty CSV entries.

### Recommended engineering QA/QC after extraction

Before using the CSV in design or assessment work, it is prudent to:

1. verify timestamp continuity and expected total record length;
2. check for duplicated rows after concatenation;
3. inspect directional wrap-around behaviour near 0°/360°;
4. compare a short overlap window against buoy or local station data when available;
5. confirm whether `mwd` and `dwi` conventions match the downstream model or reporting convention required by the project.

---

## 15. Logging, diagnostics and failure modes

The logging system records both routine operation and failure diagnostics. Typical logged events include:

- missing runtime dependencies;
- CDS client initialization failures;
- failed download attempts and retries;
- local-file reuse when monthly GRIB already exists;
- unrecognized GRIB variables;
- dimensional reduction of unexpected dimensions;
- variable-level extraction exceptions;
- GRIB-open failures;
- extraction summaries per file;
- timeout events in parallel processing;
- final CSV generation.

The extractor reports each processed file using a structured `GribProcessResult`, including counts of:

- datasets opened;
- recognized variables;
- skipped variables;
- variable errors;
- timestamps extracted.

This is useful for auditability in engineering deliverables where data provenance and processing trace must be documented.

---

## 16. Recommended engineering use cases

This tool is appropriate for:

- assembling long offshore wave/wind boundary-condition time series;
- preparing directional and scalar time histories for exploratory data analysis;
- extracting metocean series for statistical post-processing outside the script;
- creating boundary inputs for third-party nearshore wave models;
- generating rapid feasibility-stage environmental forcing records.

This tool is **not**, by itself, a substitute for:

- site-specific nearshore transformation modelling;
- wave-current interaction modelling in complex tidal channels;
- harbour agitation modelling;
- overtopping assessment at structure toe without prior transformation to the structure front;
- final design verification where local physics require nested spectral modelling or physical model testing.

---

## 17. Practical workflow for engineers

A robust production workflow is:

1. run `python dependencies.py`;
2. confirm CDS credentials and licence acceptance;
3. launch the GUI and verify target coordinates;
4. review the computed request window and surrounding nodes;
5. run a short pilot period first (for example 1 year);
6. inspect the log and CSV for unit consistency and continuity;
7. run the full multi-decadal extraction;
8. archive:
   - the script version,
   - `defaults.txt`,
   - the log file,
   - the generated CSV,
   - and the acquisition date.

For formal design studies, also archive the CDS dataset citation and the ERA5 documentation version used during the study.

---

## 18. References and source material

### ERA5 product and documentation

1. **Copernicus Climate Data Store — ERA5 hourly data on single levels from 1940 to present.**  
   Dataset entry: [https://cds.climate.copernicus.eu/datasets/reanalysis-era5-single-levels](https://cds.climate.copernicus.eu/datasets/reanalysis-era5-single-levels)

2. **ECMWF / Copernicus Knowledge Base — ERA5: data documentation.**  
   Documentation portal: [https://confluence.ecmwf.int/display/CKB/ERA5%3A+data+documentation](https://confluence.ecmwf.int/display/CKB/ERA5%3A+data+documentation)

### Scientific references

3. **Hersbach, H., Bell, B., Berrisford, P., et al. (2020).** *The ERA5 global reanalysis.* Quarterly Journal of the Royal Meteorological Society, 146, 1999-2049.  
   DOI: [https://doi.org/10.1002/qj.3803](https://doi.org/10.1002/qj.3803)

4. **Bell, B., Hersbach, H., Simmons, A., et al. (2021).** *The ERA5 global reanalysis: Preliminary extension to 1950.* Quarterly Journal of the Royal Meteorological Society, 147, 4186-4227.  
   DOI: [https://doi.org/10.1002/qj.4174](https://doi.org/10.1002/qj.4174)

### Attribution note

The supplied ECMWF documentation states that users should cite the relevant CDS catalogue entry and provide clear attribution to the Copernicus programme and the data products used. This should be followed in formal reports, papers and contractual engineering deliverables that rely on ERA5-derived inputs.

---

## 19. Summary of the present implementation

In its current form, `download_era5_data.py` is a robust engineering utility for extracting a long hourly offshore time series of:

- total significant wave height,
- mean wave direction,
- peak wave period,
- 10 m wind speed,
- and 10 m wind direction,

from ERA5 single-level products, using a compact local request window, metadata-robust GRIB parsing, and deterministic IDW interpolation to an exact user-defined target coordinate.

The present README is intentionally more detailed than a typical software quick-start because, in engineering practice, the reliability of the downstream design workflow depends as much on understanding the **data pedigree and numerical extraction method** as on knowing which button to click.
