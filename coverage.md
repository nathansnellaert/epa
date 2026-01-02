# EPA Data Coverage

Data from the U.S. Environmental Protection Agency via the [Envirofacts API](https://www.epa.gov/enviro/envirofacts-data-service-api).

## Published Datasets

### `epa_ghg_emissions_by_state`

Annual greenhouse gas emissions aggregated by U.S. state.

| Column | Description |
|--------|-------------|
| `year` | Reporting year (YYYY) |
| `state` | U.S. state abbreviation (2-letter code) |
| `state_name` | Full state name |
| `co2` | Carbon dioxide emissions (metric tons CO2e) |
| `ch4` | Methane emissions (metric tons CO2e) |
| `n2o` | Nitrous oxide emissions (metric tons CO2e) |
| `total_co2e` | Total emissions across all gases (metric tons CO2e) |
| `facility_count` | Number of reporting facilities |

**Coverage:** 756 rows (54 states/territories x 14 years)

### `epa_ghg_emissions_by_sector`

Annual greenhouse gas emissions aggregated by industry sector.

| Column | Description |
|--------|-------------|
| `year` | Reporting year (YYYY) |
| `sector` | Industry sector name |
| `co2` | Carbon dioxide emissions (metric tons CO2e) |
| `ch4` | Methane emissions (metric tons CO2e) |
| `n2o` | Nitrous oxide emissions (metric tons CO2e) |
| `total_co2e` | Total emissions across all gases (metric tons CO2e) |
| `facility_count` | Number of reporting facilities |

**Coverage:** 126 rows (9 sectors x 14 years)

**Sectors:** Chemicals, Metals, Minerals, Other, Petroleum and Natural Gas Systems, Power Plants, Pulp and Paper, Refineries, Waste

### `epa_ghg_emissions_by_gas`

Annual greenhouse gas emissions aggregated by gas type.

| Column | Description |
|--------|-------------|
| `year` | Reporting year (YYYY) |
| `gas_code` | Gas identifier code |
| `gas_name` | Full gas name |
| `total_co2e` | Total emissions (metric tons CO2e) |

**Coverage:** 162 rows (12 gas types x 14 years)

**Gas types:** CO2, CH4, N2O, BIOCO2, HFC, HFE, NF3, Other, Other_Full, PFC, SF6, Very_Short

## Raw Data Ingested

### `ghg_emissions` (from `ghg_emitter_gas`)

Facility-level greenhouse gas emissions from the [Greenhouse Gas Reporting Program (GHGRP)](https://www.epa.gov/ghgreporting).

- Years: 2010-2023 (14 years, updated annually, ~6 month lag)
- Records: 308,567 total (~17-23K/year)
- Raw file: 134 MB JSON
- Scope: Facilities emitting >25,000 metric tons CO2e/year

### `ghg_emissions_by_sector` (from `ghg_emitter_sector`)

Same as above but includes sector classification.

- Records: 308,581 total
- Raw file: ~140 MB JSON

### `tri_facilities` (from `tri_facility`)

Facilities reporting to the [Toxics Release Inventory](https://www.epa.gov/toxics-release-inventory-tri-program). Facility metadata only (location, contacts), not statistical release data.

- Records: 64,990 facilities
- Raw file: 100 MB JSON

## Not Yet Ingested

### High Value Statistical Tables

| Table | Records | Description | Crawl Difficulty |
|-------|---------|-------------|------------------|
| `tri_reporting_form` | ~3.2M | Annual toxic release filings (1987-2023) | Easy |
| `br_reporting` | ~19M | Biennial hazardous waste reports | Easy |

### Tables That Don't Work

These tables return "not available" errors despite being documented:
- `icis_air_fac` - Air facility data
- `sems_active_sites` - Superfund sites
- `radnet_analytical_results` - Radiation monitoring
- `uv_hourly` - UV index data

### Separate APIs (Not Envirofacts)

| Source | Description | Auth Required |
|--------|-------------|---------------|
| [AQS API](https://aqs.epa.gov/aqsweb/documents/data_api.html) | Air quality monitoring data | Yes (free signup) |
| [GHGRP Data Sets](https://www.epa.gov/ghgreporting/data-sets) | Detailed GHG Excel files | No (bulk download) |

## API Notes

**Endpoint:** `https://data.epa.gov/efservice/{table}/rows/{start}:{end}/JSON`

**Rate limits:** 5 requests/second (conservative), 15-minute request timeout

**Pagination:** Row-based with inclusive ranges. Best to fetch by year filter rather than raw pagination (API is flaky with large row ranges).

**Filtering:** Path-based (`/state/=/CA/year/=/2023/`)

**Formats:** JSON, CSV, Parquet, XML, Excel

**Authentication:** None required
