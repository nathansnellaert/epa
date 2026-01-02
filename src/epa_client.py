# Add parent directory (connector root) to path for utils

"""EPA Envirofacts API client with rate limiting."""

import time
from ratelimit import limits, sleep_and_retry
from subsets_utils import get

BASE_URL = "https://data.epa.gov/efservice"

# EPA Envirofacts has a 15-minute timeout per request
# Be conservative with rate limiting
@sleep_and_retry
@limits(calls=5, period=1)
def rate_limited_get(endpoint, params=None, retries=3):
    """Make a rate-limited GET request to EPA Envirofacts API with retries."""
    url = f"{BASE_URL}/{endpoint}"

    for attempt in range(retries):
        response = get(url, params=params, timeout=120.0)
        if response.status_code == 500 and attempt < retries - 1:
            wait = 2 ** attempt  # exponential backoff: 1s, 2s, 4s
            print(f"      API returned 500, retrying in {wait}s...")
            time.sleep(wait)
            continue
        return response

    return response


def get_table_data(table_name, filters=None, start_row=0, end_row=10000, format='JSON'):
    """
    Get data from an Envirofacts table.

    Args:
        table_name: The table name (e.g., 'tri_facility', 'air_facility')
        filters: Dict of column filters (e.g., {'state_abbr': 'CA'})
        start_row: Starting row number
        end_row: Ending row number
        format: Output format (JSON, CSV, XML)

    Returns:
        List of records or text depending on format
    """
    # Build the endpoint path
    endpoint_parts = [table_name]

    if filters:
        for column, value in filters.items():
            endpoint_parts.append(f"{column}/=/{value}")

    endpoint_parts.append(f"rows/{start_row}:{end_row}")
    endpoint_parts.append(format)

    endpoint = '/'.join(endpoint_parts)

    response = rate_limited_get(endpoint)
    response.raise_for_status()

    if format == 'JSON':
        return response.json()
    return response.text


def get_tri_facilities(state=None, start_row=0, end_row=10000):
    """
    Get Toxics Release Inventory facilities.

    Args:
        state: Optional state abbreviation filter
        start_row: Starting row
        end_row: Ending row

    Returns:
        List of facility records
    """
    filters = {}
    if state:
        filters['state_abbr'] = state

    return get_table_data('tri_facility', filters, start_row, end_row)


def get_air_facilities(state=None, start_row=0, end_row=10000):
    """
    Get air quality facility data from ICIS-AIR.

    Args:
        state: Optional state abbreviation filter
        start_row: Starting row
        end_row: Ending row

    Returns:
        List of facility records
    """
    filters = {}
    if state:
        filters['state_code'] = state

    return get_table_data('icis_air_fac', filters, start_row, end_row)


def get_ghg_emissions_by_gas(year=None, state=None, start_row=0, end_row=10000):
    """
    Get greenhouse gas emissions by gas type from GHGRP.

    Returns facility-level emissions broken down by gas (CO2, CH4, N2O, etc.)
    with CO2-equivalent values. Data available 2010-2023.

    Args:
        year: Optional year filter (2010-2023)
        state: Optional state abbreviation filter
        start_row: Starting row
        end_row: Ending row

    Returns:
        List of emission records with facility info, gas type, and co2e_emission
    """
    filters = {}
    if year:
        filters['year'] = year
    if state:
        filters['state'] = state

    return get_table_data('ghg_emitter_gas', filters, start_row, end_row)


def get_ghg_emissions_by_sector(year=None, state=None, start_row=0, end_row=10000):
    """
    Get greenhouse gas emissions by sector from GHGRP.

    Returns facility-level emissions with sector classification (Power Plants,
    Refineries, Chemicals, etc.). Data available 2010-2023.

    Args:
        year: Optional year filter (2010-2023)
        state: Optional state abbreviation filter
        start_row: Starting row
        end_row: Ending row

    Returns:
        List of emission records with facility info, sector, gas type, and co2e_emission
    """
    filters = {}
    if year:
        filters['year'] = year
    if state:
        filters['state'] = state

    return get_table_data('ghg_emitter_sector', filters, start_row, end_row)
