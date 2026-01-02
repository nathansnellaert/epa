"""Ingest EPA Greenhouse Gas Emissions data from GHGRP."""

from epa_client import get_ghg_emissions_by_gas
from subsets_utils import save_raw_json

# GHGRP data available from 2010 onwards
YEARS = list(range(2010, 2024))  # 2010-2023


def run():
    """Fetch all GHG emissions by gas type, year by year."""
    print("  Fetching GHG emissions data...")

    all_records = []

    for year in YEARS:
        print(f"    Fetching {year}...")
        # Each year is ~22K records, fits in one request
        batch = get_ghg_emissions_by_gas(year=year, start_row=0, end_row=30000)

        if batch:
            all_records.extend(batch)
            print(f"      Got {len(batch):,} records")

    print(f"  Total: {len(all_records):,} emission records")
    save_raw_json(all_records, "ghg_emissions")
    print("  Saved raw GHG emissions data")
