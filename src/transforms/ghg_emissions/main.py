"""Transform EPA GHG emissions into aggregate datasets."""

import sys
from pathlib import Path

# Add src/ to path when run directly
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import pyarrow as pa
from collections import defaultdict
from subsets_utils import load_raw_json, upload_data, publish
from transforms.ghg_emissions.test import test_by_state, test_by_sector, test_by_gas


# Dataset definitions
DATASETS = {
    "by_state": {
        "id": "epa_ghg_emissions_by_state",
        "title": "EPA Greenhouse Gas Emissions by State",
        "description": "Annual greenhouse gas emissions by U.S. state from EPA's Greenhouse Gas Reporting Program (GHGRP). Covers large facilities emitting >25,000 metric tons CO2e/year. Emissions reported in metric tons CO2-equivalent.",
        "column_descriptions": {
            "year": "Reporting year (YYYY)",
            "state": "U.S. state abbreviation (2-letter code)",
            "state_name": "Full state name",
            "co2": "Carbon dioxide emissions (metric tons CO2e)",
            "ch4": "Methane emissions (metric tons CO2e)",
            "n2o": "Nitrous oxide emissions (metric tons CO2e)",
            "total_co2e": "Total emissions across all gases (metric tons CO2e)",
            "facility_count": "Number of reporting facilities",
        },
    },
    "by_sector": {
        "id": "epa_ghg_emissions_by_sector",
        "title": "EPA Greenhouse Gas Emissions by Sector",
        "description": "Annual greenhouse gas emissions by industry sector from EPA's Greenhouse Gas Reporting Program (GHGRP). Sectors include Power Plants, Refineries, Chemicals, Metals, etc. Emissions reported in metric tons CO2-equivalent.",
        "column_descriptions": {
            "year": "Reporting year (YYYY)",
            "sector": "Industry sector name",
            "co2": "Carbon dioxide emissions (metric tons CO2e)",
            "ch4": "Methane emissions (metric tons CO2e)",
            "n2o": "Nitrous oxide emissions (metric tons CO2e)",
            "total_co2e": "Total emissions across all gases (metric tons CO2e)",
            "facility_count": "Number of reporting facilities",
        },
    },
    "by_gas": {
        "id": "epa_ghg_emissions_by_gas",
        "title": "EPA Greenhouse Gas Emissions by Gas Type",
        "description": "Annual greenhouse gas emissions by gas type from EPA's Greenhouse Gas Reporting Program (GHGRP). Includes CO2, CH4, N2O, HFCs, PFCs, SF6, and other fluorinated gases. Emissions reported in metric tons CO2-equivalent.",
        "column_descriptions": {
            "year": "Reporting year (YYYY)",
            "gas_code": "Gas identifier code",
            "gas_name": "Full gas name",
            "total_co2e": "Total emissions (metric tons CO2e)",
        },
    },
}


def aggregate_by_state(raw_data):
    """Aggregate emissions by state and year."""
    # Key: (year, state) -> {state_name, gases, facilities}
    agg = defaultdict(lambda: {
        "state_name": None,
        "co2": 0.0,
        "ch4": 0.0,
        "n2o": 0.0,
        "total": 0.0,
        "facilities": set(),
    })

    for r in raw_data:
        key = (r["year"], r["state"])
        agg[key]["state_name"] = r["state_name"]
        agg[key]["facilities"].add(r["facility_id"])

        emission = r["co2e_emission"] or 0.0
        agg[key]["total"] += emission

        gas = r["gas_code"]
        if gas == "CO2":
            agg[key]["co2"] += emission
        elif gas == "CH4":
            agg[key]["ch4"] += emission
        elif gas == "N2O":
            agg[key]["n2o"] += emission

    records = []
    for (year, state), data in sorted(agg.items()):
        records.append({
            "year": str(year),
            "state": state,
            "state_name": data["state_name"],
            "co2": data["co2"],
            "ch4": data["ch4"],
            "n2o": data["n2o"],
            "total_co2e": data["total"],
            "facility_count": len(data["facilities"]),
        })

    return records


def aggregate_by_sector(raw_data):
    """Aggregate emissions by sector and year."""
    # Key: (year, sector) -> {gases, facilities}
    agg = defaultdict(lambda: {
        "co2": 0.0,
        "ch4": 0.0,
        "n2o": 0.0,
        "total": 0.0,
        "facilities": set(),
    })

    for r in raw_data:
        key = (r["year"], r["sector_name"])
        agg[key]["facilities"].add(r["facility_id"])

        emission = r["co2e_emission"] or 0.0
        agg[key]["total"] += emission

        gas = r["gas_code"]
        if gas == "CO2":
            agg[key]["co2"] += emission
        elif gas == "CH4":
            agg[key]["ch4"] += emission
        elif gas == "N2O":
            agg[key]["n2o"] += emission

    records = []
    for (year, sector), data in sorted(agg.items()):
        records.append({
            "year": str(year),
            "sector": sector,
            "co2": data["co2"],
            "ch4": data["ch4"],
            "n2o": data["n2o"],
            "total_co2e": data["total"],
            "facility_count": len(data["facilities"]),
        })

    return records


def aggregate_by_gas(raw_data):
    """Aggregate emissions by gas type and year."""
    # Key: (year, gas_code) -> {gas_name, total}
    agg = defaultdict(lambda: {"gas_name": None, "total": 0.0})

    for r in raw_data:
        key = (r["year"], r["gas_code"])
        agg[key]["gas_name"] = r["gas_name"]
        agg[key]["total"] += r["co2e_emission"] or 0.0

    records = []
    for (year, gas_code), data in sorted(agg.items()):
        records.append({
            "year": str(year),
            "gas_code": gas_code,
            "gas_name": data["gas_name"],
            "total_co2e": data["total"],
        })

    return records


def run():
    """Transform GHG emissions into aggregate datasets."""
    # Load raw data
    print("  Loading raw GHG emissions data...")
    raw_gas = load_raw_json("ghg_emissions")
    raw_sector = load_raw_json("ghg_emissions_by_sector")

    # 1. Emissions by state (from gas data which has state info)
    print("  Aggregating by state...")
    state_records = aggregate_by_state(raw_gas)
    state_table = pa.Table.from_pylist(state_records)
    print(f"    {len(state_table):,} state-year combinations")
    test_by_state(state_table)
    upload_data(state_table, DATASETS["by_state"]["id"])
    publish(DATASETS["by_state"]["id"], DATASETS["by_state"])

    # 2. Emissions by sector (from sector data)
    print("  Aggregating by sector...")
    sector_records = aggregate_by_sector(raw_sector)
    sector_table = pa.Table.from_pylist(sector_records)
    print(f"    {len(sector_table):,} sector-year combinations")
    test_by_sector(sector_table)
    upload_data(sector_table, DATASETS["by_sector"]["id"])
    publish(DATASETS["by_sector"]["id"], DATASETS["by_sector"])

    # 3. Emissions by gas type (from gas data)
    print("  Aggregating by gas type...")
    gas_records = aggregate_by_gas(raw_gas)
    gas_table = pa.Table.from_pylist(gas_records)
    print(f"    {len(gas_table):,} gas-year combinations")
    test_by_gas(gas_table)
    upload_data(gas_table, DATASETS["by_gas"]["id"])
    publish(DATASETS["by_gas"]["id"], DATASETS["by_gas"])

    print("  Done!")


if __name__ == "__main__":
    run()
