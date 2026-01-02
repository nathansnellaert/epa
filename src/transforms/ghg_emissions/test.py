"""Validation tests for EPA GHG emissions datasets."""

import pyarrow as pa
from subsets_utils import validate
from subsets_utils.testing import assert_valid_year, assert_in_range


def test_by_state(table: pa.Table) -> None:
    """Validate epa_ghg_emissions_by_state output."""
    validate(table, {
        "columns": {
            "year": "string",
            "state": "string",
            "state_name": "string",
            "co2": "double",
            "ch4": "double",
            "n2o": "double",
            "total_co2e": "double",
            "facility_count": "int",
        },
        "not_null": ["year", "state", "state_name", "total_co2e", "facility_count"],
        "unique": ["year", "state"],
        "min_rows": 500,  # 14 years * ~50 states
    })

    assert_valid_year(table, "year")

    # Years should be 2010-2023
    years = [int(y) for y in table.column("year").to_pylist()]
    assert min(years) == 2010, f"Expected min year 2010, got {min(years)}"
    assert max(years) == 2023, f"Expected max year 2023, got {max(years)}"

    # State codes should be 2 chars
    states = table.column("state").to_pylist()
    assert all(len(s) == 2 for s in states), "State codes should be 2 characters"

    # Emissions should be non-negative
    totals = table.column("total_co2e").to_pylist()
    assert all(t >= 0 for t in totals), "Total emissions should be non-negative"

    # Facility count should be positive
    counts = table.column("facility_count").to_pylist()
    assert all(c > 0 for c in counts), "Facility count should be positive"


def test_by_sector(table: pa.Table) -> None:
    """Validate epa_ghg_emissions_by_sector output."""
    validate(table, {
        "columns": {
            "year": "string",
            "sector": "string",
            "co2": "double",
            "ch4": "double",
            "n2o": "double",
            "total_co2e": "double",
            "facility_count": "int",
        },
        "not_null": ["year", "sector", "total_co2e", "facility_count"],
        "unique": ["year", "sector"],
        "min_rows": 100,  # 14 years * ~10 sectors
    })

    assert_valid_year(table, "year")

    # Years should be 2010-2023
    years = [int(y) for y in table.column("year").to_pylist()]
    assert min(years) == 2010, f"Expected min year 2010, got {min(years)}"
    assert max(years) == 2023, f"Expected max year 2023, got {max(years)}"

    # Emissions should be non-negative
    totals = table.column("total_co2e").to_pylist()
    assert all(t >= 0 for t in totals), "Total emissions should be non-negative"

    # Should have expected sectors
    sectors = set(table.column("sector").to_pylist())
    assert "Power Plants" in sectors, "Expected Power Plants sector"


def test_by_gas(table: pa.Table) -> None:
    """Validate epa_ghg_emissions_by_gas output."""
    validate(table, {
        "columns": {
            "year": "string",
            "gas_code": "string",
            "gas_name": "string",
            "total_co2e": "double",
        },
        "not_null": ["year", "gas_code", "gas_name", "total_co2e"],
        "unique": ["year", "gas_code"],
        "min_rows": 100,  # 14 years * ~12 gas types
    })

    assert_valid_year(table, "year")

    # Should have major gases
    gases = set(table.column("gas_code").to_pylist())
    assert "CO2" in gases, "Expected CO2"
    assert "CH4" in gases, "Expected CH4 (Methane)"
    assert "N2O" in gases, "Expected N2O (Nitrous Oxide)"
