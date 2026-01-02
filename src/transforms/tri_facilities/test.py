import pyarrow as pa
from subsets_utils import validate
from subsets_utils.testing import assert_in_range


def test(table: pa.Table) -> None:
    """Validate EPA TRI facilities output."""
    validate(table, {
        "columns": {
            "tri_facility_id": "string",
            "facility_name": "string",
            "street_address": "string",
            "city_name": "string",
            "county_name": "string",
            "state_abbr": "string",
            "zip_code": "string",
            "region": "string",
            "latitude": "double",
            "longitude": "double",
            "parent_co_name": "string",
            "epa_registry_id": "string",
            "fac_closed_ind": "string",
        },
        "not_null": ["tri_facility_id", "facility_name", "state_abbr"],
        "min_rows": 10000,
    })

    assert_in_range(table, "latitude", 17, 72)
    assert_in_range(table, "longitude", -180, -60)

    states = set(table.column("state_abbr").to_pylist())
    assert len(states) >= 40, f"Should have facilities in most states, got {len(states)}"

    print(f"  Validated {len(table):,} TRI facilities")
