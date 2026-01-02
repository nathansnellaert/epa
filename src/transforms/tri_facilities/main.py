"""Transform EPA TRI facilities to dataset."""

import pyarrow as pa
from subsets_utils import load_raw_json, upload_data, publish
from .test import test

DATASET_ID = "epa_tri_facilities"

METADATA = {
    "id": DATASET_ID,
    "title": "EPA TRI Facilities",
    "description": "Facilities reporting to EPA's Toxics Release Inventory (TRI) program. TRI tracks the management of certain toxic chemicals that may pose a threat to human health and the environment.",
    "column_descriptions": {
        "tri_facility_id": "EPA TRI facility identifier",
        "facility_name": "Name of the facility",
        "street_address": "Street address",
        "city_name": "City name",
        "county_name": "County name",
        "state_abbr": "State abbreviation",
        "zip_code": "ZIP code",
        "region": "EPA region number",
        "latitude": "Facility latitude",
        "longitude": "Facility longitude",
        "parent_co_name": "Parent company name",
        "epa_registry_id": "EPA Facility Registry ID",
        "fac_closed_ind": "Facility closed indicator",
    }
}

SCHEMA = pa.schema([
    ('tri_facility_id', pa.string()),
    ('facility_name', pa.string()),
    ('street_address', pa.string()),
    ('city_name', pa.string()),
    ('county_name', pa.string()),
    ('state_abbr', pa.string()),
    ('zip_code', pa.string()),
    ('region', pa.string()),
    ('latitude', pa.float64()),
    ('longitude', pa.float64()),
    ('parent_co_name', pa.string()),
    ('epa_registry_id', pa.string()),
    ('fac_closed_ind', pa.string()),
])


def run():
    """Transform raw TRI facilities to PyArrow table and upload."""
    all_records = load_raw_json("tri_facilities")

    if not all_records:
        raise ValueError("No TRI facility records found")

    normalized_records = []
    for record in all_records:
        normalized_records.append({
            'tri_facility_id': record.get('tri_facility_id'),
            'facility_name': record.get('facility_name'),
            'street_address': record.get('street_address'),
            'city_name': record.get('city_name'),
            'county_name': record.get('county_name'),
            'state_abbr': record.get('state_abbr'),
            'zip_code': record.get('zip_code'),
            'region': record.get('region'),
            'latitude': record.get('pref_latitude'),
            'longitude': record.get('pref_longitude'),
            'parent_co_name': record.get('parent_co_name'),
            'epa_registry_id': record.get('epa_registry_id'),
            'fac_closed_ind': record.get('fac_closed_ind'),
        })

    print(f"  Transformed {len(normalized_records):,} TRI facilities")

    table = pa.Table.from_pylist(normalized_records, schema=SCHEMA)

    test(table)

    upload_data(table, DATASET_ID, mode="overwrite")
    publish(DATASET_ID, METADATA)


if __name__ == "__main__":
    run()
