
"""Ingest EPA Toxics Release Inventory facilities."""

from epa_client import get_tri_facilities
from subsets_utils import save_raw_json


def run():
    """Fetch all TRI facilities and save raw JSON."""
    print("  Fetching TRI facilities...")

    all_records = []
    start_row = 0
    batch_size = 9999  # EPA API uses inclusive ranges, so 0:9999 = 10000 rows

    while True:
        end_row = start_row + batch_size
        print(f"    Fetching rows {start_row:,} to {end_row:,}...")

        batch = get_tri_facilities(start_row=start_row, end_row=end_row)

        if not batch:
            break

        all_records.extend(batch)
        print(f"      Got {len(batch):,} facilities")

        if len(batch) < batch_size + 1:  # Less than full batch means we're done
            break

        start_row = end_row + 1  # Next batch starts after this one

    print(f"  Total: {len(all_records):,} facilities")
    save_raw_json(all_records, "tri_facilities")
    print("  Saved raw TRI facilities data")
