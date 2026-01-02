#!/usr/bin/env python3
import os

"""Main orchestrator for EPA Environmental Data integration."""

import argparse
import os

os.environ['RUN_ID'] = os.getenv('RUN_ID', 'local-run')

from subsets_utils import validate_environment
from ingest import tri_facilities as ingest_tri
from ingest import ghg_emissions as ingest_ghg
from ingest import ghg_emissions_by_sector as ingest_ghg_sector
from transforms import ghg_emissions as transform_ghg


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ingest-only", action="store_true", help="Only fetch data from API")
    parser.add_argument("--transform-only", action="store_true", help="Only transform existing raw data")
    args = parser.parse_args()

    validate_environment()

    should_ingest = not args.transform_only
    should_transform = not args.ingest_only

    if should_ingest:
        print("\n=== Phase 1: Ingest ===")
        print("Processing EPA TRI facilities...")
        ingest_tri.run()
        print("\nProcessing EPA GHG emissions...")
        ingest_ghg.run()
        print("\nProcessing EPA GHG emissions by sector...")
        ingest_ghg_sector.run()

    if should_transform:
        print("\n=== Phase 2: Transform ===")
        print("Transforming GHG emissions...")
        transform_ghg.run()


if __name__ == "__main__":
    main()
