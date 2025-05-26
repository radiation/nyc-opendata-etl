from __future__ import annotations
import argparse
from datetime import datetime, timedelta
import zoneinfo
from typing import Dict

import pandas as pd
from google.cloud import bigquery

from config import GCP_PROJECT, BQ_STAGING_DATASET
from etl.facts.fact_311_loader import get_311_data_between
from etl.facts.fact_parking_loader import get_parking_data_between
from etl.loaders import build_dimension_df, build_fact_df
from etl.normalization import parse_violation_time
from etl.registry import ALL_DIMS, FACT_311, FACT_PARKING
from db.adapters.staging import BigQueryAdapter

# Default to America/New_York timezone for date computations
TZ = zoneinfo.ZoneInfo("America/New_York")


def main(start: str, end: str) -> None:
    """
    Orchestrate the ETL pipeline: fetch from Socrata, 
    build dims/fact, load into BigQuery staging.
    """
    # Initialize BigQuery staging adapter
    bq_client = bigquery.Client(project=GCP_PROJECT)
    adapter = BigQueryAdapter(bq_client, BQ_STAGING_DATASET)

    # Fetch raw 311 data
    raw_311 = get_311_data_between(start, end)

    # Fetch raw parking
    raw_parking = get_parking_data_between(start, end)
    print("Fetched parking data with shape:", raw_parking.shape)
    print("Fetched parking data with cols:", raw_parking.columns)

    # Calculate derived date & time columns
    raw_311["full_date"] = (pd.to_datetime(raw_311["created_date"]).dt.date)
    raw_311["incident_time"] = pd.to_datetime(raw_311["created_date"]).dt.strftime("%H:%M:%S.000")
    raw_parking["full_date"] = (
        pd.to_datetime(
            raw_parking["issue_date"],
            infer_datetime_format=True,
            errors="coerce"
        )
        .dt.strftime("%Y-%m-%d")
    )
    raw_parking["incident_time"] = raw_parking["violation_time"].apply(parse_violation_time)

    # Rename columns to match dimension definitions
    raw_parking = raw_parking.rename(
        columns={
            "plate_id":"license_plate",
            "registration_state":"state",
        }
    )

    # Combine for dimension-building
    raw_all = pd.concat([raw_311, raw_parking], ignore_index=True, sort=False)

    # Load dimensions into staging
    staging_dims: Dict[str, pd.DataFrame] = {}
    for dim in ALL_DIMS:
        dim_df = build_dimension_df(raw_all, dim)
        adapter.load_dim(dim_df, dim.table_name, truncate=True)
        staging_dims[dim.table_name] = dim_df

    # Load the two facts separately
    f311 = build_fact_df(raw_311, FACT_311, staging_dims)
    adapter.load_fact(f311, FACT_311.table_name, truncate=True)

    fpark = build_fact_df(raw_parking, FACT_PARKING, staging_dims)
    adapter.load_fact(fpark, FACT_PARKING.table_name, truncate=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run NYC Open Data ETL")
    parser.add_argument(
        "--start",
        dest="start",
        type=str,
        help="Start timestamp (e.g. 2023-01-01T00:00:00.000)",
    )
    parser.add_argument(
        "--end",
        dest="end",
        type=str,
        help="End timestamp (e.g. 2023-01-02T00:00:00.000)",
    )
    args = parser.parse_args()

    # Compute defaults (yesterday's midnight to today's midnight)
    now = datetime.now(TZ)
    today_midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday_midnight = today_midnight - timedelta(days=1)
    default_start = yesterday_midnight.isoformat(timespec="milliseconds")
    default_end = today_midnight.isoformat(timespec="milliseconds")

    start = args.start or default_start
    end = args.end or default_end

    main(start, end)