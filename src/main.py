#!/usr/bin/env python3
# src/main.py

from __future__ import annotations
import argparse
from datetime import datetime, timedelta
import zoneinfo
from typing import Dict, Set

import pandas as pd
from google.cloud import bigquery

from config import GCP_PROJECT, BQ_STAGING_DATASET
from socrata.client import fetch_dataset
from etl.loaders import build_dimension_df, build_fact_df
from etl.registry import ALL_DIMS, FACT_311, FACT_PARKING
from etl.normalization import normalize_strings, parse_violation_time
from etl.facts.fact_parking_loader import get_parking_data_between
from db.adapters.staging import BigQueryAdapter

# Default timezone for date computations
TZ = zoneinfo.ZoneInfo("America/New_York")


def get_311_data_between(
    start: str,
    end: str,
    limit: int = 10_000_000,
) -> pd.DataFrame:
    """
    Fetch raw 311 data from Socrata for the given date range.
    Strips any timezone so SoQL sees plain datetimes.
    """
    start_dt = datetime.fromisoformat(start)
    end_dt = datetime.fromisoformat(end)
    start_str = start_dt.strftime("%Y-%m-%dT%H:%M:%S.000")
    end_str = end_dt.strftime("%Y-%m-%dT%H:%M:%S.000")
    where = f"created_date >= '{start_str}' AND created_date < '{end_str}'"
    print(f"Fetching 311 data between: {start_str} → {end_str}")
    df = fetch_dataset("erm2-nwe9", where=where, limit=limit)
    print(f"Fetched {len(df)} records")
    return df


def main(start: str, end: str) -> None:
    """
    Orchestrate the ETL pipeline: fetch from Socrata, normalize, build dims/facts,
    and load into BigQuery staging.
    """
    # Initialize BigQuery staging adapter
    bq_client = bigquery.Client(project=GCP_PROJECT)
    adapter = BigQueryAdapter(bq_client, BQ_STAGING_DATASET)

    # Fetch source data
    raw_311 = get_311_data_between(start, end)
    raw_parking = get_parking_data_between(start, end)

    # Derive date & time natural keys for 311
    raw_311["full_date"] = (
        pd.to_datetime(raw_311["created_date"])  
          .dt.strftime("%Y-%m-%d")
    )
    raw_311["incident_time"] = (
        pd.to_datetime(raw_311["created_date"])  
          .dt.strftime("%H:%M:%S.000")
    )

    # Canonicalize and derive for parking
    raw_parking = raw_parking.rename(
        columns={
            "plate_id": "license_plate",
            "registration_state": "state",
        }
    )
    raw_parking["full_date"] = (
        pd.to_datetime(
            raw_parking["issue_date"],
            infer_datetime_format=True,
            errors="coerce"
        )
        .dt.strftime("%Y-%m-%d")
    )
    raw_parking["incident_time"] = raw_parking["violation_time"].apply(
        parse_violation_time
    )

    # Normalize all dimension keys up-front
    all_keys: Set[str] = {key for dim in ALL_DIMS for key in dim.natural_keys}

    keys_311 = list(all_keys & set(raw_311.columns))
    keys_p = list(all_keys & set(raw_parking.columns))

    raw_311 = normalize_strings(raw_311, columns=keys_311)
    raw_parking = normalize_strings(raw_parking, columns=keys_p)

    # Build and load dimensions from combined data
    raw_all = pd.concat([raw_311, raw_parking], ignore_index=True, sort=False)
    staging_dims: Dict[str, pd.DataFrame] = {}
    for dim in ALL_DIMS:
        dim_df = build_dimension_df(raw_all, dim)
        adapter.load_dim(dim_df, dim.table_name, truncate=True)
        staging_dims[dim.table_name] = dim_df

    # Build and load fact tables
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

    now = datetime.now(TZ)
    today_midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday_midnight = today_midnight - timedelta(days=1)
    default_start = yesterday_midnight.isoformat(timespec="milliseconds")
    default_end = today_midnight.isoformat(timespec="milliseconds")

    start = args.start or default_start
    end = args.end or default_end

    main(start, end)
