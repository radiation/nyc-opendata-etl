from __future__ import annotations

import argparse
import sys
import unicodedata
import zoneinfo
from datetime import datetime, timedelta
from typing import Dict, Set

import pandas as pd
from google.cloud import bigquery

from config import BQ_STAGING_DATASET, GCP_PROJECT
from db.adapters.staging import BigQueryAdapter
from etl.fetchers import (fetch_311_complaints, fetch_parking,
                          fetch_parking_with_fines)
from etl.loaders import build_dimension_df, build_fact_df
from etl.normalization import normalize_strings, parse_violation_time
from etl.registry import ALL_DIMS, FACT_311, FACT_PARKING

# Default timezone for date computations
TZ = zoneinfo.ZoneInfo("America/New_York")


def normalize_summons_column(series: pd.Series) -> pd.Series:
    return (
        series.astype(str)
        .map(lambda x: unicodedata.normalize("NFKC", x))
        .str.strip()
    )


def enrich_parking_with_fines(
    df_parking: pd.DataFrame, df_fines: pd.DataFrame
) -> pd.DataFrame:
    df_fines = df_fines.copy()

    print("Sample parking summons:", df_parking["summons_number"].dropna().unique()[:5])
    print("Sample fines summons:", df_fines["summons_number"].dropna().unique()[:5])

    # Normalize summons_number for safe join
    df_parking["summons_number"] = normalize_summons_column(df_parking["summons_number"])
    df_fines["summons_number"] = normalize_summons_column(df_fines["summons_number"])

    overlap = set(df_parking["summons_number"]).intersection(
        set(df_fines["summons_number"])
    )
    print(f"🔍 Overlapping summons_numbers: {len(overlap)}")

    if not overlap:
        sys.exit(
            "❗ No overlapping summons_numbers found between parking and fines data. Exiting."
        )

    # Safe cast for optional fields
    if "payment_date" in df_fines.columns:
        df_fines["payment_date"] = pd.to_datetime(
            df_fines["payment_date"], errors="coerce"
        ).dt.date

    if "booted" in df_fines.columns:
        df_fines["booted"] = df_fines["booted"].map(lambda x: str(x).lower() == "true")

    # Limit to only available and expected columns
    expected_fields = {
        "summons_number",
        "amount_due",
        "payment_amount",
        "payment_date",
        "reduction_amount",
        "interest_amount",
        "penalty_amount",
        "booted",
        "hearing_result",
    }
    merge_fields = [col for col in expected_fields if col in df_fines.columns]

    df_parking["summons_number"] = normalize_summons_column(
        df_parking["summons_number"]
    )
    df_fines["summons_number"] = normalize_summons_column(df_fines["summons_number"])

    parking_keys = set(df_parking["summons_number"])
    fines_keys = set(df_fines["summons_number"])
    overlap = parking_keys & fines_keys

    print(
        f"🎯 Exact key overlap: {len(overlap)} of {len(parking_keys)} parking rows match fines"
    )

    merged = df_parking.merge(
        df_fines[merge_fields],
        how="left",
        on="summons_number",
    )
    print(f"\nMerged parking columns: {merged.columns.tolist()}\n")
    return merged


def main(start_ts: str, end_ts: str) -> None:
    """
    Orchestrate the ETL pipeline: fetch from Socrata, normalize, build dims/facts,
    and load into BigQuery staging.
    """

    # Initialize BigQuery staging adapter
    bq_client = bigquery.Client(project=GCP_PROJECT)
    adapter = BigQueryAdapter(bq_client, BQ_STAGING_DATASET)

    # Fetch 311 data
    print(f"Fetching data from {start_ts} to {end_ts}")
    raw_311 = fetch_311_complaints(start_ts, end_ts)
    print(f"Fetched {len(raw_311)} 311 complaints")

    # Fetch parking tickets and fines
    print("Fetching parking tickets and fines...")
    raw_parking = fetch_parking(start_ts, end_ts)
    print(f"Fetched {len(raw_parking)} parking tickets")
    raw_fines = fetch_parking_with_fines(start_ts, end_ts)
    print(f"Fetched {len(raw_fines)} parking fines")

    # Normalize dimension keys
    all_keys: Set[str] = {key for dim in ALL_DIMS for key in dim.natural_keys}
    raw_311 = normalize_strings(raw_311, columns=list(all_keys & set(raw_311.columns)))
    raw_parking = normalize_strings(
        raw_parking, columns=list(all_keys & set(raw_parking.columns))
    )
    raw_fines = normalize_strings(
        raw_fines, columns=list(all_keys & set(raw_fines.columns))
    )

    # Add fines to parking data
    raw_parking = enrich_parking_with_fines(raw_parking, raw_fines)

    # Derive date & time natural keys for 311
    raw_311["full_date"] = pd.to_datetime(raw_311["created_date"]).dt.strftime(
        "%Y-%m-%d"
    )
    raw_311["incident_time"] = pd.to_datetime(raw_311["created_date"]).dt.strftime(
        "%H:%M:%S.000"
    )

    # Canonicalize and derive for parking
    raw_parking = raw_parking.rename(
        columns={
            "plate_id": "license_plate",
            "registration_state": "state",
        }
    )
    issue_dates: pd.Series[str] = raw_parking["issue_date"].astype(str)
    raw_parking["full_date"] = pd.to_datetime(issue_dates, errors="coerce").dt.strftime(
        "%Y-%m-%d"
    )
    raw_parking["incident_time"] = raw_parking["violation_time"].apply(
        parse_violation_time
    )

    # Build and load dimensions from combined data
    raw_all = pd.concat([raw_311, raw_parking], ignore_index=True, sort=False)

    dupe_cols = raw_parking.columns[raw_parking.columns.duplicated()].tolist()
    if dupe_cols:
        print(f"⚠️ Duplicate columns found in raw_parking: {dupe_cols}")

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

    start_arg = args.start or default_start
    end_arg = args.end or default_end

    main(start_arg, end_arg)
