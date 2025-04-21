from typing import Any
from datetime import datetime, timedelta

import pandas as pd
from sodapy import Socrata # type: ignore
from google.cloud import bigquery
from config.env import NYC_API_TOKEN
from config import load_config

from etl.core.utils import normalize_strings

def get_yesterdays_311_data() -> pd.DataFrame:
    """Pulls 311 service requests from the NYC Open Data API for yesterday."""
    if not NYC_API_TOKEN:
        raise ValueError("Missing NYC_API_TOKEN. Check your .env file.")

    client = Socrata("data.cityofnewyork.us", NYC_API_TOKEN)

    yesterday: str = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%dT00:00:00.000")
    results: list[dict[str, Any]] = client.get(
        "erm2-nwe9",
        where=f"created_date >= '{yesterday}'",
        limit=10000,
    )

    return pd.DataFrame.from_records(results)


def clean_311_data(raw_df: pd.DataFrame) -> pd.DataFrame:
    """Cleans and formats the 311 data for loading."""
    df = raw_df.copy()

    # Parse date fields if present
    for col in ["created_date", "closed_date", "due_date"]:
        new_col = col.replace("_date", "").capitalize() + "_Date"
        if col in df.columns:
            df[new_col] = pd.to_datetime(df[col], errors="coerce")
        else:
            df[new_col] = pd.NaT

    # Create surrogate date keys safely
    for col in ["Created_Date", "Closed_Date", "Due_Date"]:
        key_col = f"{col}_Key"
        if col in df.columns:
            df[key_col] = df[col].dt.strftime("%Y%m%d").astype("Int64")
        else:
            df[key_col] = pd.NA

    # Trim to required columns
    keep_cols = [
        "unique_key",
        "Created_Date", "Closed_Date", "Due_Date",
        "Created_Date_Key", "Closed_Date_Key", "Due_Date_Key",
        "agency", "agency_name",
        "complaint_type", "descriptor", "location_type",
        "incident_zip", "incident_address", "street_name",
        "cross_street_1", "cross_street_2",
        "intersection_street_1", "intersection_street_2",
        "city", "borough", "latitude", "longitude",
        "status", "resolution_description",
    ]

    df["unique_key"] = pd.to_numeric(df["unique_key"], errors="coerce").astype("Int64")

    df = normalize_strings(df, [
        "agency", "agency_name",
        "complaint_type", "descriptor", "location_type",
        "incident_zip", "incident_address", "street_name",
        "cross_street_1", "cross_street_2",
        "intersection_street_1", "intersection_street_2",
        "city", "borough",
    ])

    # Only keep columns that are present
    available_cols = [col for col in keep_cols if col in df.columns]
    return df[available_cols]


def load_to_bigquery(df: pd.DataFrame) -> None:
    """Loads a DataFrame to the configured BigQuery fact table."""
    cfg = load_config()
    project_id = cfg["bigquery"]["project_id"]
    dataset = cfg["bigquery"]["dataset"]
    table_name = cfg["tables"]["fact_311_complaints"]
    table_id = f"{project_id}.{dataset}.{table_name}"

    client = bigquery.Client()

    if "__join_key__" in df.columns:
        df = df.drop(columns="__join_key__")

    job = client.load_table_from_dataframe(df, table_id)
    job.result()

    print(f"Loaded {df.shape[0]} rows to {table_id}")
