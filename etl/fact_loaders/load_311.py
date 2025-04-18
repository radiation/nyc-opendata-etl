from typing import Any
from datetime import datetime, timedelta

import pandas as pd
from sodapy import Socrata # type: ignore
from google.cloud import bigquery
from config.env import NYC_API_TOKEN
from config import load_config


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

    for col in ["created_date", "closed_date", "due_date"]:
        if col in df.columns:
            new_col = col.replace("_date", "").capitalize() + "_Date"
            df[new_col] = pd.to_datetime(df[col], errors="coerce")
        else:
            df[new_col] = pd.NaT

    df["Created_Date_Key"] = df["Created_Date"].dt.strftime("%Y%m%d").astype("Int64")
    df["Closed_Date_Key"] = df["Closed_Date"].dt.strftime("%Y%m%d").astype("Int64")
    df["Due_Date_Key"] = df["Due_Date"].dt.strftime("%Y%m%d").astype("Int64")

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
        "status", "resolution_description"
    ]
    return df[keep_cols]


def load_to_bigquery(df: pd.DataFrame) -> None:
    """Loads a DataFrame to the configured BigQuery fact table."""
    cfg = load_config()
    project_id = cfg["bigquery"]["project_id"]
    dataset = cfg["bigquery"]["dataset"]
    table_name = cfg["tables"]["fact_311_complaints"]
    table_id = f"{project_id}.{dataset}.{table_name}"

    client = bigquery.Client()
    job = client.load_table_from_dataframe(df, table_id)
    job.result()

    print(f"✅ Loaded {df.shape[0]} rows to {table_id}")
