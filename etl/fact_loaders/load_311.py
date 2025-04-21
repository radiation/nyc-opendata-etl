from datetime import datetime, timedelta

import pandas as pd
from sodapy import Socrata # type: ignore
from google.cloud import bigquery
from config.env import NYC_API_TOKEN
from config import load_config

from etl.core.utils import normalize_strings


def get_311_data_between(start: str, end: str, limit: int = 10000) -> pd.DataFrame:
    client = Socrata("data.cityofnewyork.us", NYC_API_TOKEN)
    where_clause = f"created_date >= '{start}' AND created_date < '{end}'"
    print(f"📦 Fetching 311 data between: {start} → {end}")
    results = client.get("erm2-nwe9", where=where_clause, limit=limit)
    return pd.DataFrame.from_records(results)


def get_yesterdays_311_data() -> pd.DataFrame:
    today = datetime.utcnow().date()
    start = f"{today - timedelta(days=1)}T00:00:00.000"
    end = f"{today}T00:00:00.000"
    return get_311_data_between(start, end)


def get_311_data_for_year(year: int) -> pd.DataFrame:
    start = f"{year}-01-01T00:00:00.000"
    end = f"{year + 1}-01-01T00:00:00.000"
    return get_311_data_between(start, end, limit=500000)


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

    print("📦 Columns returned from API:", df.columns.tolist())
    print("🧪 DataFrame shape:", df.shape)
    print(df.head(3))

    if "unique_key" not in df.columns:
        raise ValueError(
            "Missing required column 'unique_key' in 311 data. "
            "This likely means the API schema changed or the response was empty."
        )

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
