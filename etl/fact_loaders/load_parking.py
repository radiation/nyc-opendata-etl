from typing import Any
from datetime import datetime, timedelta

import pandas as pd
from sodapy import Socrata # type: ignore
from google.cloud import bigquery
from config.env import NYC_API_TOKEN
from config import load_config


def get_parking_data_between(start: str, end: str, limit: int = 100000) -> pd.DataFrame:
    if not NYC_API_TOKEN:
        raise ValueError("Missing NYC_API_TOKEN. Check your .env file.")

    client = Socrata("data.cityofnewyork.us", NYC_API_TOKEN)
    where_clause = f"issue_date >= '{start}' AND issue_date < '{end}'"
    print(f"Fetching parking data between {start} and {end}")
    results: list[dict[str, Any]] = client.get("pvqr-7yc4", where=where_clause, limit=limit)
    return pd.DataFrame.from_records(results)


def get_yesterdays_parking_data() -> pd.DataFrame:
    if not NYC_API_TOKEN:
        raise ValueError("Missing NYC_API_TOKEN. Check your .env file.")

    today = datetime.utcnow().date()
    start = f"{today - timedelta(days=1)}T00:00:00.000"
    end = f"{today}T00:00:00.000"
    return get_parking_data_between(start, end)


def clean_parking_data(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

    # Rename fields to match expected dim loader inputs
    df.rename(columns={
        "plate_id": "plate",
        "registration_state": "state",
        "plate_type": "license_type",
        "violation_precinct": "precinct",
        "violation_county": "borough"
    }, inplace=True)

    # Handle issue date and surrogate key
    if "issue_date" in df.columns:
        df["Issue_Date"] = pd.to_datetime(df["issue_date"], errors="coerce")
        df["Issue_Date_Key"] = df["Issue_Date"].dt.strftime("%Y%m%d").astype("Int64")
    else:
        df["Issue_Date"] = pd.NaT
        df["Issue_Date_Key"] = pd.NA

    # Ensure numeric fields are present and properly typed
    numeric_cols = [
        "fine_amount",
        "penalty_amount",
        "interest_amount",
        "reduction_amount",
        "payment_amount",
        "amount_due",
    ]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
        else:
            df[col] = 0

    # Rename to match BigQuery schema
    df.rename(
        columns={
            "fine_amount": "Fine_Amount",
            "penalty_amount": "Penalty_Amount",
            "interest_amount": "Interest_Amount",
            "reduction_amount": "Reduction_Amount",
            "payment_amount": "Payment_Amount",
            "amount_due": "Amount_Due",
        },
        inplace=True,
    )

    if "summons_number" in df.columns:
        df["summons_number"] = pd.to_numeric(df["summons_number"], errors="coerce").astype("Int64")

    keep_cols = [
        # fact fields
        "summons_number",
        "Issue_Date", "Issue_Date_Key",
        "Agency_Key", "Violation_Key", "Vehicle_Key", "Parking_Location_Key",
        "Fine_Amount", "Penalty_Amount", "Interest_Amount",
        "Reduction_Amount", "Payment_Amount", "Amount_Due",
        
        # dimension fields
        "plate", "state", "license_type",
        "violation_code", "violation_description",
        "borough", "precinct",
    ]

    df = df[[col for col in keep_cols if col in df.columns]]

    return df


def load_to_bigquery(df: pd.DataFrame) -> None:
    cfg = load_config()
    project = cfg["bigquery"]["project_id"]
    dataset = cfg["bigquery"]["dataset"]
    table_id = f"{project}.{dataset}.{cfg['tables']['fact_parking_tickets']}"

    # Deduplicate columns
    df = df.loc[:, ~df.columns.duplicated()]

    # Explicit drop just in case
    if "__join_key__" in df.columns:
        df = df.drop(columns="__join_key__")

    client = bigquery.Client()
    job = client.load_table_from_dataframe(df, table_id)
    job.result()

    print(f"Loaded {df.shape[0]} rows to {table_id}")
