from datetime import datetime, timedelta
import pandas as pd
from sodapy import Socrata  # type: ignore
from google.cloud import bigquery
from config.env import NYC_API_TOKEN
from config import load_config

from etl.core.utils import normalize_strings


def get_311_data_between(start: str, end: str, limit: int = 10_000_000) -> pd.DataFrame:
    client = Socrata("data.cityofnewyork.us", NYC_API_TOKEN)
    where_clause = f"created_date >= '{start}' AND created_date < '{end}'"
    print(f"Fetching 311 data between: {start} → {end}")
    results = client.get("erm2-nwe9", where=where_clause, limit=limit)
    print(f"Fetched {len(results)} records")
    return pd.DataFrame.from_records(results)


def get_yesterdays_311_data() -> pd.DataFrame:
    today = datetime.utcnow().date()
    start = f"{today - timedelta(days=1)}T00:00:00.000"
    end = f"{today}T00:00:00.000"
    return get_311_data_between(start, end)


def get_311_data_for_year(year: int) -> pd.DataFrame:
    start = f"{year}-01-01T00:00:00.000"
    end = f"{year + 1}-01-01T00:00:00.000"
    return get_311_data_between(start, end, limit=500_000)


def clean_311_data(raw_df: pd.DataFrame) -> pd.DataFrame:
    """Cleans and formats the 311 data for loading into fact_311_complaints."""
    df = raw_df.copy()

    # 1) Parse all timestamp fields we care about
    for raw_col, new_col in [
        ("created_date", "created_timestamp"),
        ("closed_date", "closed_timestamp"),
        ("due_date", "due_date"),
        ("resolution_action_updated_date", "resolution_action_date"),
    ]:
        if raw_col in df.columns:
            df[new_col] = pd.to_datetime(df[raw_col], errors="coerce")
        else:
            df[new_col] = pd.NaT

    # 2) Compute our new date_key, complaint_time, and time_key from created_timestamp
    df["date_key"] = df["created_timestamp"].dt.strftime("%Y%m%d").astype("Int64")
    df["complaint_time"] = df["created_timestamp"].dt.time
    # minute‐level key: HHMM00 as integer
    df["time_key"] = df["created_timestamp"].dt.strftime("%H%M00").astype("Int64")

    # 3) Ensure unique_key is present and cast to string
    if "unique_key" not in df.columns:
        raise ValueError("Missing required column 'unique_key' in 311 data")
    df["unique_key"] = df["unique_key"].astype(str)

    # 4) Standardize all our descriptive columns to lowercase & trim
    norm_cols = [
        "agency", "agency_name", "complaint_type", "descriptor", "location_type",
        "incident_zip", "incident_address", "street_name", "cross_street_1",
        "cross_street_2", "intersection_street_1", "intersection_street_2",
        "address_type", "city", "borough", "landmark", "facility_type",
        "status", "resolution_description", "community_board", "bbl",
        "open_data_channel", "park_facility_name", "park_borough",
        "vehicle_type", "taxi_company_borough", "taxi_pickup_location",
        "bridge_highway_name", "bridge_highway_direction", "road_ramp",
        "bridge_highway_segment", "location"
    ]
    df = normalize_strings(df, norm_cols)

    # 5) Select exactly the cols your BQ table expects:
    target_cols = [
        "unique_key",
        "created_timestamp", "closed_timestamp",
        "agency", "agency_name", "agency_key"
        "complaint_type", "coompalint_key", "descriptor", "location_type",
        "incident_zip", "incident_address", "street_name",
        "cross_street_1", "cross_street_2",
        "intersection_street_1", "intersection_street_2",
        "address_type", "city", "borough", "landmark", "facility_type",
        "status", "due_date", "resolution_description", "resolution_action_date",
        "community_board", "bbl",
        "x_coordinate", "y_coordinate",
        "open_data_channel", "park_facility_name", "park_borough",
        "vehicle_type", "taxi_company_borough", "taxi_pickup_location",
        "bridge_highway_name", "bridge_highway_direction",
        "road_ramp", "bridge_highway_segment",
        "latitude", "longitude", "location",
        "complaint_time", "time_key", "date_key", "location_key"
    ]
    available = [c for c in target_cols if c in df.columns]
    return df[available]


def load_to_bigquery(df: pd.DataFrame) -> None:
    """Loads the cleaned 311 DataFrame into BigQuery."""
    cfg = load_config()
    project = cfg["bigquery"]["project_id"]
    dataset = cfg["bigquery"]["dataset"]
    table = cfg["tables"]["fact_311_complaints"]
    table_id = f"{project}.{dataset}.{table}"

    client = bigquery.Client()

    job = client.load_table_from_dataframe(df, table_id)
    job.result()

    print(f"Loaded {df.shape[0]} rows to {table_id}")
