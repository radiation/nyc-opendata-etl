from typing import Any
from datetime import datetime, timedelta
import pandas as pd
from sodapy import Socrata  # type: ignore
from google.cloud import bigquery
from config.env import NYC_API_TOKEN
from config import load_config

from etl.core.utils import normalize_strings, hash_key

PARKING_DATASETS = {
    2014: "jt7v-77mi",
    2015: "c284-tqph",
    2016: "kiv2-tbus",
    2017: "2bnn-yakx",
    2018: "a5td-mswe",
    2019: "faiq-9dfq",
    2020: "p7t3-5i9s",
    2021: "kvfd-bves",
    2022: "7mxj-7a6y",
    2023: "869v-vr48",
    2024: "pvqr-7yc4",
}
LATEST_FY = max(PARKING_DATASETS)
EARLIEST_FY = min(PARKING_DATASETS)


def get_yesterdays_parking_data():
    today = datetime.utcnow().date()
    start = f"{today - timedelta(days=1)}T00:00:00.000"
    end = f"{today}T00:00:00.000"
    return get_parking_data_between(start, end)


def get_parking_data_between(start: str, end: str, limit: int = 5_000_000) -> pd.DataFrame:
    if not NYC_API_TOKEN:
        raise ValueError("Missing NYC_API_TOKEN. Check your .env file.")

    client = Socrata("data.cityofnewyork.us", NYC_API_TOKEN)
    start_dt = datetime.strptime(start[:10], "%Y-%m-%d")
    fy = start_dt.year if start_dt.month < 7 else start_dt.year + 1
    if fy < EARLIEST_FY:
        return pd.DataFrame()

    if fy > LATEST_FY:
        fy = LATEST_FY
    resource = PARKING_DATASETS[fy]
    clause = (
        f"issue_date >= '{start}' "
        f"AND issue_date < '{end}' "
    )
    print(f"Fetching parking FY{fy} from {resource} between {start}–{end}")
    recs: list[dict[str, Any]] = client.get(resource, where=clause, limit=limit)
    print(f"Fetched {len(recs)} records from {resource} between {start}–{end}")
    df = pd.DataFrame.from_records(recs)

    # Normalize Socrata’s column names to lower+underscores
    df.columns = (
        df.columns
          .str.strip()
          .str.lower()
          .str.replace(r"\s+", "_", regex=True)
    )
    # Socrata FY tables call the code “violation”, not “violation_code”
    if "violation" in df.columns and "violation_code" not in df.columns:
        df.rename(columns={"violation": "violation_code"}, inplace=True)

    return df

def clean_parking_data(raw: pd.DataFrame) -> pd.DataFrame:

    df = raw.copy()

    # 1) normalize column names
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(r"\s+", "_", regex=True)
    )

    # 2) parse dates
    df["issue_date"] = pd.to_datetime(df["issue_date"], errors="coerce")
    df["date_key"] = (df["issue_date"].dt.strftime("%Y%m%d").astype("Int64"))

    # 3) parse times of form "09:03A" / "04:15P" ⇒ HH:MM:SS
    def parse_violation_time(s: Any) -> Any:
        """
        Handle strings like "0853P" or "8:53A" or "12:05PM", returning a Python time.
        """
        if pd.isna(s):
            return None
        raw = str(s).strip().upper()

        # Extract the AM/PM marker
        if not raw or raw[-1] not in {"A", "P"}:
            return None
        ampm = raw[-1] + "M"  # turn "P" → "PM", "A"→"AM"

        # Peel off the marker
        core = raw[:-1]

        # If there's no colon, insert one before the last two digits
        if ":" not in core and len(core) in {3,4}:
            hours = core[:-2]
            mins  = core[-2:]
            core = f"{hours.zfill(2)}:{mins}"

        # Now core should be "H:MM" or "HH:MM"
        ts = core + ampm  # e.g. "08:53PM"
        try:
            return pd.to_datetime(ts, format="%I:%M%p", errors="coerce").time()
        except Exception:
            return None

    df["violation_time"] = df.get("violation_time").apply(parse_violation_time)
    df["time_key"] = (
        df["violation_time"]
        .apply(lambda t: int(t.strftime("%H%M00")) if pd.notnull(t) else pd.NA)
        .astype("Int64")
    )

    # 4) normalize the fields we'll hash for location_key
    loc_cols = [
        "house_number",
        "street_name",
        "intersecting_street",
        "violation_county",
        "violation_precinct",
    ]
    df = normalize_strings(df, loc_cols)
    df = df.dropna(subset=loc_cols)
    df["location_key"] = df.apply(lambda r: hash_key(r, loc_cols), axis=1)

    if "violation_code" not in df.columns and "violation" in df.columns:
        df = df.rename(columns={"violation": "violation_code"})

    df["violation_code"] = pd.to_numeric(df["violation_code"], errors="coerce").astype("Int64")
    if "violation_description" not in df.columns:
        df["violation_description"] = pd.NA

    return df


def load_to_bigquery(df: pd.DataFrame) -> None:
    cfg = load_config()
    project = cfg["bigquery"]["project_id"]
    dataset = cfg["bigquery"]["dataset"]
    table = cfg["tables"]["fact_parking_tickets"]
    table_id = f"{project}.{dataset}.{table}"

    client = bigquery.Client()
    job = client.load_table_from_dataframe(df, table_id)
    job.result()

    print(f"Loaded {df.shape[0]} rows to {table_id}")
