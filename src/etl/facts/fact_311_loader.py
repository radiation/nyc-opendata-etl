from datetime import datetime
import pandas as pd

from socrata.client import fetch_dataset


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
    df: pd.DataFrame = fetch_dataset("erm2-nwe9", where=where, limit=limit)
    print(f"Fetched {len(df)} records")
    return df
