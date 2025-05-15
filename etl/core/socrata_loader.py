import os
from typing import Any, Dict, Optional

import pandas as pd
from sodapy import Socrata

SOCRATA_DOMAIN: str = os.getenv("SOCRATA_DOMAIN", "data.cityofnewyork.us")
SOCRATA_APP_TOKEN: Optional[str] = os.getenv("NYC_API_TOKEN")


def fetch_from_socrata(
    dataset_id: str,
    start: Optional[str] = None,
    end: Optional[str] = None,
    domain: Optional[str] = None,
    app_token: Optional[str] = None,
) -> pd.DataFrame:
    domain = domain or SOCRATA_DOMAIN
    if not domain:
        raise RuntimeError("SOCRATA_DOMAIN not set in environment or passed in")

    token = app_token or os.getenv("NYC_API_TOKEN")
    if not token:
        raise RuntimeError("NYC_API_TOKEN not set in environment or passed in")

    client = Socrata(domain, token)

    where: Optional[str]
    if start and end:
        where = f"created_date BETWEEN '{start}' AND '{end}'"
    else:
        where = None

    print(f"Fetching data from {domain}/{dataset_id} with where={where}")
    results: list[dict[str, Any]] = client.get(
        dataset_id,
        where=where,
        limit=10_000_000,
    )
    return pd.DataFrame.from_records(results)


def fetch_parking_from_socrata(
    datasets_by_year: Dict[int, str],
    start: Optional[str] = None,
    end: Optional[str] = None,
) -> pd.DataFrame:
    """
    Loop over year:dataset_id mappings and fetch & concat all parking records
    in the given time window (or entire table if no bounds).
    """
    frames: list[pd.DataFrame] = []
    for _year, ds_id in datasets_by_year.items():
        df_year = fetch_from_socrata(ds_id, start=start, end=end)
        if not df_year.empty:
            frames.append(df_year)
    if frames:
        return pd.concat(frames, ignore_index=True)
    return pd.DataFrame()
