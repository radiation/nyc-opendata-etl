from __future__ import annotations
from datetime import datetime
import pandas as pd

from socrata.client import fetch_dataset

# your mapping of fiscal year → dataset ID
PARKING_DATASETS: dict[int, str] = {
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

def fiscal_year(dt: datetime) -> int:
    # FY runs July 1 to June 30
    year = dt.year
    return year + 1 if dt.month >= 7 else year

def get_parking_data_between(
    start: str,
    end: str,
    limit: int = 5_000_000,
) -> pd.DataFrame:
    """
    Fetch parking enforcement across all fiscal-year 
    slices that overlap the [start, end) window.
    """
    start_dt = datetime.fromisoformat(start)
    end_dt   = datetime.fromisoformat(end)

    records: list[pd.DataFrame] = []
    # determine all FYs we touch
    fy_start = fiscal_year(start_dt)
    fy_end   = fiscal_year(end_dt)
    for fy in range(fy_start, fy_end + 1):
        ds_id = PARKING_DATASETS.get(fy)
        if not ds_id:
            continue

        # build a SoQL on the issue_date column
        where = (
            f"issue_date >= '{start_dt.strftime('%Y-%m-%dT%H:%M:%S.000')}' "
            f"AND issue_date <  '{end_dt.strftime(  '%Y-%m-%dT%H:%M:%S.000')}'"
        )
        print(f"Fetching parking FY{fy} ({ds_id}) between {start} → {end}")
        df_slice = fetch_dataset(ds_id, where=where, limit=limit)
        records.append(df_slice)

    if not records:
        return pd.DataFrame()
    return pd.concat(records, ignore_index=True)
