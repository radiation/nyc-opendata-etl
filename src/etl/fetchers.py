import os
from datetime import datetime
from typing import Optional

import pandas as pd

from socrata.client import SocrataClient

# Ensure API_TOKEN is a real str (not Optional[str])
_API_TOKEN: Optional[str] = os.getenv("NYC_API_TOKEN")
if _API_TOKEN is None:
    raise RuntimeError("NYC_API_TOKEN environment variable must be set")
API_TOKEN: str = _API_TOKEN

DOMAIN: str = os.getenv("SOCRATA_DOMAIN", "data.cityofnewyork.us")

PARKING_FY_DATASETS = {
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

client = SocrataClient(domain=DOMAIN, app_token=API_TOKEN)


def _get_fiscal_year(date: datetime) -> int:
    return date.year + 1 if date.month >= 7 else date.year


def fetch_311_complaints(start: str, end: str, limit=1_000_000) -> pd.DataFrame:
    """Fetch 311 complaints between two timestamps."""
    records = client.fetch(
        "fhrw-4uyv",
        start=start,
        end=end,
        date_field="created_date",
        limit=limit
    )
    return pd.DataFrame.from_records(records)


def fetch_parking(start: str, end: str, limit=1_500_000) -> pd.DataFrame:
    start_dt = datetime.strptime(start, "%Y-%m-%dT%H:%M:%S.%f")
    end_dt = datetime.strptime(end, "%Y-%m-%dT%H:%M:%S.%f")

    start_fy = _get_fiscal_year(start_dt)
    end_fy = _get_fiscal_year(end_dt)

    dfs: list[pd.DataFrame] = []

    for fy in range(start_fy, end_fy + 1):
        dataset_id = PARKING_FY_DATASETS.get(fy)
        if not dataset_id:
            print(f"⚠️ Skipping FY{fy} — no dataset available")
            continue

        print(f"📄 Fetching parking data for FY{fy}")
        df = client.fetch(
            dataset_id=dataset_id,
            start=start,
            end=end,
            date_field="issue_date",
            limit=limit,
        )
        dfs.append(pd.DataFrame.from_records(df))

    combined = pd.concat(dfs, ignore_index=True)
    combined.drop_duplicates(subset="summons_number", inplace=True)
    return combined


def fetch_parking_with_fines(start: str, end: str, limit=1_000_000) -> pd.DataFrame:
    """Fetch camera and parking ticket fines (merged)."""
    records = client.fetch(
        "nc67-uf89",
        start=start,
        end=end,
        date_field="issue_date",
        limit=limit
    )
    return pd.DataFrame.from_records(records)
