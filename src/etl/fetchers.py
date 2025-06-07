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


def fetch_generic_data(
    dataset_id: str,
    start: str,
    end: str,
    date_field: str = "created_date",
    limit: int = 1_000_000,
) -> pd.DataFrame:
    """
    Fetch generic data from a Socrata dataset.

    Args:
        dataset_id: The Socrata dataset ID to fetch.
        start: Start timestamp in ISO format (e.g., "2023-01-01T00:00:00.000").
        end: End timestamp in ISO format (e.g., "2023-12-31T23:59:59.999").
        date_field: The field to filter by date (default is "created_date").
        limit: Maximum number of records to fetch (default is 1,000,000).

    Returns:
        A DataFrame containing the fetched records.
    """
    records = client.fetch(
        dataset_id=dataset_id,
        start=start,
        end=end,
        date_field=date_field,
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
        df = fetch_generic_data(
            dataset_id=dataset_id,
            start=start,
            end=end,
            date_field="issue_date",
            limit=limit
        )
        dfs.append(pd.DataFrame.from_records(df))

    combined = pd.concat(dfs, ignore_index=True)
    combined.drop_duplicates(subset="summons_number", inplace=True)
    return combined
