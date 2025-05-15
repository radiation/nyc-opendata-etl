from datetime import datetime, timedelta

import pandas as pd
from sodapy import Socrata

from config.env import NYC_API_TOKEN
from etl.core.bq_loader import load_df_to_bq
from etl.core.column_spec import ColumnSpec
from etl.core.key_mapper import assign_keys

OPENDATA_API_URL = "data.cityofnewyork.us"
RETURN_LIMIT = 10000000  # Monthly parking data exceeds 100k rows


class FactLoader:
    def __init__(
        self,
        table_name: str,
        specs: list[ColumnSpec],
        fk_map: list[tuple[list[str], str]],
        bq_table: str,
    ):
        self.specs = specs
        self.table_name = table_name
        self.fk_map = fk_map
        self.bq_table = bq_table

    def run(self, df: pd.DataFrame, dim_data: dict[str, pd.DataFrame]) -> pd.DataFrame:
        df = df.copy()
        df = normalize_data(df, self.specs)

        for join_cols, key_name in self.fk_map:
            df = assign_keys(
                df, dim_data[key_name.lower().replace("_key", "")], join_cols, key_name
            )

        df = df.drop(
            columns=[c for c in df.columns if c.startswith("__")], errors="ignore"
        )

        load_df_to_bq(df, self.bq_table)
        return df


def get_data_between(
    dataset: str, start: str, end: str, limit: int = RETURN_LIMIT
) -> pd.DataFrame:
    client = Socrata(OPENDATA_API_URL, NYC_API_TOKEN)
    where_clause = f"created_date >= '{start}' AND created_date < '{end}'"
    print(f"Fetching data from {dataset} between: {start} → {end}")
    results = client.get(dataset, where=where_clause, limit=limit)
    return pd.DataFrame.from_records(results)


def get_yesterdays_data(dataset: str) -> pd.DataFrame:
    today = datetime.utcnow().date()
    start = f"{today - timedelta(days=1)}T00:00:00.000"
    end = f"{today}T00:00:00.000"
    return get_data_between(dataset, start, end)


def normalize_data(df: pd.DataFrame, specs: list[ColumnSpec]) -> pd.DataFrame:
    df = df.copy()

    # Convert column names to lowercase and replace spaces with underscores
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_", regex=False)

    # Drop any columns not in the specs
    df = df[[spec.name for spec in specs if spec.name in df.columns]]

    # Normalize fields & set datatypes
    for spec in specs:
        col = spec.name
        dtype = spec.dtype

        # Create keys from dates
        if dtype == "date":
            df[col] = pd.to_datetime(df[col], errors="coerce")
            df[f"{col}_key"] = df[col].dt.strftime("%Y%m%d").astype("Int64")

        elif dtype == "numeric":
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

        elif dtype == "string":
            df[col] = df[col].astype("string").str.strip().str.lower()

        else:
            raise ValueError(f"Unknown dtype {dtype!r} for column {col!r}")

    return df
