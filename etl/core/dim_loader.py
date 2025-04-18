from abc import ABC
from typing import Protocol
import pandas as pd
from google.cloud import bigquery
from config import load_config


class DimLoaderProtocol(Protocol):
    def extract(self, df: pd.DataFrame) -> pd.DataFrame: ...
    def transform(self, df: pd.DataFrame) -> pd.DataFrame: ...
    def load(self, df: pd.DataFrame) -> None: ...


class BaseDimLoader(ABC):
    def __init__(self, table_key: str) -> None:
        cfg = load_config()
        project = cfg["bigquery"]["project_id"]
        dataset = cfg["bigquery"]["dataset"]
        tables = cfg["tables"]
        self.table_id = f"{project}.{dataset}.{tables[table_key]}"
        self.client = bigquery.Client()

    def load(self, df: pd.DataFrame) -> None:
        if df.empty:
            print(f"⚠️  No data to load into {self.table_id}")
            return

        job = self.client.load_table_from_dataframe(df, self.table_id, job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND"))
        job.result()
        print(f"✅ Loaded {df.shape[0]} rows into {self.table_id}")
