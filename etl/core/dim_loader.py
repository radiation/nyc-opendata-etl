from abc import ABC, abstractmethod
from typing import Protocol

from google.cloud import bigquery
import pandas as pd

from config import load_config


class DimLoaderProtocol(Protocol):
    table_id: str

    def extract(self, df: pd.DataFrame) -> pd.DataFrame:
        ...

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        ...

    def load(self, df: pd.DataFrame) -> None:
        ...


class BaseDimLoader(ABC):
    def __init__(self, table_key: str) -> None:
        cfg = load_config()
        project = cfg["bigquery"]["project_id"]
        dataset = cfg["bigquery"]["dataset"]
        tables = cfg["tables"]
        self.table_id = f"{project}.{dataset}.{tables[table_key]}"
        self.client = bigquery.Client()

    @abstractmethod
    def extract(self, df: pd.DataFrame) -> pd.DataFrame:
        """Pull raw rows for this dimension out of the combined DataFrame."""

    @abstractmethod
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Sanitize and key the DataFrame for loading."""

    def load(self, df: pd.DataFrame) -> None:
        if df.empty:
            print(f"No data to load into {self.table_id}")
            return

        job = self.client.load_table_from_dataframe(
            df,
            self.table_id,
            job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND"),
        )
        job.result()
        print(f"Loaded {df.shape[0]} rows into {self.table_id}")
