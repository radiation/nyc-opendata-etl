from __future__ import annotations
from abc import ABC, abstractmethod
import pandas as pd

from google.cloud import bigquery
from sqlalchemy import create_engine, text

class StagingAdapter(ABC):
    @abstractmethod
    def load_dim(self, df: pd.DataFrame, table: str, *, truncate: bool = False) -> None: ...
    @abstractmethod
    def load_fact(self, df: pd.DataFrame, table: str, *, truncate: bool = False) -> None: ...

class BigQueryAdapter(StagingAdapter):
    def __init__(self, client: bigquery.Client, dataset: str) -> None:
        self.client = client
        self.dataset = dataset

    def _load(self, df: pd.DataFrame, table: str, truncate: bool) -> None:
        null_cols = df.columns[df.isna().all()].tolist()
        if null_cols:
            print(f"[BQ] Columns with all-null values: {null_cols}")

        df = df.dropna(axis=1, how="all")

        if df.empty:
            print(f"[BQ] no rows for {table}")
            return
        cfg = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE" if truncate else "WRITE_APPEND"
        )
        table_id = f"{self.client.project}.{self.dataset}.{table}"
        job = self.client.load_table_from_dataframe(df, table_id, job_config=cfg)
        job.result()
        print(f"[BQ] loaded {len(df)} rows into {table_id}")

    def load_dim(self, df: pd.DataFrame, table: str, *, truncate: bool = False) -> None:
        self._load(df, table, truncate)

    def load_fact(self, df: pd.DataFrame, table: str, *, truncate: bool = False) -> None:
        self._load(df, table, truncate)

class SQLiteAdapter(StagingAdapter):
    def __init__(self) -> None:
        self.engine = create_engine("sqlite:///:memory:", echo=False)

    def load_dim(self, df: pd.DataFrame, table: str, *, truncate: bool = False) -> None:
        if truncate:
            with self.engine.begin() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {table}"))
        df.to_sql(table, self.engine, if_exists="append", index=False)
        print(f"[SQLITE] loaded {len(df)} rows into {table}")

    def load_fact(self, df: pd.DataFrame, table: str, *, truncate: bool = False) -> None:
        # same as load_dim for now
        self.load_dim(df, table, truncate=truncate)
