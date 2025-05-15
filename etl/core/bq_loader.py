from google.cloud import bigquery
import pandas as pd

from config import load_config


def slice_and_load(df: pd.DataFrame, columns: list[str], table_key: str) -> None:
    df2 = df[[c for c in columns if c in df.columns]]
    load_df_to_bq(df2, table_key)


def make_table_id(cfg: dict, table_key: str) -> str:
    bq = cfg["bigquery"]
    name = cfg["tables"][table_key]
    return f"{bq['project_id']}.{bq['dataset']}.{name}"


def load_df_to_bq(df: pd.DataFrame, table_key: str) -> None:
    """Dedupe, drop temp cols, load to BigQuery table identified by `table_key`."""
    df = df.loc[:, ~df.columns.duplicated()]
    if "__join_key__" in df.columns:
        df = df.drop(columns="__join_key__")

    # build table_id and run load
    cfg = load_config()
    table_id = make_table_id(cfg, table_key)

    client = bigquery.Client()
    job = client.load_table_from_dataframe(df, table_id)
    job.result()
    print(f"Loaded {df.shape[0]} rows to {table_id}")
