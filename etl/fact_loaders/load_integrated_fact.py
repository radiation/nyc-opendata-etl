import pandas as pd
from google.cloud import bigquery
from config import load_config


def load_to_bigquery(df: pd.DataFrame) -> None:
    """
    Loads a DataFrame to the Integrated_Fact_Service_Requests BigQuery table.
    """
    cfg = load_config()
    project_id = cfg["bigquery"]["project_id"]
    dataset = cfg["bigquery"]["dataset"]
    table_name = cfg["tables"]["integrated_fact_service_requests"]
    table_id = f"{project_id}.{dataset}.{table_name}"

    client = bigquery.Client()
    # Ensure no temporary columns
    if "__join_key__" in df.columns:
        df = df.drop(columns="__join_key__")

    job = client.load_table_from_dataframe(df, table_id)
    job.result()

    print(f"Loaded {df.shape[0]} rows to {table_id}")
