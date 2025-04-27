import pandas as pd

from config import load_config
from etl.core.bq_loader import load_df_to_bq


def load_to_bigquery(df: pd.DataFrame) -> None:
    cfg = load_config()
    table_id = f"{cfg['bigquery']['project_id']}.{cfg['bigquery']['dataset']}.{cfg['tables']['Fact_Integrated_Requests']}"
    load_df_to_bq(df, table_id)
