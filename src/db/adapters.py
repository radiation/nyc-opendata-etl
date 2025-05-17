from sqlalchemy import create_engine
from .staging import StagingAdapter
import pandas as pd

class SQLiteAdapter(StagingAdapter):
    def __init__(self) -> None:
        # in-memory; use 'sqlite:///myfile.db' if you prefer a temp file
        self.engine = create_engine("sqlite:///:memory:", echo=False)
        self.schema = None  # SQLite doesn’t use schemas

    def load_dim(self, df: pd.DataFrame, table: str, truncate: bool = False) -> None:
        if truncate:
            with self.engine.begin() as conn:
                conn.execute(f"DROP TABLE IF EXISTS {table}")
        df.to_sql(table, self.engine, if_exists="append", index=False)

    def load_fact(self, df: pd.DataFrame, table: str, truncate: bool = False) -> None:
        self.load_dim(df, table, truncate)
