import pandas as pd

from etl.core.bq_loader import load_df_to_bq
from etl.core.column_spec import ColumnSpec


class GenericDimLoader:
    def __init__(
        self,
        name: str,
        specs: list[ColumnSpec],
        natural_keys: list[str],
        key_name: str,
        bq_table: str,
    ):
        self.name = name
        self.specs = specs
        self.natural_keys = natural_keys
        self.key_name = key_name
        self.bq_table = bq_table

    def run(self, df: pd.DataFrame) -> pd.DataFrame:
        # Isolate & normalize
        df = df.copy()
        df.columns = (
            df.columns.str.strip().str.lower().str.replace(" ", "_", regex=False)
        )
        df = df[[s.name for s in self.specs if s.name in df.columns]]

        for spec in self.specs:
            col = spec.name
            if spec.dtype == "string":
                df[col] = df[col].astype("string").str.strip().str.lower()
            elif spec.dtype == "numeric":
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
            else:
                # date‐type dims are rare; handle if needed
                raise ValueError(f"Unsupported dtype {spec.dtype!r} in dim {self.name}")

        # Dedupe on the natural keys
        df = df.drop_duplicates(subset=self.natural_keys, ignore_index=True)

        # Generate surrogate key
        df[self.key_name] = pd.RangeIndex(start=1, stop=len(df) + 1)

        # Load to BQ and return the ready‐to‐join frame
        load_df_to_bq(df, self.bq_table)
        return df
