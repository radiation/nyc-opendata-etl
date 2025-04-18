import pandas as pd
from etl.dim_loader import BaseDimLoader
from etl.utils import hash_key


class ViolationDimLoader(BaseDimLoader):
    def __init__(self) -> None:
        super().__init__("violation_dim")

    def extract(self, df: pd.DataFrame) -> pd.DataFrame:
        return df[["violation_code", "violation_description"]].drop_duplicates().copy()

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df["Violation_Key"] = df.apply(hash_key, axis=1)
        return df[["Violation_Key", "violation_code", "violation_description"]]
