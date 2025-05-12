import pandas as pd
from etl.core.dim_loader import BaseDimLoader
from etl.core.utils import hash_key, normalize_strings


class AgencyDimLoader(BaseDimLoader):
    def __init__(self) -> None:
        super().__init__("agency_dim")

    def extract(self, df: pd.DataFrame) -> pd.DataFrame:
        return df[["agency", "agency_name"]].drop_duplicates().copy()

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        columns = ["agency", "agency_name"]
        df = normalize_strings(df, columns)
        df["agency_key"] = df.apply(lambda row: hash_key(row, columns), axis=1)
        return df[["agency_key"] + columns]
