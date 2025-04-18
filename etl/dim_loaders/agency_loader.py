import pandas as pd
from etl.core.dim_loader import BaseDimLoader
from etl.core.utils import hash_key


class AgencyDimLoader(BaseDimLoader):
    def __init__(self) -> None:
        super().__init__("agency_dim")

    def extract(self, df: pd.DataFrame) -> pd.DataFrame:
        return df[["agency", "agency_name"]].drop_duplicates().copy()

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df["Agency_Key"] = df.apply(hash_key, axis=1)
        df = df[["Agency_Key", "agency", "agency_name"]]
        return df
