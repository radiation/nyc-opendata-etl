import pandas as pd
from etl.dim_loader import BaseDimLoader
from etl.utils import hash_key


class VehicleDimLoader(BaseDimLoader):
    def __init__(self) -> None:
        super().__init__("vehicle_dim")

    def extract(self, df: pd.DataFrame) -> pd.DataFrame:
        return df[["plate", "state", "license_type"]].drop_duplicates().copy()

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df["Vehicle_Key"] = df.apply(hash_key, axis=1)
        return df[["Vehicle_Key", "plate", "state", "license_type"]]
