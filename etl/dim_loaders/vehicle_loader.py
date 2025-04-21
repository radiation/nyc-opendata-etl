import pandas as pd
from etl.core.dim_loader import BaseDimLoader
from etl.core.utils import hash_key, normalize_strings


class VehicleDimLoader(BaseDimLoader):
    def __init__(self) -> None:
        super().__init__("vehicle_dim")

    def extract(self, df: pd.DataFrame) -> pd.DataFrame:
        required_cols = ["plate", "state", "license_type"]

        if not all(col in df.columns for col in required_cols):
            print("Skipping VehicleDimLoader — missing columns.")
            return pd.DataFrame(columns=required_cols)

        return df[required_cols].drop_duplicates().copy()

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        columns = ["plate", "state", "license_type"]
        df = normalize_strings(df, columns)
        df["Vehicle_Key"] = df.apply(lambda row: hash_key(row, columns), axis=1)
        return df[["Vehicle_Key"] + columns]
