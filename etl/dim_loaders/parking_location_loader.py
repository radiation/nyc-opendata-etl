import pandas as pd

from etl.core.dim_loader import BaseDimLoader
from etl.core.utils import hash_key, normalize_strings


class ParkingLocationDimLoader(BaseDimLoader):
    def __init__(self) -> None:
        super().__init__("parking_location_dim")

    def extract(self, df: pd.DataFrame) -> pd.DataFrame:
        required_cols = ["borough", "precinct"]

        if not all(col in df.columns for col in required_cols):
            print("Skipping ParkingLocationDimLoader — missing columns.")
            return pd.DataFrame(columns=required_cols)

        return df[required_cols].drop_duplicates().copy()

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        columns = ["borough", "precinct"]
        df = normalize_strings(df, columns)
        df = df.dropna(subset=columns)
        df["Parking_Location_Key"] = df.apply(
            lambda row: hash_key(row, columns), axis=1
        )
        return df[["Parking_Location_Key"] + columns]
