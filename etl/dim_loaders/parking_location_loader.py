import pandas as pd
from etl.core.dim_loader import BaseDimLoader
from etl.core.utils import hash_key, normalize_strings

class ParkingLocationDimLoader(BaseDimLoader):
    def __init__(self) -> None:
        super().__init__("parking_location_dim")

    def extract(self, df: pd.DataFrame) -> pd.DataFrame:
        required_cols = [
            "house_number",
            "street_name",
            "intersecting_street",
            "violation_county",
            "violation_precinct",
        ]

        if not set(required_cols).issubset(df.columns):
            print("Skipping ParkingLocationDimLoader — missing columns.")
            return pd.DataFrame(columns=required_cols)
        return df[required_cols].drop_duplicates().copy()

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        cols = [
            "house_number",
            "street_name",
            "intersecting_street",
            "violation_county",
            "violation_precinct",
        ]
        df = normalize_strings(df, cols)
        df = df.dropna(subset=cols)
        df["parking_location_key"] = df.apply(lambda row: hash_key(row, cols), axis=1)
        return df[["parking_location_key"] + cols]
