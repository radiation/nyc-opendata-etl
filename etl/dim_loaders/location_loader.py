import pandas as pd

from etl.core.dim_loader import BaseDimLoader
from etl.core.utils import hash_key, normalize_strings


class LocationDimLoader(BaseDimLoader):
    def __init__(self) -> None:
        super().__init__("location_dim")

    def extract(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df[
                [
                    "borough",
                    "city",
                    "incident_zip",
                    "street_name",
                    "incident_address",
                    "cross_street_1",
                    "cross_street_2",
                    "intersection_street_1",
                    "intersection_street_2",
                    "latitude",
                    "longitude",
                ]
            ]
            .drop_duplicates()
            .copy()
        )

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        string_columns = [
            "borough",
            "city",
            "incident_zip",
            "street_name",
            "incident_address",
            "cross_street_1",
            "cross_street_2",
            "intersection_street_1",
            "intersection_street_2",
        ]

        # Normalize text fields
        df = normalize_strings(df, string_columns)

        # Coerce lat/lon to numeric
        df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
        df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")

        # Hash based only on string columns (not lat/lon)
        df["Location_Key"] = df.apply(lambda row: hash_key(row, string_columns), axis=1)

        return df[["Location_Key"] + string_columns + ["latitude", "longitude"]]
