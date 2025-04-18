import pandas as pd
from etl.core.dim_loader import BaseDimLoader
from etl.core.utils import hash_key


class LocationDimLoader(BaseDimLoader):
    def __init__(self) -> None:
        super().__init__("location_dim")

    def extract(self, df: pd.DataFrame) -> pd.DataFrame:
        return df[
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
        ].drop_duplicates().copy()

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()

        # Ensure latitude and longitude are numeric
        df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
        df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")

        df["Location_Key"] = df.apply(hash_key, axis=1)

        return df[
            [
                "Location_Key",
                "borough", "city", "incident_zip",
                "street_name", "incident_address",
                "cross_street_1", "cross_street_2",
                "intersection_street_1", "intersection_street_2",
                "latitude", "longitude"
            ]
        ]
