import pandas as pd
from etl.dim_loader import BaseDimLoader
from etl.utils import hash_key


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
        df["Location_Key"] = df.apply(hash_key, axis=1)
        # Reorder columns to match BigQuery table schema
        cols = ["Location_Key"] + [col for col in df.columns if col != "Location_Key"]
        return df[cols]
