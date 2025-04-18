import pandas as pd
from etl.dim_loader import BaseDimLoader
from etl.utils import hash_key


class ParkingLocationDimLoader(BaseDimLoader):
    def __init__(self) -> None:
        super().__init__("parking_location_dim")

    def extract(self, df: pd.DataFrame) -> pd.DataFrame:
        return df[["borough", "precinct"]].drop_duplicates().copy()

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df["Parking_Location_Key"] = df.apply(hash_key, axis=1)
        return df[["Parking_Location_Key", "borough", "precinct"]]
