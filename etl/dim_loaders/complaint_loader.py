import pandas as pd
from etl.dim_loader import BaseDimLoader
from etl.utils import hash_key


class ComplaintDimLoader(BaseDimLoader):
    def __init__(self) -> None:
        super().__init__("complaint_dim")

    def extract(self, df: pd.DataFrame) -> pd.DataFrame:
        return df[["complaint_type", "descriptor", "location_type"]].drop_duplicates().copy()

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df["Complaint_Key"] = df.apply(hash_key, axis=1)
        df = df[["Complaint_Key", "complaint_type", "descriptor", "location_type"]]
        return df
