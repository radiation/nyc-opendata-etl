import pandas as pd

from etl.core.dim_loader import BaseDimLoader
from etl.core.utils import hash_key, normalize_strings


class ComplaintDimLoader(BaseDimLoader):
    def __init__(self) -> None:
        super().__init__("complaint_dim")

    def extract(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df[["complaint_type", "descriptor", "location_type"]]
            .drop_duplicates()
            .copy()
        )

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        columns = ["complaint_type", "descriptor", "location_type"]
        df = normalize_strings(df, columns)
        df["Complaint_Key"] = df.apply(lambda row: hash_key(row, columns), axis=1)
        return df[["Complaint_Key"] + columns]
