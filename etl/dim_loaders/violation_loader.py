import pandas as pd
from etl.core.dim_loader import BaseDimLoader
from etl.core.utils import normalize_strings

class ViolationDimLoader(BaseDimLoader):
    def __init__(self) -> None:
        super().__init__("violation_dim")

    def extract(self, df: pd.DataFrame) -> pd.DataFrame:
        # pull code + description, keep rows even if description is null
        out = df[["violation_code", "violation_description"]].copy()
        return out.drop_duplicates(subset=["violation_code"])

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        # normalize text, cast code to int
        df = normalize_strings(df, ["violation_description"])
        # cast codes to int, coercing any malformed to NA
        df["violation_code"] = pd.to_numeric(df["violation_code"], errors="coerce").astype("Int64")
        return df[["violation_code", "violation_description"]]
