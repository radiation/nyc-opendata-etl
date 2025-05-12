import pandas as pd
from etl.core.dim_loader import BaseDimLoader
from etl.core.utils import normalize_strings

class ViolationDimLoader(BaseDimLoader):
    def __init__(self) -> None:
        super().__init__("violation_dim")

    def extract(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        # if Socrata JSON dropped the column because it's always null, re-add it:
        if "violation_description" not in df.columns:
            df["violation_description"] = pd.NA
        # now safe to slice both columns
        return (
            df[["violation_code", "violation_description"]]
            .drop_duplicates(subset=["violation_code"])
        )

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        # normalize description text (if any)
        df = normalize_strings(df, ["violation_description"])

        # 1) coerce code → pandas Int64, drop any rows where that fails
        df["violation_code"] = pd.to_numeric(
            df["violation_code"], errors="coerce"
        )
        df = df.dropna(subset=["violation_code"])

        # 2) cast to a plain numpy int64 array
        df["violation_code"] = df["violation_code"].astype("int64")

        return df[["violation_code", "violation_description"]]
    