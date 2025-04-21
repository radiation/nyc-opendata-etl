import pandas as pd
from etl.core.dim_loader import BaseDimLoader
from etl.core.utils import hash_key, normalize_strings


class ViolationDimLoader(BaseDimLoader):
    def __init__(self) -> None:
        super().__init__("violation_dim")

    def extract(self, df: pd.DataFrame) -> pd.DataFrame:
        # If description is missing, fall back to just violation_code or violation
        if "violation_code" in df.columns:
            if "violation_description" in df.columns:
                return df[["violation_code", "violation_description"]].drop_duplicates().copy()
            else:
                return df[["violation_code"]].drop_duplicates().copy()
        elif "violation" in df.columns:
            # Some datasets use 'violation' instead of 'violation_code'
            return df[["violation"]].rename(columns={"violation": "violation_code"}).drop_duplicates().copy()

        print("Skipping ViolationDimLoader — no known columns found.")
        return pd.DataFrame(columns=["violation_code", "violation_description"])

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        columns = ["violation_code", "violation_description"]
        df = normalize_strings(df, columns)
        df["Violation_Key"] = df.apply(lambda row: hash_key(row, columns), axis=1)
        return df[["Violation_Key"] + columns]
