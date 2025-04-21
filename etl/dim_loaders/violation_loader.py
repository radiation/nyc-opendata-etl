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
        # Only use fields that actually exist
        available_fields = [col for col in ["violation_code", "violation_description"] if col in df.columns]

        if not available_fields:
            print("ViolationDimLoader: No valid columns found — skipping transform.")
            return pd.DataFrame(columns=["Violation_Key"])

        df = normalize_strings(df, available_fields)
        df["Violation_Key"] = df.apply(lambda row: hash_key(row, available_fields), axis=1)

        return df[["Violation_Key"] + available_fields]

