import pandas as pd
from etl.core.dim_loader import BaseDimLoader
from etl.core.utils import hash_key, normalize_strings


class VehicleDimLoader(BaseDimLoader):
    def __init__(self) -> None:
        super().__init__("vehicle_dim")

    def extract(self, df: pd.DataFrame) -> pd.DataFrame:
        raw_cols = ["plate_id", "registration_state", "plate_type"]
        missing = set(raw_cols) - set(df.columns)
        if missing:
            print(f"Skipping VehicleDimLoader — missing columns: {sorted(missing)}")
            return pd.DataFrame(columns=raw_cols)
        # grab & dedupe
        out = df[raw_cols].drop_duplicates().copy()
        # rename to match your BQ dim schema
        out.rename(columns={
            "plate_id": "plate",
            "registration_state": "state",
            "plate_type": "license_type"
        }, inplace=True)
        out = out.assign(
        vehicle_body_type = df["vehicle_body_type"],
        vehicle_make      = df["vehicle_make"],
        vehicle_year      = df["vehicle_year"].astype("Int64"),
        vehicle_color     = df["vehicle_color"],
        unregistered      = df["unregistered_vehicle"].map({"Yes": True, "No": False})
        )
        return out

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        key_cols = ["plate", "state", "license_type"]
        df = normalize_strings(df, key_cols)
        df = df.dropna(subset=key_cols)
        df["vehicle_key"] = df.apply(lambda row: hash_key(row, key_cols), axis=1)
        # return the key, the natural-key cols, AND the extra attrs
        return df[
            ["vehicle_key"] 
            + key_cols 
            + [
                "vehicle_body_type",
                "vehicle_make",
                "vehicle_year",
                "vehicle_color",
                "unregistered",
            ]
        ]
