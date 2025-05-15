import argparse
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd

from etl.core.bq_loader import load_df_to_bq
from etl.core.socrata_loader import fetch_from_socrata, fetch_parking_from_socrata
from etl.dim_loader import GenericDimLoader
from etl.dim_loaders.date_loader import DateDimLoader
from etl.specs.dimensions import (
    AGENCY_NATURAL_KEYS,
    AGENCY_SPECS,
    AGENCY_TABLE,
    COMPLAINT_NATURAL_KEYS,
    COMPLAINT_SPECS,
    COMPLAINT_TABLE,
    DIMENSION_CONFIGS,
    LOCATION_NATURAL_KEYS,
    LOCATION_SPECS,
    LOCATION_TABLE,
    PARKING_LOCATION_NATURAL_KEYS,
    PARKING_LOCATION_SPECS,
    PARKING_LOCATION_TABLE,
    VEHICLE_NATURAL_KEYS,
    VEHICLE_SPECS,
    VEHICLE_TABLE,
    VIOLATION_NATURAL_KEYS,
    VIOLATION_SPECS,
    VIOLATION_TABLE,
)
from etl.specs.facts import (
    DATASET_311,
    DATASETS_PARKING,
    FACT_INT_TABLE,
    fact_311_loader,
    fact_parking_loader,
)

DIM_LOADERS: list[GenericDimLoader] = [
    GenericDimLoader(
        "agency", AGENCY_SPECS, AGENCY_NATURAL_KEYS, "agency_key", AGENCY_TABLE
    ),
    GenericDimLoader(
        "complaint",
        COMPLAINT_SPECS,
        COMPLAINT_NATURAL_KEYS,
        "complaint_key",
        COMPLAINT_TABLE,
    ),
    GenericDimLoader(
        "location",
        LOCATION_SPECS,
        LOCATION_NATURAL_KEYS,
        "location_key",
        LOCATION_TABLE,
    ),
    GenericDimLoader(
        "vehicle", VEHICLE_SPECS, VEHICLE_NATURAL_KEYS, "vehicle_key", VEHICLE_TABLE
    ),
    GenericDimLoader(
        "violation",
        VIOLATION_SPECS,
        VIOLATION_NATURAL_KEYS,
        "violation_key",
        VIOLATION_TABLE,
    ),
    GenericDimLoader(
        "parking_location",
        PARKING_LOCATION_SPECS,
        PARKING_LOCATION_NATURAL_KEYS,
        "parking_location_key",
        PARKING_LOCATION_TABLE,
    ),
]


def load_date_dim() -> None:
    loader = DateDimLoader("dim_date")
    today = datetime.today()
    start_date = datetime(2010, 1, 1)
    end_date = today + timedelta(days=365)
    loader.run(start_date, end_date)


def load_dimensions(
    df_311: pd.DataFrame, df_parking: pd.DataFrame
) -> dict[str, pd.DataFrame]:
    # map each dimension name to its raw source slice
    src_map: dict[str, pd.DataFrame] = {
        "agency": pd.concat([df_311, df_parking], ignore_index=True),
        "complaint": df_311,
        "location": df_311,
        "vehicle": df_parking,
        "violation": df_parking,
        "parking_location": df_parking,
    }

    transformed: dict[str, pd.DataFrame] = {}

    for name, specs, nat_keys, key_name, bq_table in DIMENSION_CONFIGS:
        loader = GenericDimLoader(
            name=name,
            specs=specs,
            natural_keys=nat_keys,
            key_name=key_name,
            bq_table=bq_table,
        )

        src_df = src_map[name]
        if src_df.empty:
            print(f"No data for dimension '{name}' — skipping")
            continue

        print(f"Loading dimension '{name}'…")
        transformed[name] = loader.run(src_df)

    return transformed


def build_integrated(
    df311: pd.DataFrame, dfpark: pd.DataFrame
) -> Optional[pd.DataFrame]:
    frames: list[pd.DataFrame] = []
    if not df311.empty:
        frames.append(
            df311.assign(
                Request_ID=df311["unique_key"],
                Date_Key=df311["Created_Date_Key"],
                Request_Type="311",
                Amount_Due=pd.NA,
            )[
                [
                    "Request_ID",
                    "Date_Key",
                    "Agency_Key",
                    "Complaint_Key",
                    "Location_Key",
                    "Request_Type",
                    "resolution_description",
                    "Amount_Due",
                ]
            ]
        )
    if not dfpark.empty:
        ip = dfpark.copy()
        ip["Request_ID"] = ip["summons_number"].astype("Int64")
        ip["Date_Key"] = ip["Issue_Date_Key"]
        ip[["Agency_Key", "Complaint_Key"]] = pd.NA
        ip["Location_Key"] = ip.get("Parking_Location_Key", pd.NA)
        ip["Request_Type"] = "Parking"
        ip["resolution_description"] = pd.NA
        frames.append(
            ip[
                [
                    "Request_ID",
                    "Date_Key",
                    "Agency_Key",
                    "Complaint_Key",
                    "Location_Key",
                    "Request_Type",
                    "resolution_description",
                    "Amount_Due",
                ]
            ]
        )
    return pd.concat(frames, ignore_index=True) if frames else None


def main(start: Optional[str] = None, end: Optional[str] = None) -> None:
    # Fetch raw data
    raw_311 = fetch_from_socrata(DATASET_311, start, end)
    raw_parking = fetch_parking_from_socrata(DATASETS_PARKING, start, end)

    # Exit early if nothing to do
    if raw_311.empty and raw_parking.empty:
        print("No new data to process for 311 or parking.")
        return

    # Ensure date dimension is up to date
    load_date_dim()

    # Build & load all other dimensions
    dim_data = load_dimensions(raw_311, raw_parking)

    # Normalize, assign FKs, and push each fact table
    df311 = fact_311_loader.run(raw_311, dim_data)
    dfpark = fact_parking_loader.run(raw_parking, dim_data)

    # Build & load integrated fact table
    integrated = build_integrated(df311, dfpark)
    if integrated is not None:
        load_df_to_bq(integrated, FACT_INT_TABLE)

    print("ETL complete!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run NYC Open Data ETL")
    parser.add_argument(
        "--start",
        type=str,
        help="Start timestamp (e.g. 2023-01-01T00:00:00.000)",
    )
    parser.add_argument(
        "--end",
        type=str,
        help="End timestamp (e.g. 2023-01-02T00:00:00.000)",
    )
    args = parser.parse_args()
    main(start=args.start, end=args.end)
