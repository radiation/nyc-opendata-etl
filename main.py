# main.py
from datetime import datetime, timedelta
import argparse
from typing import Optional, Dict

import pandas as pd

from etl.fact_loaders.load_311 import (
    get_311_data_between,
    get_yesterdays_311_data,
    clean_311_data,
    load_to_bigquery as load_311_fact,
)
from etl.fact_loaders.load_parking import (
    get_parking_data_between,
    get_yesterdays_parking_data,
    clean_parking_data,
    load_to_bigquery as load_parking_fact,
)
from etl.core.key_mapper import assign_keys
from etl.core.utils import normalize_strings

from etl.dim_loaders.agency_loader import AgencyDimLoader
from etl.dim_loaders.complaint_loader import ComplaintDimLoader
from etl.dim_loaders.location_loader import LocationDimLoader
from etl.dim_loaders.vehicle_loader import VehicleDimLoader
from etl.dim_loaders.violation_loader import ViolationDimLoader
from etl.dim_loaders.parking_location_loader import ParkingLocationDimLoader
from etl.dim_loaders.date_loader import DateDimLoader
from etl.dim_loaders.time_loader import TimeDimLoader


def load_date_and_time_dims() -> None:
    # (unchanged)
    date_loader = DateDimLoader()
    start_date = datetime(2010, 1, 1)
    end_date = datetime.today() + timedelta(days=365)
    df_dates = date_loader.generate_date_range(start_date, end_date)
    date_loader.load(df_dates)

    time_loader = TimeDimLoader()
    df_times = time_loader.generate_time_range()
    time_loader.load(df_times)


def load_dimensions(
    df_311: pd.DataFrame, df_parking: pd.DataFrame
) -> Dict[str, pd.DataFrame]:
    loaders = {
        "agency": (AgencyDimLoader(), pd.concat([df_311, df_parking], ignore_index=True)),
        "complaint": (ComplaintDimLoader(), df_311),
        "location": (LocationDimLoader(), df_311),
        "vehicle": (VehicleDimLoader(), df_parking),
        "violation": (ViolationDimLoader(), df_parking),
        "parking_location": (ParkingLocationDimLoader(), df_parking),
    }

    dims: Dict[str, pd.DataFrame] = {}
    for name, (loader, src) in loaders.items():
        print(f"\nRunning {loader.__class__.__name__}…")
        ext = loader.extract(src)
        if ext.empty:
            print(f"No data for {loader.table_id}")
        else:
            tf = loader.transform(ext)
            loader.load(tf)
            dims[name] = tf
    return dims


def main(start: Optional[str] = None, end: Optional[str] = None) -> None:
    print("Running ETL for NYC Open Data…")
    load_date_and_time_dims()

    # 1) Fetch raw slices
    if start and end:
        raw_311 = get_311_data_between(start, end)
        raw_parking = get_parking_data_between(start, end)
    else:
        raw_311 = get_yesterdays_311_data()
        raw_parking = get_yesterdays_parking_data()

    # 2) Normalize joinable fields in raw_parking so dimensions and keys align
    raw_parking = normalize_strings(
        raw_parking,
        [
            "plate_id", "registration_state", "plate_type",
            "violation_code", "violation_description",
            "house_number", "street_name", "intersecting_street",
            "violation_county", "violation_precinct",
        ],
    )
    raw_parking["violation_code"] = (
        pd.to_numeric(raw_parking["violation_code"], errors="coerce")
        .astype("Int64")
    )

    # 3) Load all dims off the full raw sets
    dim_data = load_dimensions(raw_311, raw_parking)

    # ── 311 FACT ────────────────────────────────────────────────────────────────
    cleaned_311 = clean_311_data(raw_311) if not raw_311.empty else pd.DataFrame()

    if not cleaned_311.empty:
        # stamp FK columns
        cleaned_311 = assign_keys(
            cleaned_311,
            dim_data["agency"],
            ["agency", "agency_name"],
            "agency_key",
        )

        # guarantee the column exists
        if "location_type" not in cleaned_311.columns:
            cleaned_311["location_type"] = ""
        # turn any NaN into a real string

        cleaned_311["location_type"] = cleaned_311["location_type"].fillna("")
        cleaned_311 = assign_keys(
            cleaned_311,
            dim_data["complaint"],
            ["complaint_type", "descriptor", "location_type"],
            "complaint_key",
        )
        cleaned_311 = assign_keys(
            cleaned_311,
            dim_data["location"],
            [
                "borough", "city", "incident_zip", "street_name",
                "incident_address", "cross_street_1", "cross_street_2",
                "intersection_street_1", "intersection_street_2",
                "latitude", "longitude",
            ],
            "location_key",
        )

        # slice to your fact schema
        fact_311_cols = [
            "unique_key",
            "created_date_key", "created_time_key",
            "closed_date_key", "closed_time_key",
            "agency_key", "complaint_key", "location_key",
            "resolution_action_date", "due_date", "closed_timestamp",
        ]
        fact_311 = cleaned_311[[c for c in fact_311_cols if c in cleaned_311.columns]]
        load_311_fact(fact_311)

    # ── PARKING FACT ────────────────────────────────────────────────────────────
    cleaned_parking = clean_parking_data(raw_parking) if not raw_parking.empty else pd.DataFrame()

    if not cleaned_parking.empty:
        # rename for VehicleDim natural key
        cleaned_parking.rename(
            columns={
                "plate_id": "plate",
                "registration_state": "state",
                "plate_type": "license_type",
            },
            inplace=True,
        )

        # Vehicle FK
        cleaned_parking = assign_keys(
            cleaned_parking,
            dim_data["vehicle"],
            ["plate", "state", "license_type"],
            "vehicle_key",
        )

        # slice to your parking fact schema
        fact_parking_cols = [
            "summons_number",
            "date_key", "time_key",
            "violation_code",  # natural key
            "location_key",    # from clean_parking_data
            "vehicle_key",
        ]
        fact_parking = cleaned_parking[
            [c for c in fact_parking_cols if c in cleaned_parking.columns]
        ]
        load_parking_fact(fact_parking)

    print("ETL complete!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run NYC Open Data ETL")
    parser.add_argument("--start", type=str, help="Start timestamp (e.g. 2023-01-01T00:00:00.000)")
    parser.add_argument("--end", type=str, help="End timestamp (e.g. 2023-01-02T00:00:00.000)")
    args = parser.parse_args()
    main(start=args.start, end=args.end)
