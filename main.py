import argparse
from datetime import datetime, timedelta
from typing import Optional, Tuple

import pandas as pd

from etl.constants import FACT_311_COLUMNS, LOCATION_DIM_COLUMNS, PARKING_FACT_COLUMNS
from etl.core.bq_loader import load_df_to_bq
from etl.core.dim_loader import DimLoaderProtocol
from etl.core.key_mapper import assign_keys
from etl.dim_loaders.agency_loader import AgencyDimLoader
from etl.dim_loaders.complaint_loader import ComplaintDimLoader
from etl.dim_loaders.date_loader import DateDimLoader
from etl.dim_loaders.location_loader import LocationDimLoader
from etl.dim_loaders.parking_location_loader import ParkingLocationDimLoader
from etl.dim_loaders.vehicle_loader import VehicleDimLoader
from etl.dim_loaders.violation_loader import ViolationDimLoader
from etl.fact_loaders.load_311 import (
    clean_311_data,
    get_311_data_between,
    get_yesterdays_311_data,
)
from etl.fact_loaders.load_parking import (
    clean_parking_data,
    get_parking_data_between,
    get_yesterdays_parking_data,
)


def load_date_dim() -> None:
    loader = DateDimLoader()
    today = datetime.today()
    start_date = datetime(2010, 1, 1)
    end_date = today + timedelta(days=365)
    df = loader.generate_date_range(start_date, end_date)
    loader.load(df)


def load_dimensions(
    df_311: pd.DataFrame, df_parking: pd.DataFrame
) -> dict[str, pd.DataFrame]:
    loaders: dict[str, Tuple[DimLoaderProtocol, pd.DataFrame]] = {
        "agency": (
            AgencyDimLoader(),
            pd.concat([df_311, df_parking], ignore_index=True),
        ),
        "complaint": (ComplaintDimLoader(), df_311),
        "location": (LocationDimLoader(), df_311),
        "vehicle": (
            VehicleDimLoader(),
            df_parking if not df_parking.empty else pd.DataFrame(),
        ),
        "violation": (
            ViolationDimLoader(),
            df_parking if not df_parking.empty else pd.DataFrame(),
        ),
        "parking_location": (
            ParkingLocationDimLoader(),
            df_parking if not df_parking.empty else pd.DataFrame(),
        ),
    }
    transformed: dict[str, pd.DataFrame] = {}
    for name, (loader, src) in loaders.items():
        print(f"Running {loader.__class__.__name__}...")
        df_ex = loader.extract(src)
        if df_ex.empty:
            print(f"No data to load into {loader.table_id}")
        else:
            df_tr = loader.transform(df_ex)
            loader.load(df_tr)
            transformed[name] = df_tr
    return transformed


def fetch_sources(
    start: Optional[str], end: Optional[str]
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if start and end:
        return get_311_data_between(start, end), get_parking_data_between(start, end)
    return get_yesterdays_311_data(), get_yesterdays_parking_data()


def clean_sources(
    raw_311: pd.DataFrame, raw_parking: pd.DataFrame
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    df311 = clean_311_data(raw_311) if not raw_311.empty else pd.DataFrame()
    dfpark = (
        clean_parking_data(raw_parking) if not raw_parking.empty else pd.DataFrame()
    )
    return df311, dfpark


def process_311(
    cleaned_311: pd.DataFrame, dim_data: dict[str, pd.DataFrame]
) -> pd.DataFrame:
    if cleaned_311.empty:
        return cleaned_311
    df = assign_keys(
        cleaned_311, dim_data["agency"], ["agency", "agency_name"], "Agency_Key"
    )
    df = assign_keys(
        df,
        dim_data["complaint"],
        ["complaint_type", "descriptor", "location_type"],
        "Complaint_Key",
    )
    df = assign_keys(df, dim_data["location"], LOCATION_DIM_COLUMNS, "Location_Key")
    if "__join_key__" in df.columns:
        df = df.drop(columns="__join_key__")
    cleaned_311 = cleaned_311[[c for c in FACT_311_COLUMNS if c in cleaned_311.columns]]
    load_df_to_bq(cleaned_311, "fact_311_complaints")

    return df


def process_parking(
    cleaned_parking: pd.DataFrame, dim_data: dict[str, pd.DataFrame]
) -> pd.DataFrame:
    if cleaned_parking.empty:
        return cleaned_parking
    df = cleaned_parking
    if "vehicle" in dim_data:
        df = assign_keys(
            df, dim_data["vehicle"], ["plate", "state", "license_type"], "Vehicle_Key"
        )
    if "violation" in dim_data:
        df = assign_keys(
            df,
            dim_data["violation"],
            ["violation_code", "violation_description"],
            "Violation_Key",
        )
    if "parking_location" in dim_data:
        df = assign_keys(
            df,
            dim_data["parking_location"],
            ["borough", "precinct"],
            "Parking_Location_Key",
        )
    df = df[[c for c in PARKING_FACT_COLUMNS if c in df.columns]]
    load_df_to_bq(cleaned_parking, "fact_parking_tickets")
    return df


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
    raw_311, raw_park = fetch_sources(start, end)
    cleaned_311, cleaned_parking = clean_sources(raw_311, raw_park)
    if cleaned_311.empty and cleaned_parking.empty:
        print("No new data to process for 311 or parking.")
        return
    load_date_dim()
    dim_data = load_dimensions(cleaned_311, cleaned_parking)
    cleaned_311 = process_311(cleaned_311, dim_data)
    cleaned_parking = process_parking(cleaned_parking, dim_data)
    integrated = build_integrated(cleaned_311, cleaned_parking)
    if integrated is not None:
        load_df_to_bq(integrated, "fact_integrated_requests")
    print("ETL complete!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run NYC Open Data ETL")
    parser.add_argument(
        "--start", type=str, help="Start timestamp (e.g. 2023-01-01T00:00:00.000)"
    )
    parser.add_argument(
        "--end", type=str, help="End timestamp (e.g. 2023-01-02T00:00:00.000)"
    )
    args = parser.parse_args()
    main(start=args.start, end=args.end)
