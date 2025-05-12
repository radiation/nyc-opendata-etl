from datetime import datetime, timedelta
import argparse
import pandas as pd

from etl.fact_loaders.load_311 import (
    get_311_data_between, 
    get_yesterdays_311_data, 
    clean_311_data, 
    load_to_bigquery as load_311_fact
)
from etl.fact_loaders.load_parking import (
    get_parking_data_between,
    get_yesterdays_parking_data, 
    clean_parking_data, 
    load_to_bigquery as load_parking_fact
)
from etl.core.key_mapper import assign_keys

from etl.dim_loaders.agency_loader import AgencyDimLoader
from etl.dim_loaders.complaint_loader import ComplaintDimLoader
from etl.dim_loaders.location_loader import LocationDimLoader
from etl.dim_loaders.vehicle_loader import VehicleDimLoader
from etl.dim_loaders.violation_loader import ViolationDimLoader
from etl.dim_loaders.parking_location_loader import ParkingLocationDimLoader
from etl.dim_loaders.date_loader import DateDimLoader
from etl.dim_loaders.time_loader import TimeDimLoader


def load_date_and_time_dims() -> None:
    date_loader = DateDimLoader()
    today = datetime.today()
    start_date = datetime(2010, 1, 1)
    end_date = today + timedelta(days=365)

    df = date_loader.generate_date_range(start_date, end_date)
    date_loader.load(df)

    time_loader = TimeDimLoader()
    df_time = time_loader.generate_time_range()
    time_loader.load(df_time)


def load_dimensions(df_311: pd.DataFrame, df_parking: pd.DataFrame) -> dict[str, pd.DataFrame]:
    loaders = {
        "agency": (AgencyDimLoader(), pd.concat([df_311, df_parking], ignore_index=True)),
        "complaint": (ComplaintDimLoader(), df_311),
        "location": (LocationDimLoader(), df_311),
        "vehicle": (VehicleDimLoader(), df_parking if not df_parking.empty else pd.DataFrame()),
        "violation": (ViolationDimLoader(), df_parking if not df_parking.empty else pd.DataFrame()),
        "parking_location": (ParkingLocationDimLoader(), df_parking if not df_parking.empty else pd.DataFrame()),
    }

    transformed_dfs = {}

    for name, (loader, source_df) in loaders.items():
        print(f"\nRunning {loader.__class__.__name__}...")
        extracted = loader.extract(source_df)
        if extracted.empty:
            print(f"No data to load into {loader.table_id}")
        else:
            transformed = loader.transform(extracted)
            loader.load(transformed)
            transformed_dfs[name] = transformed

    return transformed_dfs


def main(start: str | None = None, end: str | None = None) -> None:
    print("Running ETL for NYC Open Data...")

    load_date_and_time_dims()

    if start and end:
        raw_311 = get_311_data_between(start, end)
        raw_parking = get_parking_data_between(start, end)
    else:
        raw_311 = get_yesterdays_311_data()
        raw_parking = get_yesterdays_parking_data()

    # Load dimensions and keep them in memory for FK resolution
    dim_data = load_dimensions(raw_311, raw_parking)

    cleaned_311 = clean_311_data(raw_311) if not raw_311.empty else pd.DataFrame()
    cleaned_parking = clean_parking_data(raw_parking) if not raw_parking.empty else pd.DataFrame()

    # Assign foreign keys to fact_311
    if not cleaned_311.empty:
        cleaned_311["date_key"] = cleaned_311["created_timestamp"].dt.strftime("%Y%m%d").astype(int)
        cleaned_311["time_key"] = cleaned_311["created_timestamp"].dt.strftime("%H%M%S").astype(int)
        cleaned_311 = assign_keys(
            cleaned_311,
            dim_data["agency"],
            ["agency", "agency_name"],
            "agency_key"
        )
        cleaned_311 = assign_keys(
            cleaned_311,
            dim_data["complaint"],
            ["complaint_type", "descriptor", "location_type"],
            "complaint_key"
        )
        cleaned_311 = assign_keys(
            cleaned_311,
            dim_data["location"],
            [
                "borough", "city", "incident_zip", "street_name", "incident_address",
                "cross_street_1", "cross_street_2", "intersection_street_1", "intersection_street_2",
                "latitude", "longitude"
            ],
            "location_key"
        )
        if "__join_key__" in cleaned_311.columns:
            cleaned_311 = cleaned_311.drop(columns="__join_key__")
        load_311_fact(cleaned_311)

    # Assign foreign keys to fact_parking
    if not cleaned_parking.empty:
        print(">>> cleaned_parking.columns:", cleaned_parking.columns.tolist())
        # 1) compute keys
        cleaned_parking["date_key"] = (
            cleaned_parking["issue_date"]
            .dt.strftime("%Y%m%d")
            .astype("Int64")
        )
        cleaned_parking["time_key"] = (
            cleaned_parking["violation_time"]
            .apply(lambda t: int(t.strftime("%H%M00")) if pd.notnull(t) else pd.NA)
            .astype("Int64")
        )

        cleaned_parking.rename(columns={
            "plate_id": "plate",
            "registration_state": "state",
            "plate_type": "license_type"
        }, inplace=True)
        cleaned_parking = assign_keys(
            cleaned_parking,
            dim_data["vehicle"],
            ["plate", "state", "license_type"],
            "vehicle_key"
        )

        load_parking_fact(cleaned_parking)

    print("ETL complete!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run NYC Open Data ETL")
    parser.add_argument("--start", type=str, help="Start timestamp (e.g. 2023-01-01T00:00:00.000)")
    parser.add_argument("--end", type=str, help="End timestamp (e.g. 2023-01-02T00:00:00.000)")
    args = parser.parse_args()

    main(start=args.start, end=args.end)
