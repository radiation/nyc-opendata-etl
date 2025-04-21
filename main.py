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

def load_date_dim() -> None:
    date_loader = DateDimLoader()
    today = datetime.today()
    start_date = datetime(2010, 1, 1)
    end_date = today + timedelta(days=365)

    df = date_loader.generate_date_range(start_date, end_date)
    date_loader.load(df)

def load_dimensions(df_311: pd.DataFrame, df_parking: pd.DataFrame) -> dict[str, pd.DataFrame]:
    loaders = {
        "agency": (AgencyDimLoader(), pd.concat([df_311, df_parking], ignore_index=True)),
        "complaint": (ComplaintDimLoader(), df_311),
        "location": (LocationDimLoader(), df_311),
        # Only include the transformed df if it's non-empty
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

    if start and end:
        raw_311 = get_311_data_between(start, end)
        raw_parking = get_parking_data_between(start, end)
    else:
        raw_311 = get_yesterdays_311_data()
        raw_parking = get_yesterdays_parking_data()

    if raw_311.empty:
        print("No new 311 data to process.")
        cleaned_311 = pd.DataFrame()
    else:
        cleaned_311 = clean_311_data(raw_311)

    if raw_parking.empty:
        print("No new parking data to process.")
        cleaned_parking = pd.DataFrame()
    else:
        cleaned_parking = clean_parking_data(raw_parking)

    if cleaned_311.empty and cleaned_parking.empty:
        print("No new data to process for 311 or parking.")
        return

    # Step 3: Load dimensions and keep them in memory for FK resolution
    dim_data = load_dimensions(cleaned_311, cleaned_parking)

    # Step 4: Assign foreign keys to fact_311
    if not cleaned_311.empty:
        cleaned_311 = assign_keys(cleaned_311, dim_data["agency"], ["agency", "agency_name"], "Agency_Key")
        cleaned_311 = assign_keys(cleaned_311, dim_data["complaint"], ["complaint_type", "descriptor", "location_type"], "Complaint_Key")
        cleaned_311 = assign_keys(cleaned_311, dim_data["location"], [
            "borough", "city", "incident_zip", "street_name", "incident_address",
            "cross_street_1", "cross_street_2", "intersection_street_1", "intersection_street_2",
            "latitude", "longitude"
        ], "Location_Key")

        if "__join_key__" in cleaned_311.columns:
            cleaned_311 = cleaned_311.drop(columns="__join_key__")

        load_311_fact(cleaned_311)

    # Step 5: Assign foreign keys to fact_parking
    if not cleaned_parking.empty:
        if "vehicle" in dim_data:
            cleaned_parking = assign_keys(cleaned_parking, dim_data["vehicle"], ["plate", "state", "license_type"], "Vehicle_Key")
        if "violation" in dim_data:
            cleaned_parking = assign_keys(cleaned_parking, dim_data["violation"], ["violation_code", "violation_description"], "Violation_Key")
        if "parking_location" in dim_data:
            cleaned_parking = assign_keys(cleaned_parking, dim_data["parking_location"], ["borough", "precinct"], "Parking_Location_Key")

        fact_fields = [
            "summons_number",
            "Issue_Date", "Issue_Date_Key",
            "Agency_Key", "Violation_Key", "Vehicle_Key", "Parking_Location_Key",
            "Fine_Amount", "Penalty_Amount", "Interest_Amount",
            "Reduction_Amount", "Payment_Amount", "Amount_Due"
        ]

        cleaned_parking = cleaned_parking[[col for col in fact_fields if col in cleaned_parking.columns]]

        load_parking_fact(cleaned_parking)

    print("ETL complete!")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run NYC Open Data ETL")
    parser.add_argument("--start", type=str, help="Start timestamp (e.g. 2023-01-01T00:00:00.000)")
    parser.add_argument("--end", type=str, help="End timestamp (e.g. 2023-01-02T00:00:00.000)")
    args = parser.parse_args()

    main(start=args.start, end=args.end)
