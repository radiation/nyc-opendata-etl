from datetime import datetime, timedelta
import pandas as pd

from etl.fact_loaders.load_311 import get_yesterdays_311_data, clean_311_data, load_to_bigquery as load_311_fact
from etl.fact_loaders.load_parking import get_yesterdays_parking_data, clean_parking_data, load_to_bigquery as load_parking_fact
from etl.core.key_mapper import assign_keys
from etl.core.dim_loader import DimLoaderProtocol

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


def load_dimensions(df_311: pd.DataFrame, df_parking: pd.DataFrame) -> dict[str, DimLoaderProtocol]:
    loaders: dict[str, DimLoaderProtocol] = {
        "agency": AgencyDimLoader(),
        "complaint": ComplaintDimLoader(),
        "location": LocationDimLoader(),
        "vehicle": VehicleDimLoader(),
        "violation": ViolationDimLoader(),
        "parking_location": ParkingLocationDimLoader(),
    }

    combined_df = pd.concat([df_311, df_parking], ignore_index=True)

    for name, loader in loaders.items():
        print(f"▶️ Running {loader.__class__.__name__}...")
        extracted = loader.extract(combined_df)
        transformed = loader.transform(extracted)
        loader.load(transformed)

    return loaders


def main() -> None:
    print("Running ETL for NYC Open Data...")

    # Step 1: Ensure Date_Dim is up to date
    load_date_dim()

    # Step 2: Pull source data
    raw_311 = get_yesterdays_311_data()
    cleaned_311 = clean_311_data(raw_311)

    raw_parking = get_yesterdays_parking_data()
    cleaned_parking = clean_parking_data(raw_parking)

    if cleaned_311.empty and cleaned_parking.empty:
        print("No new data to process for 311 or parking.")
        return

    # Step 3: Load dimensions and keep them in memory for FK resolution
    loaders = load_dimensions(cleaned_311, cleaned_parking)

    # Step 4: Assign foreign keys to fact_311
    if not cleaned_311.empty:
        cleaned_311 = assign_keys(cleaned_311, loaders["agency"].transform(loaders["agency"].extract(cleaned_311)), ["agency", "agency_name"], "Agency_Key")
        cleaned_311 = assign_keys(cleaned_311, loaders["complaint"].transform(loaders["complaint"].extract(cleaned_311)), ["complaint_type", "descriptor", "location_type"], "Complaint_Key")
        cleaned_311 = assign_keys(cleaned_311, loaders["location"].transform(loaders["location"].extract(cleaned_311)), [
            "borough", "city", "incident_zip", "street_name", "incident_address",
            "cross_street_1", "cross_street_2", "intersection_street_1", "intersection_street_2",
            "latitude", "longitude"
        ], "Location_Key")

        print("Columns BEFORE upload:", cleaned_311.columns.tolist())

        if "__join_key__" in cleaned_311.columns:
            print("⚠️ __join_key__ still present before upload! Dropping it here.")
            cleaned_311 = cleaned_311.drop(columns="__join_key__")
            
        load_311_fact(cleaned_311)

    # Step 5: Assign foreign keys to fact_parking
    if not cleaned_parking.empty:
        cleaned_parking = assign_keys(cleaned_parking, loaders["vehicle"].transform(loaders["vehicle"].extract(cleaned_parking)), ["plate", "state", "license_type"], "Vehicle_Key")
        cleaned_parking = assign_keys(cleaned_parking, loaders["violation"].transform(loaders["violation"].extract(cleaned_parking)), ["violation_code", "violation_description"], "Violation_Key")
        cleaned_parking = assign_keys(cleaned_parking, loaders["parking_location"].transform(loaders["parking_location"].extract(cleaned_parking)), ["borough", "precinct"], "Parking_Location_Key")

        load_parking_fact(cleaned_parking)

    print("ETL complete!")


if __name__ == "__main__":
    main()
