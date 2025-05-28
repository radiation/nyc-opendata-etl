from .config import DimensionConfig, FactConfig

# Dimension definitions
DIM_AGENCY = DimensionConfig(
    table_name="dim_agency",
    primary_key="agency_key",
    natural_keys=["agency", "agency_name"],
)

DIM_DATE = DimensionConfig(
    table_name="dim_date",
    primary_key="date_key",
    natural_keys=["full_date"],
)

DIM_COMPLAINT = DimensionConfig(
    table_name="dim_complaint",
    primary_key="complaint_key",
    natural_keys=["complaint_type"],
)

DIM_LOCATION = DimensionConfig(
    table_name="dim_location",
    primary_key="location_key",
    natural_keys=["incident_zip", "incident_address"],
)

DIM_PARKING_LOCATION = DimensionConfig(
    table_name="dim_parking_location",
    primary_key="parking_location_key",
    natural_keys=["summons_number", "violation_location"],
)

DIM_TIME = DimensionConfig(
    table_name="dim_time",
    primary_key="time_key",
    natural_keys=["incident_time"],
)

DIM_VEHICLE = DimensionConfig(
    table_name="dim_vehicle",
    primary_key="vehicle_key",
    natural_keys=["license_plate", "state"],
)

DIM_VIOLATION = DimensionConfig(
    table_name="dim_violation",
    primary_key="violation_key",
    natural_keys=["violation_code", "violation_description"],
)

# Fact definitions
FACT_311 = FactConfig(
    table_name="fact_311_complaints",
    primary_key="unique_key",
    foreign_keys={
        "agency_key": DIM_AGENCY,
        "complaint_key": DIM_COMPLAINT,
        "location_key": DIM_LOCATION,
        "created_date_key": DIM_DATE,
        "created_time_key": DIM_TIME,
        "closed_date_key": DIM_DATE,
        "closed_time_key": DIM_TIME,
    },
)

FACT_PARKING = FactConfig(
    table_name="fact_parking_tickets",
    primary_key="summons_number",
    foreign_keys={
        "date_key": DIM_DATE,
        "time_key": DIM_TIME,
        "location_key": DIM_PARKING_LOCATION,
        "vehicle_key": DIM_VEHICLE,
        "violation_code": DIM_VIOLATION,
    },
)

# Registry definitions
ALL_DIMS = [
    DIM_AGENCY,
    DIM_DATE,
    DIM_COMPLAINT,
    DIM_LOCATION,
    DIM_PARKING_LOCATION,
    DIM_TIME,
    DIM_VEHICLE,
    DIM_VIOLATION,
]
ALL_FACTS = [FACT_311, FACT_PARKING]
