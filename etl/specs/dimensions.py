from etl.core.column_spec import ColumnSpec

# column specs for each dimension
AGENCY_SPECS: list[ColumnSpec] = [
    ColumnSpec("agency", "string"),
    ColumnSpec("agency_name", "string"),
]

COMPLAINT_SPECS: list[ColumnSpec] = [
    ColumnSpec("complaint_type", "string"),
    ColumnSpec("descriptor", "string"),
    ColumnSpec("location_type", "string"),
]

LOCATION_SPECS: list[ColumnSpec] = [
    ColumnSpec("borough", "string"),
    ColumnSpec("precinct", "numeric"),
]

VEHICLE_SPECS: list[ColumnSpec] = [
    ColumnSpec("plate", "string"),
    ColumnSpec("state", "string"),
    ColumnSpec("license_type", "string"),
]

VIOLATION_SPECS: list[ColumnSpec] = [
    ColumnSpec("violation_code", "string"),
    ColumnSpec("violation_description", "string"),
]

PARKING_LOCATION_SPECS: list[ColumnSpec] = [
    ColumnSpec("borough", "string"),
    ColumnSpec("precinct", "numeric"),
]

# natural-key columns (in the raw DataFrame) for assign_keys
AGENCY_NATURAL_KEYS: list[str] = ["agency", "agency_name"]
COMPLAINT_NATURAL_KEYS: list[str] = ["complaint_type", "descriptor", "location_type"]
LOCATION_NATURAL_KEYS: list[str] = ["borough", "precinct"]
VEHICLE_NATURAL_KEYS: list[str] = ["plate", "state", "license_type"]
VIOLATION_NATURAL_KEYS: list[str] = ["violation_code", "violation_description"]
PARKING_LOCATION_NATURAL_KEYS = LOCATION_NATURAL_KEYS  # same as location

# output BigQuery table IDs
AGENCY_TABLE = "dim_agency"
COMPLAINT_TABLE = "dim_complaint"
LOCATION_TABLE = "dim_location"
VEHICLE_TABLE = "dim_vehicle"
VIOLATION_TABLE = "dim_violation"
PARKING_LOCATION_TABLE = "dim_parking_location"

# name, specs, natural_keys, key_name, bq_table
DIMENSION_CONFIGS: list[tuple[str, list[ColumnSpec], list[str], str, str]] = [
    ("agency", AGENCY_SPECS, AGENCY_NATURAL_KEYS, "agency_key", AGENCY_TABLE),
    (
        "complaint",
        COMPLAINT_SPECS,
        COMPLAINT_NATURAL_KEYS,
        "complaint_key",
        COMPLAINT_TABLE,
    ),
    ("location", LOCATION_SPECS, LOCATION_NATURAL_KEYS, "location_key", LOCATION_TABLE),
    ("vehicle", VEHICLE_SPECS, VEHICLE_NATURAL_KEYS, "vehicle_key", VEHICLE_TABLE),
    (
        "violation",
        VIOLATION_SPECS,
        VIOLATION_NATURAL_KEYS,
        "violation_key",
        VIOLATION_TABLE,
    ),
    (
        "parking_location",
        PARKING_LOCATION_SPECS,
        PARKING_LOCATION_NATURAL_KEYS,
        "parking_location_key",
        PARKING_LOCATION_TABLE,
    ),
]
