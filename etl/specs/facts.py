from etl.constants import DIMENSION_COLUMNS
from etl.core.column_spec import ColumnSpec
from etl.fact_loader import FactLoader

DATASET_311 = "erm2-nwe9"
DATASETS_PARKING = {
    2014: "jt7v-77mi",
    2015: "c284-tqph",
    2016: "kiv2-tbus",
    2017: "2bnn-yakx",
    2018: "a5td-mswe",
    2019: "faiq-9dfq",
    2020: "p7t3-5i9s",
    2021: "kvfd-bves",
    2022: "7mxj-7a6y",
    2023: "869v-vr48",
    2024: "pvqr-7yc4",
}

LATEST_PARKING_YEAR = max(DATASETS_PARKING.keys())
EARLIEST_PARKING_YEAR = min(DATASETS_PARKING.keys())

# Fact‐table column lists
FACT_311_COLUMNS: list[ColumnSpec] = [
    ColumnSpec("unique_key", "numeric"),
    ColumnSpec("created_date", "date"),
    ColumnSpec("closed_date", "date"),
    ColumnSpec("due_date", "date"),
    ColumnSpec("agency_key", "numeric"),
    ColumnSpec("complaint_key", "numeric"),
    ColumnSpec("location_key", "numeric"),
    ColumnSpec("status", "string"),
    ColumnSpec("resolution_description", "string"),
]

FACT_PARKING_COLUMNS: list[ColumnSpec] = [
    ColumnSpec("summons_number", "numeric"),
    ColumnSpec("issue_date", "date"),
    ColumnSpec("issue_date_key", "numeric"),
    ColumnSpec("agency_key", "numeric"),
    ColumnSpec("violation_key", "numeric"),
    ColumnSpec("vehicle_key", "numeric"),
    ColumnSpec("parking_location_key", "numeric"),
    ColumnSpec("fine_amount", "numeric"),
    ColumnSpec("penalty_amount", "numeric"),
    ColumnSpec("interest_amount", "numeric"),
    ColumnSpec("reduction_amount", "numeric"),
    ColumnSpec("payment_amount", "numeric"),
    ColumnSpec("amount_due", "numeric"),
]

FACT_INTEGRATED_COLUMNS: list[ColumnSpec] = [
    ColumnSpec("request_id", "numeric"),
    ColumnSpec("date_key", "numeric"),
    ColumnSpec("agency_key", "numeric"),
    ColumnSpec("complaint_key", "numeric"),
    ColumnSpec("location_key", "numeric"),
    ColumnSpec("request_type", "string"),
    ColumnSpec("resolution_description", "string"),
    ColumnSpec("amount_due", "numeric"),
]

FACT_311_FK_MAP = [
    (DIMENSION_COLUMNS["agency"], "Agency_Key"),
    (DIMENSION_COLUMNS["complaint"], "Complaint_Key"),
    (DIMENSION_COLUMNS["location"], "Location_Key"),
]

FACT_PARKING_FK_MAP = [
    (DIMENSION_COLUMNS["vehicle"], "Vehicle_Key"),
    (DIMENSION_COLUMNS["violation"], "Violation_Key"),
    (DIMENSION_COLUMNS["parking_location"], "Parking_Location_Key"),
]

# BigQuery target table names
FACT_311_TABLE = "fact_311_complaints"
FACT_PARKING_TABLE = "fact_parking_tickets"
FACT_INT_TABLE = "fact_integrated_requests"

# FactLoader instances
fact_311_loader = FactLoader(
    table_name="311 complaints",
    specs=FACT_311_COLUMNS,
    fk_map=FACT_311_FK_MAP,
    bq_table="fact_311_complaints",
)

fact_parking_loader = FactLoader(
    table_name="parking tickets",
    specs=FACT_PARKING_COLUMNS,
    fk_map=FACT_PARKING_FK_MAP,
    bq_table="fact_parking_tickets",
)
