FACT_311_COLUMNS = [
    "unique_key",
    "Created_Date",
    "Closed_Date",
    "Due_Date",
    "Created_Date_Key",
    "Closed_Date_Key",
    "Due_Date_Key",
    "Agency_Key",
    "Complaint_Key",
    "Location_Key",
    "status",
    "resolution_description",
]

LOCATION_DIM_COLUMNS = [
    "borough",
    "city",
    "incident_zip",
    "street_name",
    "incident_address",
    "cross_street_1",
    "cross_street_2",
    "intersection_street_1",
    "intersection_street_2",
    "latitude",
    "longitude",
]

PARKING_FACT_COLUMNS = [
    "summons_number",
    "Issue_Date",
    "Issue_Date_Key",
    "Agency_Key",
    "Violation_Key",
    "Vehicle_Key",
    "Parking_Location_Key",
    "Fine_Amount",
    "Penalty_Amount",
    "Interest_Amount",
    "Reduction_Amount",
    "Payment_Amount",
    "Amount_Due",
]
