from dataclasses import dataclass
from typing import Literal

DataType = Literal["date", "numeric", "string"]


@dataclass(frozen=True)
class ColumnSpec:
    name: str
    dtype: DataType


# Dimension‐loader field definitions
DIMENSION_COLUMNS: dict[str, list[str]] = {
    "agency": ["agency", "agency_name"],
    "complaint": ["complaint_type", "descriptor", "location_type"],
    "location": [
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
    ],
    "vehicle": ["plate", "state", "license_type"],
    "violation": ["violation_code", "violation_description"],
    "parking_location": ["borough", "precinct"],
}
