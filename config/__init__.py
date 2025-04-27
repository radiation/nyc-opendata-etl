from pathlib import Path
from typing import TypedDict

import tomllib


class BQConfig(TypedDict):
    project_id: str
    dataset: str

class TableConfig(TypedDict):
    agency_dim: str
    complaint_dim: str
    date_dim: str
    fact_311_complaints: str
    fact_parking_tickets: str
    fact_integrated_requests: str
    location_dim: str
    parking_location_dim: str
    vehicle_dim: str
    violation_dim: str

class Config(TypedDict):
    bigquery: BQConfig
    tables: dict[str, str]

def load_config() -> Config:
    with open(Path(__file__).parent / "settings.toml", "rb") as f:
        return tomllib.load(f)  # type: ignore
