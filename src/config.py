from __future__ import annotations
import os
from dotenv import load_dotenv

load_dotenv()

SOCRATA_API_TOKEN: str = os.environ["NYC_API_TOKEN"]
SOCRATA_DOMAIN: str = os.environ.get("SOCRATA_DOMAIN", "data.cityofnewyork.us")

GCP_PROJECT: str = os.environ["GCP_PROJECT"]
BQ_STAGING_DATASET: str = os.environ["BQ_STAGING_DATASET"]
