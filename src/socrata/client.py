import os
from typing import Any, Mapping, Sequence, cast, Optional

import pandas as pd
from sodapy import Socrata

# Ensure API_TOKEN is a real str (not Optional[str])
_API_TOKEN: Optional[str] = os.getenv("NYC_API_TOKEN")
if _API_TOKEN is None:
    raise RuntimeError("NYC_API_TOKEN environment variable must be set")
API_TOKEN: str = _API_TOKEN

DOMAIN: str = os.getenv("SOCRATA_DOMAIN", "data.cityofnewyork.us")


def fetch_dataset(
    dataset_id: str,
    *,
    where: Optional[str] = None,
    limit: int = 1_000_000,
    **extra_kwargs: Any,
) -> pd.DataFrame:
    """
    Fetch from Socrata and return a DataFrame.

    :param dataset_id: e.g. "erm2-nwe9"
    :param where: optional SoQL WHERE clause
    :param limit: max rows
    :param extra_kwargs: passed through to Socrata.get()
    :returns: a pd.DataFrame
    """
    client: Socrata = Socrata(DOMAIN, API_TOKEN)

    # client.get isn’t typed, so mypy/Pylance sees Any — cast it explicitly
    raw: Any = client.get(dataset_id, where=where, limit=limit, **extra_kwargs)
    records: Sequence[Mapping[str, Any]] = cast(
        Sequence[Mapping[str, Any]], raw
    )

    return pd.DataFrame.from_records(records)