from __future__ import annotations

import re
import unicodedata
from typing import Any, Sequence

import pandas as pd


def normalize_strings(
    df: pd.DataFrame,
    *,
    columns: Sequence[str],
) -> pd.DataFrame:
    """
    Standardize string columns in‐place:
      1) Fill nulls → ''
      2) Unicode‐normalize (NFKD) & strip accents
      3) Trim whitespace
      4) Lowercase

    Returns a new DataFrame.
    """
    df_out: pd.DataFrame = df.copy()

    # Drop duplicate columns (must be done before accessing individual Series)
    df_out = df_out.loc[:, ~df_out.columns.duplicated()].copy()

    for col in columns:
        if col not in df_out.columns:
            continue

        series = df_out[col]
        if isinstance(series, pd.DataFrame):
            raise TypeError(
                f"Expected Series for column '{col}', got DataFrame — duplicate column name?"
            )

        normalized = (
            series.fillna("")
            .astype(str)
            .map(lambda s: unicodedata.normalize("NFKD", s))
            .str.strip()
            .str.lower()
        )

        df_out[col] = normalized

    return df_out


def parse_violation_time(s: Any) -> str:
    """
    Turns '0823A' → '08:23:00.000', '1215P' → '12:15:00.000'.
    Returns '' on any invalid input.
    """
    if not isinstance(s, str):
        return ""
    # remove everything except digits and A/P
    clean = re.sub(r"[^0-9APap]", "", s)
    # must end in A or P
    m = re.match(r"^([0-9]+)([APap])$", clean)
    if not m:
        return ""
    hhmm, ampm = m.groups()
    # pad to 4 digits (e.g. '823' -> '0823')
    hhmm = hhmm.zfill(4)
    hour = int(hhmm[:2])
    minute = int(hhmm[2:4])

    ampm = ampm.upper()
    if ampm == "P" and hour != 12:
        hour += 12
    if ampm == "A" and hour == 12:
        hour = 0
    return f"{hour:02d}:{minute:02d}:00.000"
