# src/etl/normalization.py
from __future__ import annotations
from typing import Sequence
import unicodedata

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
    for col in columns:
        if col not in df_out.columns:
            continue

        # chain map/str methods for speed & clarity
        normalized = (
            df_out[col]
            .fillna("")
            .astype(str)
            .map(lambda s: unicodedata.normalize("NFKD", s))
            .str.strip()
            .str.lower()
        )
        df_out.loc[:, col] = normalized

    return df_out
