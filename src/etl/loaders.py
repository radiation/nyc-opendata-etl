from __future__ import annotations
import pandas as pd
from typing import Dict

from .config import DimensionConfig, FactConfig
from .normalization import normalize_strings
from .hashing import default_hash


def build_dimension_df(
    raw: pd.DataFrame,
    dim: DimensionConfig,
) -> pd.DataFrame:
    """
    1. Normalize the natural-key columns.
    2. Drop duplicates.
    3. Generate the surrogate key.
    """
    # 1) clean
    clean = normalize_strings(raw, columns=dim.natural_keys)

    # 2) dedupe
    keys_only = clean[dim.natural_keys].drop_duplicates()

    # 3) hash
    keys_only[dim.primary_key] = keys_only.apply(
        lambda row: dim.hash_fn(tuple(row[col] for col in dim.natural_keys)), axis=1
    )

    return keys_only.reset_index(drop=True)


def build_fact_df(
    raw: pd.DataFrame,
    fact: FactConfig,
    staging_dims: Dict[str, pd.DataFrame],
) -> pd.DataFrame:
    """
    Join in all the FK columns from the staging_dims dict, then
    select only fact.primary_key + the FK columns.
    """
    df = raw.copy()
    for fk_col, dim in fact.foreign_keys.items():
        df = (
            df
            .merge(
                staging_dims[dim.table_name][dim.natural_keys + [dim.primary_key]],
                on=dim.natural_keys,
                how="left",
            )
            .rename(columns={dim.primary_key: fk_col})
        )
    return df[[fact.primary_key, *fact.foreign_keys.keys()]]
