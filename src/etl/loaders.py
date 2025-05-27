from __future__ import annotations
import pandas as pd
from typing import Dict

from .config import DimensionConfig, FactConfig
from .normalization import normalize_strings


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
    print(f"Building fact table: {fact.table_name}")
    print(f"  primary_key: {fact.primary_key}")
    print(f"  foreign_keys: {list(fact.foreign_keys.keys())}")
    df = raw.copy()
    for fk_col, dim in fact.foreign_keys.items():
        print(f"  Joining dimension {dim.table_name} with {fk_col} on natural keys: {dim.natural_keys}")
        df = (
            df
            .merge(
                staging_dims[dim.table_name][dim.natural_keys + [dim.primary_key]],
                on=dim.natural_keys,
                how="left",
            )
            .rename(columns={dim.primary_key: fk_col})
        )
    # 2) finally project only the fact PK and the FK columns
    result = df[[fact.primary_key, *fact.foreign_keys.keys()]]

    # 3) (optional) dedupe any accidental duplicate column names
    result = result.loc[:, ~result.columns.duplicated()]
    print(f"  Joined {len(result)} rows with {len(result.columns)} columns after FK joins")

    return result
