from __future__ import annotations

from typing import Dict

import pandas as pd

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
    print(f"  extra_fields: {fact.extra_fields}")

    df = raw.copy()
    natural_keys = {
        key for dim in fact.foreign_keys.values() for key in dim.natural_keys
    }
    required_columns = list(natural_keys | {fact.primary_key} | set(fact.extra_fields))
    print(f"  Required columns for fact table: {required_columns}")

    df = df[[col for col in required_columns if col in df.columns]].copy()
    print(f"  Initial columns: {df.columns.tolist()}")
    for fk_col, dim in fact.foreign_keys.items():
        right = staging_dims[dim.table_name][dim.natural_keys + [dim.primary_key]]
        df = df.merge(
            right,
            on=dim.natural_keys,
            how="left",
        )
        df = df.rename(columns={dim.primary_key: fk_col})
        print(f"  Joined {fk_col} from {dim.table_name} on {dim.natural_keys}")

    print(f"  After FK joins: {df.columns.tolist()}")

    # Project only the fact PK and the FK columns
    available = [col for col in fact.all_columns() if col in df.columns]
    result = df[available].copy()

    print(f"  Selected columns: {result.columns.tolist()}")

    # (Optional) dedupe any accidental duplicate column names
    result = result.loc[:, ~result.columns.duplicated()]
    print(
        f"  Joined {len(result)} rows with {len(result.columns)} columns after FK joins"
    )

    print(f"  Final columns: {result.columns.tolist()}")
    return result
