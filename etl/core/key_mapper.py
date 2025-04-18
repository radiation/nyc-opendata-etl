import pandas as pd
from etl.core.utils import hash_key


def assign_keys(
    fact_df: pd.DataFrame,
    dim_df: pd.DataFrame,
    dim_fields: list[str],
    key_name: str
) -> pd.DataFrame:
    if dim_df.empty or not all(field in dim_df.columns for field in dim_fields):
        print(f"Skipping key assignment for {key_name} — missing fields or empty dim_df.")
        fact_df[key_name] = pd.NA
        return fact_df

    if not all(field in fact_df.columns for field in dim_fields):
        print(f"Skipping key assignment for {key_name} — missing fields in fact table.")
        fact_df[key_name] = pd.NA
        return fact_df

    dim_df = dim_df.copy()
    fact_df = fact_df.copy()

    dim_df[key_name] = dim_df.apply(hash_key, axis=1)

    # Defensive join key generation
    try:
        dim_df["__join_key__"] = dim_df[dim_fields].astype(str).agg("|".join, axis=1)
        fact_df["__join_key__"] = fact_df[dim_fields].astype(str).agg("|".join, axis=1)
    except Exception as e:
        print(f"Failed to generate join keys for {key_name}: {e}")
        fact_df[key_name] = pd.NA
        return fact_df

    joined = fact_df.merge(dim_df[[key_name, "__join_key__"]], on="__join_key__", how="left")

    # Clean up only if the column exists
    drop_cols = ["__join_key"] + [f for f in dim_fields if f in joined.columns]
    joined = joined.drop(columns=drop_cols, errors="ignore")

    return joined
