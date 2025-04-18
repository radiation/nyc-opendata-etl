import pandas as pd
from etl.utils import hash_key


def assign_keys(
    fact_df: pd.DataFrame,
    dim_df: pd.DataFrame,
    dim_fields: list[str],
    key_name: str
) -> pd.DataFrame:
    """
    Assigns a hashed surrogate key from dim_df 
    to fact_df using business fields.
    """
    dim_df = dim_df.copy()
    dim_df[key_name] = dim_df.apply(hash_key, axis=1)

    # Create join key
    dim_df["__join_key__"] = dim_df[dim_fields].astype(str).agg("|".join, axis=1)
    fact_df["__join_key__"] = fact_df[dim_fields].astype(str).agg("|".join, axis=1)

    # Merge and assign key
    joined = fact_df.merge(dim_df[[key_name, "__join_key__"]], on="__join_key__", how="left")
    joined = joined.drop(columns=["__join_key__"] + dim_fields)

    return joined
