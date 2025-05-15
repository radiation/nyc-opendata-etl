import hashlib

import pandas as pd


def hash_key(row: pd.Series, columns: list[str]) -> int:
    """Generate a stable, numeric hash key based on row contents."""
    input_str = "|".join(str(row[col]) for col in columns)
    return int(hashlib.md5(input_str.encode()).hexdigest(), 16) % (10**9)


def normalize_strings(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """Standardizes string columns for hashing or join use."""
    df = df.copy()
    df.columns = df.columns.str.strip().str.replace(" ", "_").str.lower()
    for col in columns:
        if col in df.columns:
            df[col] = df[col].fillna("").astype(str).str.strip().str.upper()
    return df
