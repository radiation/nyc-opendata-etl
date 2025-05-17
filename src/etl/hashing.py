from __future__ import annotations
import hashlib
import pandas as pd
from typing import Callable, Sequence, Any


def default_hash(values: Sequence[Any]) -> int:
    """
    Generate a stable, 9-digit numeric hash based on the given values.
    """
    # 1) concatenate with a delimiter
    concatenated: str = "|".join(str(v) for v in values)
    # 2) MD5 it
    digest: str = hashlib.md5(concatenated.encode("utf-8")).hexdigest()
    # 3) return as int, constrained to 9 digits
    return int(digest, 16) % (10**9)


def add_hash_column(
    df: pd.DataFrame,
    *,
    new_column: str,
    source_columns: Sequence[str],
    hash_fn: Callable[[Sequence[Any]], int] = default_hash,
) -> pd.DataFrame:
    """
    Vectorized: create `new_column` by hashing each row's `source_columns` via `hash_fn`.
    """
    df_out: pd.DataFrame = df.copy()
    df_out[new_column] = df_out[source_columns].apply(
        lambda row: hash_fn(row.tolist()), axis=1
    )
    return df_out
