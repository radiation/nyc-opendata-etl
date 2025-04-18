import hashlib
import pandas as pd


def hash_key(row: pd.Series) -> int:
    """ Generate a stable, numeric hash key based on row contents. """
    input_str = "|".join(str(row[col]) for col in row.index)
    return int(hashlib.md5(input_str.encode()).hexdigest(), 16) % (10**9)
