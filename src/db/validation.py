from google.cloud import bigquery
from typing import List, Tuple

def validate_foreign_keys(
    client: bigquery.Client,
    fact_table: str,
    fk_to_dim: List[Tuple[str, str, str]],
) -> None:
    """
    For each (fk_col, dim_table, dim_pk), ensure all facts join.
    Raises if any orphan keys found.
    """
    for fk_col, dim_table, dim_pk in fk_to_dim:
        query = f"""
        SELECT COUNT(*) AS orphan_count
        FROM `{fact_table}` f
        LEFT JOIN `{dim_table}` d
          ON f.{fk_col} = d.{dim_pk}
        WHERE d.{dim_pk} IS NULL
        """
        orphan_count = client.query(query).result().to_dataframe()["orphan_count"][0]
        if orphan_count > 0:
            raise ValueError(f"Found {orphan_count} orphan keys in {fk_col} → {dim_table}.{dim_pk}")

def validate_unique(
    client: bigquery.Client,
    table: str,
    key_cols: List[str],
) -> None:
    """
    Ensure no duplicates on the given key columns.
    """
    cols = ", ".join(key_cols)
    query = f"""
    SELECT 
      COUNT(*) AS total,
      COUNT(DISTINCT {cols}) AS distinct_count
    FROM `{table}`
    """
    row = client.query(query).result().to_dataframe().iloc[0]
    if row["total"] != row["distinct_count"]:
        raise ValueError(f"Duplicates detected in {table} on keys: {cols}")
