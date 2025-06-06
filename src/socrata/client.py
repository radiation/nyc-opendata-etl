from typing import Optional, Union

from sodapy import Socrata


class SocrataClient:
    def __init__(self, domain: str, app_token: Optional[str] = None) -> None:
        self.client = Socrata(domain, app_token)

    def fetch(
        self,
        dataset_id: str,
        start: Optional[str] = None,
        end: Optional[str] = None,
        limit: int = 50000,
        date_field: str = "issue_date",
    ) -> list[dict[str, Union[str, float, int, None]]]:
        where_clauses: list[str] = []

        if start:
            where_clauses.append(f"{date_field} >= '{start}'")
        if end:
            where_clauses.append(f"{date_field} < '{end}'")

        where = " AND ".join(where_clauses) if where_clauses else None

        return self.client.get(dataset_id, where=where, limit=limit)
