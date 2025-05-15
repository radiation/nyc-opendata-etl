from datetime import datetime

import pandas as pd

from etl.core.bq_loader import load_df_to_bq


class DateDimLoader:
    """
    Generates a calendar dimension (one row per date) and loads it into BigQuery.
    """

    def __init__(self, table_id: str = "dim_date") -> None:
        self.table_id = table_id

    def generate_date_range(
        self, start_date: datetime, end_date: datetime
    ) -> pd.DataFrame:
        # Build the raw date frame
        dates = pd.date_range(start=start_date, end=end_date, freq="D")
        df = pd.DataFrame({"full_date": dates})

        # Derive attributes
        df["date_key"] = df["full_date"].dt.strftime("%Y%m%d").astype(int)
        df["day"] = df["full_date"].dt.day.astype(int)
        df["month"] = df["full_date"].dt.month.astype(int)
        df["year"] = df["full_date"].dt.year.astype(int)
        df["weekday"] = df["full_date"].dt.day_name()

        # Reorder to your desired schema
        return df[["date_key", "full_date", "day", "month", "year", "weekday"]]

    def run(self, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """
        Generate the date dimension and push it to BigQuery.
        Returns the DataFrame in case downstream code wants to reference it.
        """
        df = self.generate_date_range(start_date, end_date)
        load_df_to_bq(df, self.table_id)
        return df
