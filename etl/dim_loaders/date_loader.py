from datetime import datetime
import pandas as pd
from etl.core.dim_loader import BaseDimLoader


class DateDimLoader(BaseDimLoader):
    def __init__(self) -> None:
        super().__init__("date_dim")

    def generate_date_range(self, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        dates = pd.date_range(start=start_date, end=end_date)
        df = pd.DataFrame({"full_date": dates})
        df["date_key"] = df["full_date"].dt.strftime("%Y%m%d").astype(int)
        df["day"]      = df["full_date"].dt.day
        df["month"]    = df["full_date"].dt.month
        df["year"]     = df["full_date"].dt.year
        df["weekday"]  = df["full_date"].dt.day_name()
        return df[
            ["date_key", "full_date", "day", "month", "year", "weekday"]
        ]
