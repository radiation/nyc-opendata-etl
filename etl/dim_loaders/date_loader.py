from datetime import datetime
import pandas as pd
from etl.dim_loader import BaseDimLoader


class DateDimLoader(BaseDimLoader):
    def __init__(self) -> None:
        super().__init__("date_dim")

    def generate_date_range(self, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        dates = pd.date_range(start=start_date, end=end_date)
        df = pd.DataFrame({"Full_Date": dates})
        df["Date_Key"] = df["Full_Date"].dt.strftime("%Y%m%d").astype(int)
        df["Day"] = df["Full_Date"].dt.day
        df["Month"] = df["Full_Date"].dt.month
        df["Year"] = df["Full_Date"].dt.year
        df["Weekday"] = df["Full_Date"].dt.day_name()
        return df[
            ["Date_Key", "Full_Date", "Day", "Month", "Year", "Weekday"]
        ]
