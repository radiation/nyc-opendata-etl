from etl.core.dim_loader import BaseDimLoader
import pandas as pd

class TimeDimLoader(BaseDimLoader):
    def __init__(self) -> None:
        super().__init__("time_dim")

    def generate_time_range(self) -> pd.DataFrame:
        """
        Generate one row per minute from 00:00 to 23:59.
        time_key will be HHMM00 (seconds always zero).
        """
        # Create a DatetimeIndex at 1-minute intervals
        times = pd.date_range("00:00", "23:59", freq="T").time
        df = pd.DataFrame({"time": times})
        # HHMMSS as integer, but since seconds=00, this is HHMM00
        df["time_key"] = df["time"].apply(lambda t: int(t.strftime("%H%M00")))
        df["hour"]     = df["time"].apply(lambda t: t.hour)
        df["minute"]   = df["time"].apply(lambda t: t.minute)
        # drop the helper column
        return df[["time_key", "hour", "minute"]]