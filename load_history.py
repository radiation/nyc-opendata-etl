from datetime import datetime, timedelta
import subprocess

START = datetime(2010, 1, 1)
TODAY = datetime.today()
current = START

while current < TODAY:
    next_month = (current.replace(day=1) + timedelta(days=32)).replace(day=1)

    start_str = current.strftime("%Y-%m-%dT00:00:00.000")
    end_str = next_month.strftime("%Y-%m-%dT00:00:00.000")

    print(f"📅 Running ETL for {start_str} → {end_str}")
    subprocess.run(["python", "main.py", "--start", start_str, "--end", end_str])

    current = next_month

