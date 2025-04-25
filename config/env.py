import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(dotenv_path=Path(__file__).parent.parent / ".env")

NYC_API_TOKEN: str | None = os.getenv("NYC_API_TOKEN")
