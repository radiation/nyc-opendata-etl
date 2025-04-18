import os
from dotenv import load_dotenv
from pathlib import Path

load_dotenv(dotenv_path=Path(__file__).parent.parent / ".env")

NYC_API_TOKEN: str | None = os.getenv("NYC_API_TOKEN")
