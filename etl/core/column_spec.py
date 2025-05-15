from dataclasses import dataclass
from typing import Literal

DataType = Literal["date", "numeric", "string"]


@dataclass(frozen=True)
class ColumnSpec:
    name: str
    dtype: DataType
