from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Tuple

HashFn = Callable[[Tuple[str, ...]], int]


def default_hash(natural_vals: Tuple[str, ...]) -> int:
    raw = "|".join(natural_vals).encode("utf-8")
    return int(hashlib.md5(raw).hexdigest()[:8], 16)


@dataclass(frozen=True)
class DimensionConfig:
    table_name: str
    primary_key: str
    natural_keys: List[str]
    hash_fn: HashFn = default_hash

    def make_key(self, row: Dict[str, Any]) -> int:
        vals = tuple(str(row[k]) for k in self.natural_keys)
        return self.hash_fn(vals)


@dataclass(frozen=True)
class FactConfig:
    table_name: str
    primary_key: str
    foreign_keys: Dict[str, DimensionConfig]
    extra_fields: List[str]

    def all_columns(self) -> list[str]:
        return [self.primary_key] + list(self.foreign_keys.keys()) + self.extra_fields
