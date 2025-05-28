# src/db/adapters/production.py
from __future__ import annotations

from abc import ABC, abstractmethod


class ProductionAdapter(ABC):
    @abstractmethod
    def promote_dim(self, table: str) -> None:
        ...

    @abstractmethod
    def promote_fact(self, table: str) -> None:
        ...
