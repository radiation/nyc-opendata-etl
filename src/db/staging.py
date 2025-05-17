from __future__ import annotations
from abc import ABC, abstractmethod
import pandas as pd


class StagingAdapter(ABC):
    """
    Defines the interface for loading dims/facts to a staging store.
    """

    @abstractmethod
    def load_dim(
        self,
        df: pd.DataFrame,
        table: str,
        *,
        truncate: bool = False,
    ) -> None:
        ...

    @abstractmethod
    def load_fact(
        self,
        df: pd.DataFrame,
        table: str,
        *,
        truncate: bool = False,
    ) -> None:
        ...
