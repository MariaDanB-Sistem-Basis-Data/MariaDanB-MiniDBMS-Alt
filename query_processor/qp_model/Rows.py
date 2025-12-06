from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
from typing import Generic, List, TypeVar, Union

T = TypeVar("T")

@dataclass
class Rows(Generic[T]):
    data: List[T]
    rows_count: int
    table_source: Union[str, None] = None

    @classmethod
    def from_list(cls, items: List[T], table_source: Union[str, None] = None) -> "Rows[T]":
        return cls(data=items, rows_count=len(items), table_source=table_source)
