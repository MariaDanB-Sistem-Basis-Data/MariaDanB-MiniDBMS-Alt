from __future__ import annotations
from dataclasses import dataclass
from typing import Generic, List, TypeVar

T = TypeVar("T")

@dataclass
class Rows(Generic[T]):
    data: List[T]
    rows_count: int

    @classmethod
    def from_list(cls, items: List[T]) -> "Rows[T]":
        return cls(data=items, rows_count=len(items))
