from __future__ import annotations
from typing import Protocol, runtime_checkable
from .IParsedQuery import IParsedQuery
__all__ = ["IOptimizationEngine"]


@runtime_checkable
class IOptimizationEngine(Protocol):
    def parse_query(self, query: str) -> IParsedQuery:
        ...

    def optimize_query(self, parsed_query: IParsedQuery) -> IParsedQuery:
        ...

    def optimize_query_non_join(self, parsed_query: IParsedQuery) -> IParsedQuery:
        ...

    def get_cost(self, parsed_query: IParsedQuery) -> int:
        ...

