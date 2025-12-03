from __future__ import annotations
from typing import Protocol, runtime_checkable
from .IQueryTree import IQueryTree


@runtime_checkable
class IParsedQuery(Protocol):
    query: str
    query_tree: IQueryTree | None

