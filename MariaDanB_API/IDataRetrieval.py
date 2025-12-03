from __future__ import annotations
from typing import Sequence, Protocol, runtime_checkable
from .ICondition import ICondition
__all__ = ["IDataRetrieval"]


@runtime_checkable
class IDataRetrieval(Protocol):
    table: str
    column: str | None
    conditions: Sequence[ICondition]

