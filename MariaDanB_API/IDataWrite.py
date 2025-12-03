from __future__ import annotations
from typing import Any, Sequence, Protocol, runtime_checkable
from .ICondition import ICondition
__all__ = ["IDataWrite"]


@runtime_checkable
class IDataWrite(Protocol):
    table: str
    column: str | None
    conditions: Sequence[ICondition]
    new_value: Any

