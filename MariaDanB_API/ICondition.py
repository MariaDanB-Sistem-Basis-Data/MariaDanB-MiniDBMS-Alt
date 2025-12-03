from __future__ import annotations
from typing import Any, Protocol, runtime_checkable
__all__ = ["ICondition"]


@runtime_checkable
class ICondition(Protocol):
    column: str
    operation: str
    operand: Any

