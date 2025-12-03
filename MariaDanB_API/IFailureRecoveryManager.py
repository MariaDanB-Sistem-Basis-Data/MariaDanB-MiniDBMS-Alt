from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

__all__ = [
    "IFailureRecoveryManager",
    "SupportsExecutionResult",
    "SupportsRecoveryCriteria",
]


@runtime_checkable
class SupportsExecutionResult(Protocol):
    transaction_id: int
    query: str
    data: Any
    timestamp: Any
    message: str


@runtime_checkable
class SupportsRecoveryCriteria(Protocol):
    def getTimestamp(self) -> Any:
        ...

    def getTransactionId(self) -> Any:
        ...


@runtime_checkable
class IFailureRecoveryManager(Protocol):
    def writeLog(self, info: SupportsExecutionResult) -> None:
        ...

    def saveCheckpoint(self, activeTransactions: list[int] | None = None) -> None:
        ...

    def recover(self, criteria: SupportsRecoveryCriteria) -> list[dict[str, Any]]:
        ...

    def recoverFromSystemFailure(self) -> dict[str, list[dict[str, Any]]]:
        ...
