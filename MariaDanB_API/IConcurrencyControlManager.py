from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

__all__ = [
    "IConcurrencyControlManager",
    "IConcurrencyMethod",
    "ITransactionResponse",
]


@runtime_checkable
class ITransactionResponse(Protocol):
    success: bool
    message: str


@runtime_checkable
class IConcurrencyMethod(Protocol):
    def set_transaction_manager(self, transaction_manager: Any) -> None:
        ...

    def log_object(self, obj: Any, transaction_id: int) -> Any:
        ...

    def validate_object(self, obj: Any, transaction_id: int, action: Any) -> Any:
        ...

    def end_transaction(self, transaction_id: int) -> Any:
        ...


@runtime_checkable
class IConcurrencyControlManager(Protocol):
    concurrency_method: IConcurrencyMethod | None

    def set_method(self, method: IConcurrencyMethod) -> None:
        ...

    def begin_transaction(self) -> int:
        ...

    def log_object(self, obj: Any, transaction_id: int) -> Any:
        ...

    def validate_object(self, obj: Any, transaction_id: int, action: Any) -> Any:
        ...

    def end_transaction(self, transaction_id: int) -> Any:
        ...

    def commit_transaction(self, transaction_id: int) -> ITransactionResponse | Any:
        ...

    def abort_transaction(self, transaction_id: int) -> ITransactionResponse | Any:
        ...
