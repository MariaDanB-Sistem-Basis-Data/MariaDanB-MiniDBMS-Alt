from __future__ import annotations
from typing import Any, Protocol, runtime_checkable

from .IDataRetrieval import IDataRetrieval
from .IDataWrite import IDataWrite
from .ISchemaManager import ISchemaManager

__all__ = ["IStorageManager"]


@runtime_checkable
class IStorageManager(Protocol):
    base_path: str
    schema_manager: ISchemaManager
    def read_block(self, request: IDataRetrieval) -> Any:
        ...

    def write_block(self, request: IDataWrite) -> Any:
        ...

    def delete_block(self, request: IDataWrite) -> Any:
        ...

    def get_stats(self, table_name=None) -> Any:
        ...
