from __future__ import annotations
from typing import Protocol, runtime_checkable
from .ISchema import ISchema
__all__ = ["ISchemaManager"]


@runtime_checkable
class ISchemaManager(Protocol):
    schemas : dict
    
    def list_tables(self) -> list[str]:
        ...

    def get_table_schema(self, table: str) -> ISchema | None:
        ...

    def add_table_schema(self, table: str, schema: ISchema) -> None:
        ...

    def save_schemas(self) -> None:
        ...

    def load_schemas(self) -> None:
        ...
