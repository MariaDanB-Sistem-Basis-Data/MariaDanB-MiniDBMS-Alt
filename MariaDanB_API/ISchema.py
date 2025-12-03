from __future__ import annotations
from typing import Protocol, runtime_checkable, Any, Dict, List, Optional
__all__ = ["ISchema"]


@runtime_checkable
class ISchema(Protocol):
    def add_attribute(self, attributes: Optional[List[Dict[str, Any]]] = None):
        ...
    def get_attributes(self) -> dict[str, tuple[str, int]]:
        ...
    def get_metadata(self):
        ...
    def serialize(self):
        ...
    def deserialize(self, data: bytes):
        ...