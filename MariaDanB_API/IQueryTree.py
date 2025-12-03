from __future__ import annotations
from typing import Any, Protocol, runtime_checkable
__all__ = ["IQueryTree"]


@runtime_checkable
class IQueryTree(Protocol):
	type: str
	val: Any
	childs: list[IQueryTree]
	parent: IQueryTree | None

	def add_child(self, node: IQueryTree) -> None:
		...

	def replace_child(self, old: IQueryTree, new: IQueryTree) -> bool:
		...

	def detach(self) -> None:
		...

