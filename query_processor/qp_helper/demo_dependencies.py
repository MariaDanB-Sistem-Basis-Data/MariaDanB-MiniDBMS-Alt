from __future__ import annotations

import sys
from importlib import import_module
from pathlib import Path
from typing import Any, Iterable, TYPE_CHECKING

ROOT = Path(__file__).resolve().parent.parent
WORKSPACE_ROOT = ROOT.parent

print("ROOT:", ROOT)
print("WORKSPACE_ROOT:", WORKSPACE_ROOT)


def _maybe_add_path(path: Path) -> None:
    if not path.exists():
        return
    path_str = str(path)
    if path_str not in sys.path:
        sys.path.append(path_str)


for candidate in (
    WORKSPACE_ROOT,
    WORKSPACE_ROOT / "storage_manager",
    WORKSPACE_ROOT / "query_optimizer",
    WORKSPACE_ROOT / "query_processor",
):
    _maybe_add_path(candidate)

def _import_attr(candidates: Iterable[tuple[str, str]]) -> Any:
    last_error: ModuleNotFoundError | None = None
    for module_name, attr_name in candidates:
        try:
            module = import_module(module_name)
            return getattr(module, attr_name)
        except ModuleNotFoundError as exc:
            last_error = exc
            continue
    raise ModuleNotFoundError(
        "Unable to import required dependency; attempted: "
        + ", ".join(f"{mod}.{attr}" for mod, attr in candidates)
    ) from last_error


StorageManagerCls = _import_attr(
    [
        ("storage_manager.StorageManager", "StorageManager"),
        ("StorageManager", "StorageManager"),
    ]
)
OptimizationEngineCls = _import_attr(
    [
        ("query_optimizer.QueryOptimizer", "OptimizationEngine"),
        ("QueryOptimizer", "OptimizationEngine"),
    ]
)
DataRetrievalCls = _import_attr(
    [
        ("storage_manager.storagemanager_model.data_retrieval", "DataRetrieval"),
        ("storagemanager_model.data_retrieval", "DataRetrieval"),
    ]
)
DataWriteCls = _import_attr(
    [
        ("storage_manager.storagemanager_model.data_write", "DataWrite"),
        ("storagemanager_model.data_write", "DataWrite"),
    ]
)
ConditionCls = _import_attr(
    [
        ("storage_manager.storagemanager_model.condition", "Condition"),
        ("storagemanager_model.condition", "Condition"),
    ]
)
SchemaCls = _import_attr(
    [
        ("storage_manager.storagemanager_helper.schema", "Schema"),
        ("storagemanager_helper.schema", "Schema"),
    ]
)

_DEFAULT_DATA_DIR = (ROOT / "testing_data" ).resolve()
_DEFAULT_DATA_DIR.mkdir(parents=True, exist_ok=True)

if TYPE_CHECKING:
    from QueryProcessor import QueryProcessor


def build_query_processor(storage_path: str | Path | None = None) -> "QueryProcessor":
    from QueryProcessor import QueryProcessor

    data_dir = Path(storage_path) if storage_path else _DEFAULT_DATA_DIR
    data_dir.mkdir(parents=True, exist_ok=True)

    storage_manager = StorageManagerCls(str(data_dir))
    optimization_engine = OptimizationEngineCls()

    def _make_data_retrieval(*, table: str, column: Any, conditions: Any) -> Any:
        return DataRetrievalCls(table, column, conditions)

    def _make_data_write(*, table: str, column: Any | None, conditions: Any, new_value: Any) -> Any:
        return DataWriteCls(table, column, conditions, new_value)

    def _make_condition(*, column: str, operation: str, operand: Any) -> Any:
        return ConditionCls(column, operation, operand)

    def _make_schema() -> Any:
        return SchemaCls()

    return QueryProcessor(
        optimization_engine=optimization_engine,
        storage_manager=storage_manager,
        data_retrieval_factory=_make_data_retrieval,
        data_write_factory=_make_data_write,
        condition_factory=_make_condition,
        schema_factory=_make_schema,
    )


__all__ = ["build_query_processor"]
