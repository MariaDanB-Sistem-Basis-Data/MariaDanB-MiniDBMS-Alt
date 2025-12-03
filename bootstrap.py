# ini buat ngeload dependencies dari submodul
from __future__ import annotations

import sys
from dataclasses import dataclass
from importlib import import_module
from pathlib import Path
from typing import Any, Callable

ROOT = Path(__file__).resolve().parent
SUBMODULE_PATHS = [
    ROOT / "query_processor",
    ROOT / "query_optimizer",
    ROOT / "storage_manager",
    ROOT / "concurrency_control_manager",
    ROOT / "failure_recovery_manager",
    ROOT / "MariaDanB_API",
]


def ensure_sys_path() -> None:
    for candidate in reversed(SUBMODULE_PATHS):
        if candidate.exists():
            path_str = str(candidate)
            if path_str not in sys.path:
                sys.path.insert(0, path_str)

def _import_attr(module_path: str, attr_name: str) -> Any:
    module = import_module(module_path)
    return getattr(module, attr_name)


@dataclass(frozen=True)
class Dependencies:
    query_processor_factory: Callable[[], Any]
    rows_cls: Any
    execution_result_cls: Any
    query_type_enum: Any
    query_type_resolver: Callable[[str], Any]
    concurrency_control_cls: Any
    failure_recovery_factory: Callable[[], Any]


def load_dependencies() -> Dependencies:
    ensure_sys_path()

    QueryProcessor = _import_attr("QueryProcessor", "QueryProcessor")
    query_utils = import_module("qp_helper.query_utils")
    QueryType = query_utils.QueryType
    get_query_type = query_utils.get_query_type
    ExecutionResult = _import_attr("qp_model.ExecutionResult", "ExecutionResult")
    Rows = _import_attr("qp_model.Rows", "Rows")

    OptimizationEngine = _import_attr("QueryOptimizer", "OptimizationEngine")
    StorageManager = _import_attr("StorageManager", "StorageManager")
    DataRetrieval = _import_attr("storagemanager_model.data_retrieval", "DataRetrieval")
    DataWrite = _import_attr("storagemanager_model.data_write", "DataWrite")
    Condition = _import_attr("storagemanager_model.condition", "Condition")
    Schema = _import_attr("storagemanager_helper.schema", "Schema")

    storage_data_dir = ROOT / "data"
    storage_data_dir.mkdir(parents=True, exist_ok=True)

    def _make_data_retrieval(*, table: str, column: Any, conditions: Any) -> Any:
        return DataRetrieval(table, column, conditions)

    def _make_data_write(*, table: str, column: Any, conditions: Any, new_value: Any) -> Any:
        return DataWrite(table, column, conditions, new_value)

    def _make_condition(*, column: str, operation: str, operand: Any) -> Any:
        return Condition(column, operation, operand)

    def _make_schema() -> Any:
        return Schema()

    def _build_query_processor() -> Any:
        storage_manager = StorageManager(str(storage_data_dir))
        optimization_engine = OptimizationEngine()
        return QueryProcessor(
            optimization_engine=optimization_engine,
            storage_manager=storage_manager,
            data_retrieval_factory=_make_data_retrieval,
            data_write_factory=_make_data_write,
            condition_factory=_make_condition,
            schema_factory=_make_schema,
        )

    ConcurrencyControlManager = _import_attr(
        "ConcurrencyControlManager",
        "ConcurrencyControlManager",
    )

    get_failure_recovery_manager = _import_attr(
        "FailureRecovery",
        "getFailureRecoveryManager",
    )

    return Dependencies(
        query_processor_factory=_build_query_processor,
        rows_cls=Rows,
        execution_result_cls=ExecutionResult,
        query_type_enum=QueryType,
        query_type_resolver=get_query_type,
        concurrency_control_cls=ConcurrencyControlManager,
        failure_recovery_factory=get_failure_recovery_manager,
    )
