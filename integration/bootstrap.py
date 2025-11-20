# ini buat ngeload dependencies dari submodul
from __future__ import annotations

import sys
from dataclasses import dataclass
from importlib import import_module
from pathlib import Path
from typing import Any, Callable

ROOT = Path(__file__).resolve().parent.parent
SUBMODULE_PATHS = [
    ROOT / "Query-Processor",
    ROOT / "Query-Processor" / "query_processor",
    ROOT / "Query-Processor" / "concurrency_control_manager",
    ROOT / "Query-Processor" / "failure_recovery_manager",
    ROOT / "Query-Processor" / "storage_manager",
    ROOT / "Query-Processor" / "query_optimizer",
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
    query_processor_cls: Any
    rows_cls: Any
    execution_result_cls: Any
    query_type_enum: Any
    query_type_resolver: Callable[[str], Any]
    concurrency_control_cls: Any
    failure_recovery_factory: Callable[[], Any]


def load_dependencies() -> Dependencies:
    ensure_sys_path()

    QueryProcessor = _import_attr("query_processor.QueryProcessor", "QueryProcessor")
    query_utils = import_module("query_processor.helper.query_utils")
    QueryType = query_utils.QueryType
    get_query_type = query_utils.get_query_type
    ExecutionResult = _import_attr("query_processor.model.ExecutionResult", "ExecutionResult")
    Rows = _import_attr("query_processor.model.Rows", "Rows")

    ConcurrencyControlManager = _import_attr(
        "concurrency_control_manager.ConcurrencyControlManager",
        "ConcurrencyControlManager",
    )

    get_failure_recovery_manager = _import_attr(
        "failure_recovery_manager.FailureRecovery",
        "getFailureRecoveryManager",
    )

    return Dependencies(
        query_processor_cls=QueryProcessor,
        rows_cls=Rows,
        execution_result_cls=ExecutionResult,
        query_type_enum=QueryType,
        query_type_resolver=get_query_type,
        concurrency_control_cls=ConcurrencyControlManager,
        failure_recovery_factory=get_failure_recovery_manager,
    )
