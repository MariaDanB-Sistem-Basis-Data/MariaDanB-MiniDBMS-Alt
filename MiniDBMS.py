from __future__ import annotations

import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

from bootstrap import Dependencies


class MiniDBMS:
    def __init__(self, deps: Dependencies) -> None:
        self._deps = deps
        self.query_processor = deps.query_processor_factory()
        self.concurrency_manager = deps.concurrency_control_cls()
        

        # inisialisasi metode concurrency control, misal Two-Phase Locking
        try:
            import sys
            from pathlib import Path
            ccm_path = Path(__file__).parent / "Concurrency-Control-Manager"
            if str(ccm_path) not in sys.path:
                sys.path.insert(0, str(ccm_path))
            
            from ccm_methods.TwoPhaseLocking import TwoPhaseLocking  # type: ignore
            self.concurrency_manager.set_method(TwoPhaseLocking())
        except (ImportError, AttributeError):
            pass  
        
        self.failure_recovery_manager = deps.failure_recovery_factory()
        self._rows_cls = deps.rows_cls
        self._execution_result_cls = deps.execution_result_cls
        self._query_type_enum = deps.query_type_enum
        self._get_query_type = deps.query_type_resolver
        self._active_transactions: set[int] = set()
        self._latest_transaction_id: Optional[int] = None

    def execute(self, query: str) -> Any:
        query_type = self._get_query_type(query)

        if query_type in (self._query_type_enum.SELECT, self._query_type_enum.UPDATE):
            result = self.query_processor.execute_query(query)
            self._log_with_failure_recovery(result)
            return result

        if query_type == self._query_type_enum.BEGIN_TRANSACTION:
            transaction_id = self.concurrency_manager.begin_transaction()
            self._active_transactions.add(transaction_id)
            self._latest_transaction_id = transaction_id

            payload = self._rows_cls.from_list([
                {"transaction_id": transaction_id, "status": "ACTIVE"}
            ])
            result = self._execution_result_cls(
                transaction_id=transaction_id,
                timestamp=datetime.now(),
                message="Transaction started via Concurrency Control Manager",
                data=payload,
                query=query,
            )
            self._log_with_failure_recovery(result)
            return result

        if query_type == self._query_type_enum.COMMIT:
            return self._handle_transaction_completion(query, commit=True)

        if query_type in (self._query_type_enum.ABORT, self._query_type_enum.ROLLBACK):
            return self._handle_transaction_completion(query, commit=False)

        fallback = self.query_processor.execute_query(query)
        self._log_with_failure_recovery(fallback)
        return fallback

    def _handle_transaction_completion(self, query: str, commit: bool) -> Any:
        transaction_id = self._resolve_transaction_id(query)

        if transaction_id is None:
            return self._execution_result_cls(
                transaction_id=-1,
                timestamp=datetime.now(),
                message="No active transaction available for completion",
                data=-1,
                query=query,
            )

        if transaction_id not in self._active_transactions:
            return self._execution_result_cls(
                transaction_id=transaction_id,
                timestamp=datetime.now(),
                message=f"Transaction {transaction_id} not tracked by coordinator",
                data=-1,
                query=query,
            )

        response = self._dispatch_completion(transaction_id, commit)
        success = getattr(response, "success", False)
        message = getattr(response, "message", "Operation dispatched to transaction manager")

        if success:
            self._active_transactions.discard(transaction_id)
            if self._latest_transaction_id == transaction_id:
                self._latest_transaction_id = max(self._active_transactions) if self._active_transactions else None

        payload = self._rows_cls.from_list([
            {
                "transaction_id": transaction_id,
                "status": "COMMITTED" if commit else "ABORTED",
                "success": success,
            }
        ])

        result = self._execution_result_cls(
            transaction_id=transaction_id,
            timestamp=datetime.now(),
            message=message,
            data=payload,
            query=query,
        )

        self._log_with_failure_recovery(result)
        return result

    def _dispatch_completion(self, transaction_id: int, commit: bool) -> Any:
        if commit:
            return self.concurrency_manager.commit_transaction(transaction_id)
        return self.concurrency_manager.abort_transaction(transaction_id)

    def _resolve_transaction_id(self, query: str) -> Optional[int]:
        explicit_id = self._extract_transaction_id(query)
        if explicit_id is not None:
            return explicit_id

        if self._latest_transaction_id and self._latest_transaction_id in self._active_transactions:
            return self._latest_transaction_id

        if len(self._active_transactions) == 1:
            return next(iter(self._active_transactions))

        return None

    @staticmethod
    def _extract_transaction_id(query: str) -> Optional[int]:
        match = re.search(r"\d+", query)
        if match:
            try:
                return int(match.group())
            except ValueError:
                return None
        return None

    def _log_with_failure_recovery(self, result: Any) -> None:
        writer = getattr(self.failure_recovery_manager, "writeLog", None)
        if callable(writer):
            try:
                writer(result)
            except Exception:
                pass

    
    def checkpoint(self) -> bool:
        # ini buat nge-trigger checkpoint di Failure Recovery Manager
        # bisa diinvoke scr manual atau otomatis periodik
        # kalo manual pake dbms.checkpoint()
        checkpoint_fn = getattr(self.failure_recovery_manager, "saveCheckpoint", None)
        if callable(checkpoint_fn):
            try:
                active_tx_list = list(self._active_transactions) if self._active_transactions else None
                checkpoint_fn(active_tx_list)
                return True
            except Exception as e:
                print(f"Checkpoint failed: {e}")
                return False
        return False
    
    def recover_from_failure(self) -> Any:
        # ini buat nge-trigger recovery process di Failure Recovery Manager
        recover_fn = getattr(self.failure_recovery_manager, "recoverFromSystemFailure", None)
        if callable(recover_fn):
            try:
                return recover_fn()
            except Exception as e:
                print(f"Recovery failed: {e}")
                return None
        return None
    
