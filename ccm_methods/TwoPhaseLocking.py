import random
import time

from ConcurrencyMethod import ConcurrencyMethod
from ccm_helper.Operation import Operation
from ccm_model.Transaction import Transaction
from ccm_model.Response import Response
from ccm_model.Enums import Action, TransactionStatus
from ccm_model.DeadlockDetector import DeadlockDetector
from ccm_model.LockManager import LockManager
from ccm_model.TransactionManager import TransactionManager
from ccm_helper.Row import Row


class TwoPhaseLocking(ConcurrencyMethod):
    def __init__(self):
        self.transaction_manager: TransactionManager = None
        self.lock_table: dict[int, Transaction] = {}
        self.deadlock_detector: DeadlockDetector = DeadlockDetector()
        self.lock_manager: LockManager = LockManager()
        self._next_tid = 1
        
    def set_transaction_manager(self, transaction_manager: TransactionManager) -> None:
        self.transaction_manager = transaction_manager

    def log_object(self, object: Row, transaction_id: int) -> None:
        """Mencatat objek (Row) yang diakses oleh transaksi."""
        transaction = self.transaction_manager.get_transaction(transaction_id)
        if not transaction:
             print(f"ERROR: Transaksi {transaction_id} tidak ditemukan untuk logging.")
             return
             
        transaction.write_set.append(object.resource_key)
        
        print(f"[LOG] {object.resource_key} dicatat ke Write Set T{transaction_id}.")

    def validate_object(self, object: Row, transaction_id: int, action: Action) -> Response:
        """Memvalidasi apakah transaksi boleh melakukan aksi tertentu pada objek."""
        resource_id = object.resource_key
        
        op = Operation(transaction_id=transaction_id, resource_id=resource_id, operation_type="R" if action == Action.READ else "W")

        success = self.lock_manager.request_lock(op)
        
        if success:
            print(f"[VALID] {action.name} pada {resource_id} berhasil divalidasi dan dikunci oleh T{transaction_id}")
            return Response(True, f"{action.name} pada {resource_id} divalidasi.")
        else:
            print(f"[KONFLIK] Gagal mendapatkan kunci untuk {resource_id} oleh T{transaction_id}. ABORT.")
            self.transaction_manager.abort_transaction(transaction_id) 
            self.lock_manager.release_locks(transaction_id)
            return Response(False, f"Transaksi {transaction_id} dibatalkan karena konflik pada {resource_id}.")

    def end_transaction(self, transaction_id: int) -> None:
        """Mengakhiri transaksi."""
        transaction = self.transaction_manager.get_transaction(transaction_id)
        if not transaction:
            return Response(False, f"Transaksi {transaction_id} tidak ditemukan.")
        
        self.transaction_manager.terminate_transaction(transaction_id)
        print(f"Transaksi {transaction_id} status menjadi TERMINATED.")

        try:
            self.lock_manager.release_locks(transaction_id)
        except Exception as e:
            return Response(False, f"Gagal melepaskan kunci untuk T{transaction_id}: {e}")

        try:
            self.transaction_manager.remove_transaction(transaction_id, None)
        except Exception as e:
            return Response(False, f"Gagal menghapus transaksi {transaction_id}: {e}")

        return Response(True, f"Transaksi {transaction_id} berakhir (status={transaction.status.name}).")
            
