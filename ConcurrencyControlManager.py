import random
import time

from ccm_model.Transaction import Transaction
from ccm_model.Response import Response
from ccm_model.Enums import Action, TransactionStatus
from ccm_model.DeadlockDetector import DeadlockDetector
from ccm_model.LockManager import LockManager
from ccm_model.TransactionManager import TransactionManager
from ccm_methods.ConcurrencyMethod import ConcurrencyMethod
# sementara
class Row:
    def __init__(self, name: str):
        self.name = name

class ConcurrencyControlManager:
    def __init__(self):
        self.transaction_manager = TransactionManager()
        self.concurrency_method = None
        
    def set_method(self, method: ConcurrencyMethod):
        """Ganti algoritma concurrency control secara dinamis."""
        self.concurrency_method = method
        method.set_transaction_manager(self.transaction_manager)

    def begin_transaction(self, transaction_id) -> int:
        """Memulai transaksi baru dan mengembalikan transaction_id."""
        print("[CCM] Begin transaction called")

        transaction_id = self.transaction_manager.begin_transaction(transaction_id)

        return transaction_id
    
    def log_object(self, obj, transaction_id: int) -> None:
        """Forward ke concurrency method."""
        if not self.concurrency_method:
            raise RuntimeError("Concurrency method belum diset!")
        return self.concurrency_method.log_object(obj, transaction_id)
    
    def validate_object(self, obj, transaction_id: int, action):
        """Forward ke concurrency method."""
        if not self.concurrency_method:
            raise RuntimeError("Concurrency method belum diset!")
        return self.concurrency_method.validate_object(obj, transaction_id, action)

    def end_transaction(self, transaction_id: int):
        """Forward ke concurrency method."""
        if not self.concurrency_method:
            raise RuntimeError("Concurrency method belum diset!")
        return self.concurrency_method.end_transaction(transaction_id)

    def commit_transaction(self, transaction_id: int) -> None:
        """Melakukan commit terhadap transaksi (write data ke log / storage)."""
        transaction = self.transaction_manager.commit_transaction(transaction_id)
        if transaction:
            print(f"Melakukan commit transaksi {transaction_id}")
            self.end_transaction(transaction_id)
            return Response(True, f"Transaksi {transaction_id} berhasil di-commit & terminated.")
        else:
            print(f"Commit gagal. Transaksi {transaction_id} tidak ditemukan.")
            return Response(False, f"Transaksi {transaction_id} tidak ditemukan.")
            
    def abort_transaction(self, transaction_id: int) -> None:
        """Membatalkan transaksi dan melakukan rollback (abort)."""
        success = self.transaction_manager.abort_transaction(transaction_id)
        if success:
            print(f"Membatalkan transaksi {transaction_id}")
            # rollback
            self.end_transaction(transaction_id)
            return Response(True, f"Transaksi {transaction_id} berhasil di-abort & terminated.")
        else:
            print(f"Abort gagal. Transaksi {transaction_id} tidak ditemukan.")
            return Response(False, f"Transaksi {transaction_id} tidak ditemukan.")