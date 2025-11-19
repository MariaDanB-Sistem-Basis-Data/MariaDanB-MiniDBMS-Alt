import random
import time

from ccm_model.Transaction import Transaction
from ccm_model.Response import Response
from ccm_model.Enums import Action, TransactionStatus
from ccm_model.DeadlockDetector import DeadlockDetector
from ccm_model.LockManager import LockManager
from ccm_model.TransactionManager import TransactionManager
from ConcurrencyMethod import ConcurrencyMethod
# sementara
class Row:
    def __init__(self, name: str):
        self.name = name

class ConcurrencyControlManager:
    def __init__(self):
        self.transaction_manager = TransactionManager()
        self.concurrency_method = ConcurrencyMethod()  
        
    def set_method(self, method: ConcurrencyMethod):
        """Ganti algoritma concurrency control secara dinamis."""
        self.concurrency_method = method

    def begin_transaction(self) -> int:
        """Memulai transaksi baru dan mengembalikan transaction_id."""
        print("[CCM] Begin transaction called")

        transaction_id = random.randint(1, 100)
        while self.transaction_manager.has_transaction(transaction_id):
            transaction_id = random.randint(1, 100)

        self.transaction_manager.begin_transaction(transaction_id)

        return transaction_id

    def commit_transaction(self, transaction_id: int) -> None:
        """Melakukan commit terhadap transaksi (write data ke log / storage)."""
        transaction = self.transaction_manager.get_transaction(transaction_id)
        if transaction:
            transaction.status = TransactionStatus.COMMITTED
            print(f"Melakukan commit transaksi {transaction_id}")
            self.end_transaction(transaction_id)
            return Response(True, f"Transaksi {transaction_id} berhasil di-commit.")
        else:
            print(f"Commit gagal. Transaksi {transaction_id} tidak ditemukan.")
            return Response(False, f"Transaksi {transaction_id} tidak ditemukan.")
            
    def abort_transaction(self, transaction_id: int) -> None:
        """Membatalkan transaksi dan melakukan rollback (abort)."""
        transaction = self.transaction_manager.get_transaction(transaction_id)
        if transaction:
            transaction.status = TransactionStatus.ABORTED
            print(f"Membatalkan transaksi {transaction_id}")
            # rollback
            self.end_transaction(transaction_id)
            return Response(True, f"Transaksi {transaction_id} berhasil di-abort.")
        else:
            print(f"Abort gagal. Transaksi {transaction_id} tidak ditemukan.")
            return Response(False, f"Transaksi {transaction_id} tidak ditemukan.")