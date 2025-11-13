from models.Transaction import Transaction
from models.Response import Response
from models.Enums import Action
from models.DeadlockDetector import DeadlockDetector

# sementara
class Row:
    def __init__(self, name: str):
        self.name = name

class ConcurrencyControlManager:
    def __init__(self):
        self.transaction_table: dict[int, Transaction] = {}
        self.lock_table: dict[int, Transaction] = {}
        self.deadlock_detector: DeadlockDetector = DeadlockDetector()
        self._next_tid = 1

    def begin_transaction(self) -> int:
        """Memulai transaksi baru dan mengembalikan transaction_id."""
        print(f"Transaksi dimulai.")

    def log_object(self, object: Row, transaction_id: int) -> None:
        """Mencatat objek (Row) yang diakses oleh transaksi."""
        print(f"Mencatat akses ke {object.name} oleh Transaksi {transaction_id}")

    def validate_object(self, object: Row, transaction_id: int, action: Action) -> Response:
        """Memvalidasi apakah transaksi boleh melakukan aksi tertentu pada objek."""
        print(f"Memvalidasi {action.name} pada {object.name} untuk T{transaction_id}")
        return Response(True, f"{action.name} pada {object.name} divalidasi untuk T{transaction_id}")

    def end_transaction(self, transaction_id: int) -> None:
        """Mengakhiri transaksi."""
        print(f"Transaksi {transaction_id} diakhiri.")
            
    def commit_transaction(self, transaction_id: int) -> None:
        """Melakukan commit terhadap transaksi (write data ke log / storage)."""
        print(f"Melakukan commit transaksi {transaction_id}")
            
    def abort_transaction(self, transaction_id: int) -> None:
        """Membatalkan transaksi dan melakukan rollback (abort)."""
        print(f"Membatalkan transaksi {transaction_id}")
