from datetime import datetime

from ccm_methods.ConcurrencyMethod import ConcurrencyMethod
from ccm_model.Transaction import Transaction
from ccm_model.Response import Response
from ccm_model.Enums import Action, TransactionStatus
from ccm_model.TransactionManager import TransactionManager
from ccm_helper.Row import Row


class Validation(ConcurrencyMethod):
    def __init__(self):
        self.transaction_manager: TransactionManager = None
        self.read_sets: dict[int, set[int]] = {}  # {transaction_id: set of resource_ids}
        self.write_sets: dict[int, set[int]] = {}  # {transaction_id: set of resource_ids}

        self.local_copies: dict[int, dict[int, any]] = {}  # {transaction_id: {resource_id: value}}
        self.validation_timestamps: dict[int, datetime] = {}
        self.finish_timestamps: dict[int, datetime] = {}
        
    def set_transaction_manager(self, transaction_manager: TransactionManager) -> None:
        self.transaction_manager = transaction_manager

    def log_object(self, obj: Row, transaction_id: int) -> None:
        transaction = self.transaction_manager.get_transaction(transaction_id)
        if not transaction:
            print(f"ERROR: Transaksi {transaction_id} tidak ditemukan untuk logging.")
            return
        
        # Initialize sets jika belum ada
        if transaction_id not in self.write_sets:
            self.write_sets[transaction_id] = set()
        if transaction_id not in self.local_copies:
            self.local_copies[transaction_id] = {}
        
        resource_id = obj.resource_key
        self.write_sets[transaction_id].add(resource_id)
        
        self.local_copies[transaction_id][resource_id] = obj
        
        print(f"[LOG] {resource_id} dicatat ke Write Set T{transaction_id} (local copy).")

    def validate_object(self, obj: Row, transaction_id: int, action: Action) -> Response:
        # gak validasi apa-apa, di akhir
        transaction = self.transaction_manager.get_transaction(transaction_id)
        if not transaction:
            return Response(False, f"Transaksi {transaction_id} tidak ditemukan.")

        resource_id = obj.resource_key

        if transaction_id not in self.read_sets:
            self.read_sets[transaction_id] = set()
        if transaction_id not in self.write_sets:
            self.write_sets[transaction_id] = set()
        if transaction_id not in self.local_copies:
            self.local_copies[transaction_id] = {}

        if action == Action.READ:
            self.read_sets[transaction_id].add(resource_id)
            print(f"T{transaction_id} membaca {resource_id} (dicatat ke Read Set)")
            return Response(True, f"Read pada {resource_id} berhasil.")
        
        elif action == Action.WRITE:
            self.write_sets[transaction_id].add(resource_id)
            self.local_copies[transaction_id][resource_id] = obj
            print(f"[WRITE] T{transaction_id} menulis {resource_id} (ke local copy)")
            return Response(True, f"Write pada {resource_id} berhasil (local).")
        
        return Response(False, f"Action {action} tidak dikenali.")

    def validate_transaction(self, transaction_id: int) -> Response:
        transaction = self.transaction_manager.get_transaction(transaction_id)
        if not transaction:
            return Response(False, f"Transaksi {transaction_id} tidak ditemukan.")

        self.validation_timestamps[transaction_id] = datetime.now()
        validation_ts = self.validation_timestamps[transaction_id]
        start_ts = transaction.get_start_time()

        print(f"[VALIDATION] Memvalidasi T{transaction_id}...")

        committed_transactions = []
        for tid, finish_ts in self.finish_timestamps.items():
            if tid != transaction_id:
                committed_transactions.append((tid, finish_ts))
        
        committed_transactions.sort(key=lambda x: x[1])

        for tid, finish_ts in committed_transactions:
            ti = self.transaction_manager.get_transaction(tid)
            if not ti:
                continue
            
            if finish_ts < start_ts:
                print(f"T{tid} selesai sebelum T{transaction_id} mulai")
                continue
            
            if start_ts < finish_ts < validation_ts:
                write_set_ti = self.write_sets.get(tid, set())
                read_set_tj = self.read_sets.get(transaction_id, set())
                
                intersection = write_set_ti & read_set_tj
                
                if len(intersection) == 0:
                    print(f"T{tid} dan T{transaction_id} tidak ada konflik")
                    continue
                else:
                    print(f"Validation gagal T{tid} menulis {intersection} yang dibaca T{transaction_id}")
                    return Response(False, f"Validation gagal. T{transaction_id} di-abort.")
            
            print(f"Validation gagal T{tid} conflict dengan T{transaction_id}")
            return Response(False, f"Validation gagal. T{transaction_id} di-abort.")

        print(f"T{transaction_id} berhasil divalidasi.")
        return Response(True, f"Validation berhasil untuk T{transaction_id}.")

    def end_transaction(self, transaction_id: int) -> Response:
        transaction = self.transaction_manager.get_transaction(transaction_id)
        if not transaction:
            return Response(False, f"Transaksi {transaction_id} tidak ditemukan.")
    
        validation_result = self.validate_transaction(transaction_id)
        if not validation_result.success:
            print(f"Validation gagal untuk T{transaction_id}.")
            self.transaction_manager.abort_transaction(transaction_id)
            self._cleanup_transaction(transaction_id)
            return Response(False, f"T{transaction_id} di-abort.")
        write_result = self.local_copies
        # return gimana ya
        
        self.finish_timestamps[transaction_id] = datetime.now()
        self.transaction_manager.terminate_transaction(transaction_id)
        self._cleanup_transaction(transaction_id)
        
        print(f"T{transaction_id} berakhir (status={transaction.status.name}).")
        return Response(True, f"T{transaction_id} berakhir dengan sukses.")

    def _cleanup_transaction(self, transaction_id: int) -> None:
        self.read_sets.pop(transaction_id, None)
        self.write_sets.pop(transaction_id, None)
        self.local_copies.pop(transaction_id, None)
        self.validation_timestamps.pop(transaction_id, None)
