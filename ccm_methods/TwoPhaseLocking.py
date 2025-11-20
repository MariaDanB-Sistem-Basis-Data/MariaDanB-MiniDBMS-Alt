import random
import time

from ccm_methods.ConcurrencyMethod import ConcurrencyMethod
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

    def validate_object(self, obj: Row, transaction_id: int, action: Action) -> Response:
        transaction = self.transaction_manager.get_transaction(transaction_id)
        if not transaction:
            return Response(False, f"Transaksi {transaction_id} tidak ditemukan.")

        resource_id = obj.resource_key

        op_type = "R" if action == Action.READ else "W"
        operation = Operation(transaction_id, op_type, resource_id)

        result = self.lock_manager.request_lock(op, return_lock_holders=True)

        if isinstance(result, tuple):
            success, lock_holders = result
        else:
            success = result
            lock_holders = set()
            
        if success:
            print(f"[VALID] {action.name} pada {resource_id} berhasil divalidasi")
            return Response(True, ...)
        else:
            for h in lock_holders:
                self.deadlock_detector.add_wait_edge(transaction_id, h)

            has_dl, cycle = self.deadlock_detector.check_deadlock()
            if has_dl:
                victim = self.pick_victim(cycle) 
                print(f"[DEADLOCK] Victim: T{victim}")
                self.abort_transaction(victim)
                return Response(False, f"Deadlock. Victim T{victim} di-abort.")

            # klo bukan deadlock, wound-wait
            wait = False
            for h in lock_holders:
                if self.transaction_manager.get_transaction(transaction_id).get_start_time() < self.transaction_manager.get_transaction(h).get_start_time():
                    wait = True
                    break 
            if wait: 
                print(f"[WAIT] T{transaction_id} menunggu lock dari {lock_holders}")
                return Response(False, f"T{transaction_id} harus menunggu {lock_holders}")
            else:
                print(f"[WOUND] T{transaction_id} membunuh {lock_holders}")
                for h in lock_holders:
                    self.transaction_manager.abort_transaction(h)
                    self.lock_manager.release_locks(transaction_id)
                return Response(False, f"{lock_holders} di abort karena T{transaction_id} adalah transaksi yang lebih tua.")


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
            self.transaction_manager.remove_transaction(transaction_id)
        except Exception as e:
            return Response(False, f"Gagal menghapus transaksi {transaction_id}: {e}")

        return Response(True, f"Transaksi {transaction_id} berakhir (status={transaction.status.name}).")
            
    def pick_victim(self, cycle):
        # yg paling muda
        victim = None
        max_start_time = -1
        
        for tid in cycle:
            trx = self.transaction_manager.get_transaction(tid)
            if trx.get_start_time() > max_start_time:
                max_start_time = trx.get_start_time()
                victim = trx

        return victim

