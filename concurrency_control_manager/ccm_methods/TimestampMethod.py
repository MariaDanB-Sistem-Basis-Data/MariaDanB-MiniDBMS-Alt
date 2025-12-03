from typing import Any

from ccm_methods.ConcurrencyMethod import ConcurrencyMethod
from ccm_helper.Operation import Operation
from ccm_model.Response import Response
from ccm_model.Enums import Action
from ccm_model.Timestamp import TimestampManager
from ccm_model.TransactionManager import TransactionManager
from ccm_helper.resource_identifier import get_resource_id


class TimestampMethod(ConcurrencyMethod):
    def __init__(self):
        self.transaction_manager: TransactionManager = None
        self.timestamp_manager: TimestampManager = TimestampManager()
        
    def set_transaction_manager(self, transaction_manager: TransactionManager) -> None:
        self.transaction_manager = transaction_manager

    def log_object(self, obj: Any, transaction_id: int) -> None:
        transaction = self.transaction_manager.get_transaction(transaction_id)
        if not transaction:
            print(f"ERROR: Transaksi {transaction_id} tidak ditemukan untuk logging.")
            return
             
        resource_id = get_resource_id(obj)
        transaction.write_set.append(resource_id)
        print(f"[LOG] {resource_id} dicatat ke Write Set T{transaction_id}.")

    def validate_object(self, obj: Any, transaction_id: int, action: Action) -> Response:
        transaction = self.transaction_manager.get_transaction(transaction_id)
        if not transaction:
            return Response(False, f"Transaksi {transaction_id} tidak ditemukan.")

        resource_id = get_resource_id(obj)
        
        tx_datetime = transaction.get_start_time()

        op_type = "read" if action == Action.READ else "write"
        operation = Operation(transaction_id, op_type, resource_id)

        response = self.timestamp_manager.validate_operation(operation, tx_datetime)
        
        if response.success:
            if op_type == "read":
                self.transaction_manager.add_read_set(transaction_id, resource_id)
            else:
                self.transaction_manager.add_write_set(transaction_id, resource_id)
            print(f"[VALID] {action.name} pada {resource_id} oleh T{transaction_id} berhasil")
        else:
            print(f"[ABORT] T{transaction_id}: {response.message}")
            self.transaction_manager.abort_transaction(transaction_id)
            
        return response

    def end_transaction(self, transaction_id: int) -> Response:
        transaction = self.transaction_manager.get_transaction(transaction_id)
        if not transaction:
            return Response(False, f"Transaksi {transaction_id} tidak ditemukan.")
        
        self.transaction_manager.terminate_transaction(transaction_id)
        print(f"[END] Transaksi {transaction_id} status menjadi {transaction.status.name}.")

        try:
            self.transaction_manager.remove_transaction(transaction_id)
        except Exception as e:
            return Response(False, f"Gagal menghapus transaksi {transaction_id}: {e}")

        return Response(True, f"Transaksi {transaction_id} berakhir(status={transaction.status.name}).")
