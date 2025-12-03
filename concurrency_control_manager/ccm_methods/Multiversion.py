from typing import Any, Dict, List, Tuple, Optional
from datetime import datetime

from ccm_methods.ConcurrencyMethod import ConcurrencyMethod
from ccm_model.Transaction import Transaction
from ccm_model.Response import Response
from ccm_model.Enums import Action, TransactionStatus
from ccm_model.TransactionManager import TransactionManager
from ccm_helper.resource_identifier import get_resource_id

class DataVersion:
    def __init__(self, value, write_timestamp: float, read_timestamp: float):
        self.value = value
        self.write_timestamp = write_timestamp  # W-timestamp(Q)
        self.read_timestamp = read_timestamp    # R-timestamp(Q)


class Multiversion(ConcurrencyMethod):
    def __init__(self):
        self.transaction_manager: TransactionManager = None
        # Versions sorted by write_timestamp, descensding
        self.versions: Dict[str, List[DataVersion]] = {}
        
        # Track transaction timestamps
        self.transaction_timestamps: Dict[int, float] = {}
        self._timestamp_counter: float = 0.0
        
        # Track read/write sets for each transaction
        self.read_sets: Dict[int, set[str]] = {}
        self.write_sets: Dict[int, set[str]] = {}
        
    def set_transaction_manager(self, transaction_manager: TransactionManager) -> None:
        self.transaction_manager = transaction_manager
        
    def _get_transaction_timestamp(self, transaction_id: int) -> float:
        if transaction_id not in self.transaction_timestamps:
            self._timestamp_counter += 1
            self.transaction_timestamps[transaction_id] = self._timestamp_counter
        return self.transaction_timestamps[transaction_id]

    def log_object(self, obj: Any, transaction_id: int) -> None:
        transaction = self.transaction_manager.get_transaction(transaction_id)
        if not transaction:
            return
        
        if transaction_id not in self.write_sets:
            self.write_sets[transaction_id] = set()
        
        resource_id = get_resource_id(obj)
        self.write_sets[transaction_id].add(resource_id)
        
        tx_timestamp = self._get_transaction_timestamp(transaction_id)
        
        value = getattr(obj, "data", None)
        result = self._write_version(resource_id, value, tx_timestamp, transaction_id)
        
        if not result.success:
            print(f"[MVTO LOG] T{transaction_id} write to {resource_id} failed: {result.message}")

    def validate_object(self, obj: Any, transaction_id: int, action: Action) -> Response:
        transaction = self.transaction_manager.get_transaction(transaction_id)
        if not transaction:
            return Response(False, f"Transaction {transaction_id} not found.")
        
        resource_id = get_resource_id(obj)
        tx_timestamp = self._get_transaction_timestamp(transaction_id)
        
        if transaction_id not in self.read_sets:
            self.read_sets[transaction_id] = set()
        if transaction_id not in self.write_sets:
            self.write_sets[transaction_id] = set()
        
        if action == Action.READ:
            # Track read
            self.read_sets[transaction_id].add(resource_id)
            
            # Find version to read
            result = self._read_version(resource_id, tx_timestamp, transaction_id)
            self.transaction_manager.add_read_set(transaction_id, resource_id)
            return result
            
        elif action == Action.WRITE:
            self.write_sets[transaction_id].add(resource_id)
            
            # Validate write
            value = getattr(obj, "data", None)
            result = self._write_version(resource_id, value, tx_timestamp, transaction_id)
            
            if not result.success:
                print(f"[MVTO WRITE] T{transaction_id} write to {resource_id} FAILED - will abort")
                self.transaction_manager.abort_transaction(transaction_id)
            self.transaction_manager.add_write_set(transaction_id, resource_id)
            return result
        
        return Response(False, f"Action {action} not recognized.")

    def _read_version(self, resource_id: str, tx_timestamp: float, transaction_id: int) -> Response:
        if resource_id not in self.versions:
            self.versions[resource_id] = [DataVersion(None, 0.0, 0.0)]
        
        selected_version = None
        for version in self.versions[resource_id]:
            if version.write_timestamp <= tx_timestamp:
                selected_version = version
                break
        
        if selected_version is None:
            return Response(False, f"No suitable version found for T{transaction_id} on {resource_id}")
        
        if tx_timestamp > selected_version.read_timestamp:
            selected_version.read_timestamp = tx_timestamp
        
        return Response(True, 
            f"Read successful: T{transaction_id} on {resource_id}, version W-TS={selected_version.write_timestamp}")

    def _write_version(self, resource_id: str, value, tx_timestamp: float, transaction_id: int) -> Response:
        if resource_id not in self.versions:
            self.versions[resource_id] = [DataVersion(None, 0.0, 0.0)]
        
        selected_version = None
        selected_index = None
        for i, version in enumerate(self.versions[resource_id]):
            if version.write_timestamp <= tx_timestamp:
                selected_version = version
                selected_index = i
                break
        
        if selected_version is None:
            return Response(False, f"No suitable version found for write by T{transaction_id} on {resource_id}")
        
        # Check if TS(T) < R-timestamp(Qi)
        #CONFLICT
        if tx_timestamp < selected_version.read_timestamp:
            return Response(False, 
                f"ROLLBACK required: T{transaction_id} (TS={tx_timestamp}) < " +
                f"R-timestamp({selected_version.read_timestamp}) on {resource_id}")
        
        if tx_timestamp == selected_version.write_timestamp:
            # Overwrite
            selected_version.value = value
            return Response(True, f"Overwrite successful for T{transaction_id} on {resource_id}")
        
        # TS(T) > W-timestamp(Qi) - Create new version
        new_version = DataVersion(value, tx_timestamp, tx_timestamp)
        self.versions[resource_id].insert(selected_index, new_version)
        
        return Response(True, f"New version created for T{transaction_id} on {resource_id}")

    def end_transaction(self, transaction_id: int) -> Response:
        transaction = self.transaction_manager.get_transaction(transaction_id)
        if not transaction:
            return Response(False, f"Transaction {transaction_id} not found.")
        
        
        self.transaction_manager.terminate_transaction(transaction_id)
        
        self._cleanup_transaction(transaction_id)
        
        return Response(True, f"T{transaction_id} ended successfully (MVTO).")

    def _cleanup_transaction(self, transaction_id: int) -> None:
        self.read_sets.pop(transaction_id, None)
        self.write_sets.pop(transaction_id, None)
        self.transaction_timestamps.pop(transaction_id, None)

    def get_versions(self, resource) -> List[DataVersion]:
        """
        Return version list for a resource; accepts Row or plain identifier.
        Falls back to table-only match if caller passes a row-style key.
        """
        resource_id = get_resource_id(resource)
        versions = self.versions.get(resource_id)

        # If caller passed row-style key (e.g., "Users:1") but locking is table-level.
        if versions is None and isinstance(resource, str) and ":" in resource:
            table_only = resource.split(":", 1)[0]
            versions = self.versions.get(table_only)

        return versions or []
