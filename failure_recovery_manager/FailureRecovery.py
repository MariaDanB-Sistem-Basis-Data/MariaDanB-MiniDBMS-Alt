from typing import Dict, Optional, List, Any
from datetime import datetime

# Exceptions
from frm_model.Rows import Rows
from frm_model.ExecutionResult import ExecutionResult


# We'll use this mainly
from frm_model.RecoveryCriteria import RecoveryCriteria
from frm_model.LogEntry import LogEntry, LogEntryType
from frm_model.Checkpoint import Checkpoint
from frm_helper.Singleton import singleton
from frm_helper.WriteAheadLog import WriteAheadLog
from frm_helper.Buffer import Buffer


@singleton
class FailureRecoveryManager:
    def __init__(
        self,
        logFilePath: str = "frm_logs/wal.log",
        bufferSize: int = 100,
        checkpointIntervalSeconds: int = 300
    ):
        if not hasattr(self, 'initialized'):
            self._writeAheadLog = WriteAheadLog(logFilePath)
            self._buffer: Buffer[Any] = Buffer(maxSize=bufferSize)
            self._checkpointInterval = checkpointIntervalSeconds
            self._lastCheckpointTime = datetime.now()
            self._routine = None  # Storage Manager's flush method (set via setRoutine)
            self._readFromDisk = None  # Storage Manager's read method (set via setReadMethod)
            """
            Usage from Storage Manager:
            self.frm_instance = frm_instance
            if self.frm_instance is not None:
                self.frm_instance.setRoutine(self.flushBufferToDisk) # tentative name
                self.frm_instance.setReadMethod(self._readFullTableAsRows) # tentative name
            """
            self.initialized = True

    def _validateExecutionResult(self, info: Any) -> bool:
        required_attrs = ['query', 'transaction_id', 'data', 'timestamp', 'message']

        for attr in required_attrs:
            if not hasattr(info, attr):
                raise ValueError(f"ExecutionResult missing required attribute: {attr}")

        return True

    # cc: @Query-Processor
    def writeLog(self, info: ExecutionResult) -> None:
        self._validateExecutionResult(info)

        next_log_id = self._writeAheadLog.getNextLogId()

        query = info.query

        entry_type = LogEntryType.UPDATE
        if "BEGIN" in query.upper():
            entry_type = LogEntryType.START
        elif "COMMIT" in query.upper():
            entry_type = LogEntryType.COMMIT
        elif "ABORT" in query.upper() or "ROLLBACK" in query.upper():
            entry_type = LogEntryType.ABORT

        data_item = None
        old_val = None
        new_val = None

        data = info.data
        update_details: Dict[str, Any] = {}

        if isinstance(data, Rows):
            if data.data and len(data.data) > 0:
                update_details = data.data[0]
        elif isinstance(data, int):
            pass

        # {'table': 'table_name', 'column': 'col_name', 'id': row_id, 'old_value': old, 'new_value': new}
        if entry_type == LogEntryType.UPDATE and update_details:
            table = update_details.get('table', '')
            col = update_details.get('column', '')
            row_id = update_details.get('id', '')

            data_item = f"{table}.{col}[{row_id}]"
            old_val = update_details.get('old_value')
            new_val = update_details.get('new_value')

        log_entry = LogEntry(
            logId=next_log_id,
            transactionId=info.transaction_id,
            timestamp=datetime.now(),
            entryType=entry_type,
            dataItem=data_item,
            oldValue=old_val,
            newValue=new_val
        )

        # Write to WAL buffer (WAL protocol: log before data write)
        self._writeAheadLog.appendLog(log_entry)

        # Trigger checkpoint if needed
        if self._writeAheadLog.needsFlush() or self._shouldCheckpoint():
            self.saveCheckpoint()

    def setRoutine(self, routine: callable) -> None:
        self._routine = routine

    def setReadMethod(self, readMethod: callable) -> None:
        self._readFromDisk = readMethod

    def saveCheckpoint(self, activeTransactions: Optional[List[int]] = None) -> None:
        if self._routine is not None:
            self._routine() # ini isinya method flush ke disk

        # Flush WAL buffer to persistent storage
        self._writeAheadLog.flushBuffer()

        # Create checkpoint with active transaction list (provided by CCM)
        if activeTransactions is None:
            activeTransactions = []

        checkpoint = self._writeAheadLog.createCheckpoint(activeTransactions)

        # Update last checkpoint time
        self._lastCheckpointTime = datetime.now()

        # Truncate old log entries before checkpoint (log maintenance)
        self._writeAheadLog.truncateBeforeCheckpoint(checkpoint.getCheckpointId())

    # cc: @Concurrency-Control-Manager
    def recover(self, criteria: RecoveryCriteria) -> None:
        # Get logs backward from WAL for undo
        logs = self._writeAheadLog.getAllLogsBackward()

        # Filter logs matching recovery criteria and perform undo
        for log in logs:
            if criteria.getTimestamp() and log.getTimestamp() <= criteria.getTimestamp():
                break

            if criteria.getTransactionId() and log.getTransactionId() != criteria.getTransactionId():
                continue

            if log.getEntryType() == LogEntryType.UPDATE:
                self._undoLogEntry(log)

        # Abort log
        abort_log_id = self._writeAheadLog.getNextLogId()
        abort_entry = LogEntry(
            logId=abort_log_id,
            transactionId=criteria.getTransactionId() or 0,
            timestamp=datetime.now(),
            entryType=LogEntryType.ABORT
        )
        self._writeAheadLog.appendLog(abort_entry)
        self._writeAheadLog.flushBuffer()

        if self._routine is not None:
            self._routine() # flush ke disk

    def _undoLogEntry(self, log: LogEntry) -> None:
        data_item = log.getDataItem()
        old_value = log.getOldValue()

        if not data_item:
            return

        try:
            table, rest = data_item.split('.')
            column, row_id = rest.split('[')
            row_id = row_id.rstrip(']')
        except Exception as e:
            print(f"[UNDO ERROR] Failed to parse data_item '{data_item}': {e}")
            return

        table_data = self._buffer.get(table)

        if table_data is None:
            print(f"[UNDO] Table '{table}' not in buffer, reading from disk...")

            try:
                table_data = self._readTableFromDisk(table)

                if table_data is None or len(table_data) == 0:
                    print(f"[UNDO ERROR] Cannot read table '{table}' from disk or table is empty")
                    raise RuntimeError(
                        f"UNDO FAILED: Cannot read table '{table}' from disk. "
                        f"Recovery cannot proceed without data."
                    )
            except Exception as e:
                print(f"[UNDO ERROR] Exception while reading table '{table}': {e}")
                raise RuntimeError(
                    f"UNDO FAILED: Cannot read table '{table}' from disk: {e}"
                )

        # Find and update the row
        row_found = False
        for row in table_data:
            if str(row.get('id')) == str(row_id):
                print(f"[UNDO] Restoring {table}.{column}[{row_id}]: "
                      f"{row.get(column)} → {old_value}")
                row[column] = old_value
                row_found = True
                break

        if not row_found:
            print(f"[UNDO WARNING] Row {row_id} not found in table '{table}'")
            # This might be okay if the row was deleted, so don't raise exception

        # Mark as dirty so it gets written back to disk
        self._buffer.put(table, table_data, isDirty=True)

    def _readTableFromDisk(self, table_name: str) -> Optional[List[Dict[str, Any]]]:
        if self._readFromDisk is None:
            print(f"[UNDO ERROR] Cannot read '{table_name}' from disk: No read method set")
            print(f"[UNDO ERROR] Use frm.setReadMethod(storage_manager._readFullTableAsRows)")
            return None

        try:
            table_data = self._readFromDisk(table_name)
            print(f"[UNDO] Read {len(table_data) if table_data else 0} rows from '{table_name}' on disk")
            return table_data

        except Exception as e:
            print(f"[UNDO ERROR] Failed to read table '{table_name}' from disk: {e}")
            return None

    def tableFromBuffer(self, table_name: str) -> Optional[List[Dict[str, Any]]]:
        return self._buffer.get(table_name)

    def sendTableToBuffer(self, table_name: str, data: List[Dict[str, Any]], isDirty: bool = False) -> None:
        self._buffer.put(table_name, data, isDirty=isDirty)
        if self._buffer.isNearlyFull():
            self.saveCheckpoint()

    def _shouldCheckpoint(self) -> bool:
        time_elapsed = (datetime.now() - self._lastCheckpointTime).total_seconds()
        return time_elapsed >= self._checkpointInterval

    # Will look into here later
    def recoverFromSystemFailure(self) -> Dict[str, List[Dict[str, Any]]]:
        committed, aborted, active_txns, last_checkpoint = self._analysisPhase()
        redo_ops = self._redoPhase(last_checkpoint)
        undo_ops = self._undoPhase(active_txns)

        return {
            'redo_operations': redo_ops,
            'undo_operations': undo_ops,
            'committed_transactions': list(committed),
            'aborted_transactions': list(aborted),
            'active_transactions': list(active_txns)
        }

    def _analysisPhase(self) -> tuple:
        committed = set()
        aborted = set()
        active = set()

        # Scan logs from last checkpoint
        last_checkpoint = self._writeAheadLog.getLatestCheckpoint()
        if last_checkpoint:
            # Start with active transactions from checkpoint
            active = set(last_checkpoint.getActiveTransactions())
            logs_to_scan = self._writeAheadLog.getLogsSinceCheckpoint(last_checkpoint.getCheckpointId())
        else:
            # No checkpoint, scan all logs from beginning
            logs_to_scan = self._writeAheadLog.getAllLogsBackward()[::-1]

        for log in logs_to_scan:
            txn_id = log.getTransactionId()
            entry_type = log.getEntryType()

            if entry_type == LogEntryType.START:
                active.add(txn_id)
            elif entry_type == LogEntryType.COMMIT:
                if txn_id in active:
                    active.remove(txn_id)
                committed.add(txn_id)
            elif entry_type == LogEntryType.ABORT:
                if txn_id in active:
                    active.remove(txn_id)
                aborted.add(txn_id)

        return committed, aborted, active, last_checkpoint

    def _redoPhase(self, checkpoint: Optional[Checkpoint]) -> List[Dict[str, Any]]:
        redo_operations = []
        start_log_id = checkpoint.getLastLogId() if checkpoint else 0
        logs_to_redo = self._writeAheadLog.getAllLogsBackward()[::-1]  # Forward order

        for log in logs_to_redo:
            # Skip logs before checkpoint
            if log.getLogId() <= start_log_id:
                continue

            # Redo UPDATE and COMPENSATION logs
            if log.getEntryType() in (LogEntryType.UPDATE, LogEntryType.COMPENSATION):
                new_value = log.performRedo()

                if new_value is not None:
                    # Add redo operation for Storage Manager
                    redo_operations.append({
                        'data_item': log.getDataItem(),
                        'new_value': new_value,
                        'transaction_id': log.getTransactionId()
                    })

        return redo_operations

    def _undoPhase(self, activeTransactions: List[int]) -> List[Dict[str, Any]]:
        undo_operations = []
        logs_backward = self._writeAheadLog.getAllLogsBackward()

        for log in logs_backward:
            if log.getTransactionId() in activeTransactions:
                # Perform undo for UPDATE logs
                if log.getEntryType() == LogEntryType.UPDATE:
                    old_value = log.performUndo()
                    if old_value is not None:
                        # Create compensation log record (CLR)
                        clr_id = self._writeAheadLog.getNextLogId()
                        clr_entry = LogEntry(
                            logId=clr_id,
                            transactionId=log.getTransactionId(),
                            timestamp=datetime.now(),
                            entryType=LogEntryType.COMPENSATION,
                            dataItem=log.getDataItem(),
                            newValue=old_value
                        )

                        # Write CLR to WAL
                        self._writeAheadLog.appendLog(clr_entry)

                        # Add undo operation for Storage Manager
                        undo_operations.append({
                            'data_item': log.getDataItem(),
                            'old_value': old_value,
                            'transaction_id': log.getTransactionId()
                        })

        # Write ABORT log for each incomplete transaction
        for txn_id in activeTransactions:
            abort_log_id = self._writeAheadLog.getNextLogId()
            abort_entry = LogEntry(
                logId=abort_log_id,
                transactionId=txn_id,
                timestamp=datetime.now(),
                entryType=LogEntryType.ABORT
            )
            self._writeAheadLog.appendLog(abort_entry)

        return undo_operations


def getFailureRecoveryManager() -> FailureRecoveryManager:
    return FailureRecoveryManager()