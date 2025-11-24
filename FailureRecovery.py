from typing import Dict, Optional, List, Any
from datetime import datetime
import sys
from pathlib import Path

query_processor_path = Path(__file__).parent.parent / "Query-Processor"
if str(query_processor_path) not in sys.path:
    sys.path.insert(0, str(query_processor_path))

from query_processor.model.ExecutionResult import ExecutionResult
from query_processor.model.Rows import Rows

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
            self.initialized = True

    def writeLog(self, info: ExecutionResult) -> None:
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

        if isinstance(data, int):
            pass
        elif isinstance(data, Rows):
            if data.data and len(data.data) > 0:
                update_details = data.data[0]

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

    def saveCheckpoint(self, activeTransactions: Optional[List[int]] = None) -> None:
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

    def recover(self, criteria: RecoveryCriteria) -> List[Dict[str, Any]]:
        undo_operations = []

        # Get logs backward from WAL for undo
        logs = self._writeAheadLog.getAllLogsBackward()

        # Filter logs matching recovery criteria and perform undo
        for log in logs:
            if criteria.getTimestamp() and log.getTimestamp() < criteria.getTimestamp():
                break

            if criteria.getTransactionId() and log.getTransactionId() != criteria.getTransactionId():
                continue

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

                    # Add undo operation for Storage Manager to execute
                    undo_operations.append({
                        'data_item': log.getDataItem(),
                        'old_value': old_value,
                        'transaction_id': log.getTransactionId()
                    })

        return undo_operations

    def _shouldCheckpoint(self) -> bool:
        # check if checkpoint interval has elapsed
        time_elapsed = (datetime.now() - self._lastCheckpointTime).total_seconds()
        return time_elapsed >= self._checkpointInterval

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