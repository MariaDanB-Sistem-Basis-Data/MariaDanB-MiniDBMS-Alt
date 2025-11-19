from typing import Dict, Optional, List, Any
from datetime import datetime

from frm_model import ExecutionResult, RecoveryCriteria, LogEntry, LogEntryType, Checkpoint
from frm_helper import singleton, WriteAheadLog, Buffer, TransactionManager, RecoveryExecutor


@singleton
class FailureRecoveryManager:

    def __init__(
        self,
        logFilePath: str = "logs/wal.json",
        bufferSize: int = 100,
        checkpointIntervalSeconds: int = 300
    ):
        if not hasattr(self, 'initialized'):
            self._writeAheadLog = WriteAheadLog(logFilePath)
            self._buffer: Buffer[Any] = Buffer(maxSize=bufferSize) # harusnya WAL pake kelas buffer sekalian no?
            self._transactionManager = TransactionManager()
            self._recoveryExecutor = RecoveryExecutor()
            self._checkpointInterval = checkpointIntervalSeconds
            self._lastCheckpointTime = datetime.now()
            self.initialized = True

    def writeLog(self, info: ExecutionResult) -> None:
        #TODO: Extract data according to format
        #TODO: Update transaction last access timestamp using TransactionManager (CC Manager?)
        next_log_id = self._writeAheadLog.getNextLogId()
        
        query = info.getQuery()

        # determine log entry type
        entry_type = LogEntryType.UPDATE
        if "BEGIN" in query.upper():   # bcs di spek "begin transaction"
            entry_type = LogEntryType.START
        elif "COMMIT" in query.upper():
            entry_type = LogEntryType.COMMIT
        elif "ABORT" in query.upper():
            entry_type = LogEntryType.ABORT

        # extract data
        data_item = None
        old_val = None
        new_val = None

        data = info.getData()
        update_details: Dict[str, Any] = {}

        if isinstance(data, int):
            pass
        elif isinstance(data, ExecutionResult.Rows):
            rows_data = data.getData()
            # asumsi 1 query results in 1 row?
            if rows_data and len(rows_data) > 0:
                update_details = rows_data[0]

        # expected format (NTAR SESUAIN):
        # {'table': 'A', 'column': 'x', 'id': 1, 'old_val': 10, 'new_val': 20}
        if entry_type == LogEntryType.UPDATE and update_details:
            table = update_details.get('table', '')
            col = update_details.get('column', '')
            row_id = update_details.get('id', '')
            
            data_item = f"{table}.{col}:{row_id}"
            old_val = update_details.get('old_val')
            new_val = update_details.get('new_val')

        log_entry = LogEntry(
            logId = next_log_id,
            transactionId = info.transaction_id,
            timestamp = datetime.now(),
            entryType = entry_type,
            dataItem = data_item,
            oldValue = old_val,
            newValue = new_val
        )

        # write to WAL
        self._writeAheadLog.appendLog(log_entry)

        # update transaction last access time
        txn = self._transactionManager.getTransaction(info.getTransactionId())
        if txn:
            txn.updateLastAccessTimestamp(log_entry.timestamp)
        
        # check trigger for checkpoint (buffer nearly full or time interval exceeded)
        if self._writeAheadLog.needsFlush() or self._shouldCheckpoint():
            self.saveCheckpoint()

    def saveCheckpoint(self) -> None:
        # get dirty entries from buffer n flush to storage
        self._writeAheadLog.flushBuffer()

        # create checkpoint with active transactions
        active_txns = self._transactionManager.getActiveTransactionIds()
        checkpoint = self._writeAheadLog.createCheckpoint(active_txns)

        # update last checkpoint time
        self._lastCheckpointTime = datetime.now()

        # truncate old log entries before checkpoint
        self._writeAheadLog.truncateBeforeCheckpoint(checkpoint.getCheckpointId())

    def recover(self, criteria: RecoveryCriteria) -> None:
        #TODO: write old value to storage (StorageManager?)

        # get logs backward from WAL
        logs = self._writeAheadLog.getAllLogsBackward()

        # filter logs matching recovery criteria
        for log in logs:
            if criteria.getTimestamp() and log.timestamp < criteria.getTimestamp():
                break

            if criteria.getTransactionId() and log.transactionId != criteria.getTransactionId():
                continue

            # perform undo for UPDATE logs
            if log.entryType == LogEntryType.UPDATE:
                val_restored = log.performUndo()
                if val_restored is not None:
                    clr_id = self._writeAheadLog.getNextLogId()
                    clr_entry = LogEntry(
                        logId = clr_id,
                        transactionId = log.getTransactionId(),
                        timestamp = datetime.now(),
                        entryType = LogEntryType.COMPENSATION,
                        dataItem = log.getDataItem(),
                        newValue = val_restored,
                        undoLogId = log.logId
                    )
                    
                    # write compensation log record (clr)
                    self._writeAheadLog.appendLog(clr_entry)

                    # write old value to storage (not done)

    def _shouldCheckpoint(self) -> bool:
        # check if checkpoint interval has elapsed
        time_elapsed = (datetime.now() - self._lastCheckpointTime).total_seconds()
        return time_elapsed >= self._checkpointInterval

    def recoverFromSystemFailure(self) -> None:
        # ARIES-style recovery after system crash (analysis, redo, undo)
        _, _, active_txns, last_checkpoint = self._analysisPhase()
        self._redoPhase(last_checkpoint)
        self._undoPhase(active_txns)

    def _analysisPhase(self) -> tuple: 
        committed = set()
        aborted = set()
        active = set()

        # scan logs from last checkpoint
        last_checkpoint = self._writeAheadLog.getLatestCheckpoint()
        if last_checkpoint:
            active = set(last_checkpoint.getActiveTransactions())
            logs_to_scan = self._writeAheadLog.getLogsSinceCheckpoint(last_checkpoint.getCheckpointId())
        else: # no checkpoint, scan all logs
            logs_to_scan = self._writeAheadLog.getAllLogsBackward()[::-1] # reverse to get forward order
        
        for log in logs_to_scan:
            txn_id = log.getTransactionId()
            entry_type = log.getEntryType()
            
            if entry_type == LogEntryType.START:
                active.add(txn_id)
            elif entry_type == LogEntryType.COMMIT:
                # remove committed txn from active n add to committed
                if txn_id in active:
                    active.remove(txn_id)
                committed.add(txn_id)
            elif entry_type == LogEntryType.ABORT:
                # remove aborted txn from active n add to aborted
                if txn_id in active:
                    active.remove(txn_id)
                aborted.add(txn_id)

        return committed, aborted, active, last_checkpoint

    def _redoPhase(self, checkpoint: Optional[Checkpoint]) -> None:
        #TODO: write new value to storage (StorageManager?)
        start_log_id = checkpoint.getLastLogId() if checkpoint else 0
        logs_to_redo = self._writeAheadLog.getAllLogsBackward()[::-1] # forward order
        
        for log in logs_to_redo:
            # skip logs before checkpoint
            if log.getLogId() <= start_log_id:
                continue

            # redo UPDATE and COMPENSATION logs
            if log.getEntryType() in (LogEntryType.UPDATE, LogEntryType.COMPENSATION):
                val_applied = log.performRedo()
                
                if val_applied is not None:
                    # write new value to storage (not done)
                    pass

    def _undoPhase(self, activeTransactions: List[int]) -> None:
        #TODO: write old value to storage (StorageManager?)

        logs_backward = self._writeAheadLog.getAllLogsBackward()

        for log in logs_backward:
            if log.getTransactionId() in activeTransactions:
                # perform undo for UPDATE logs
                if log.getEntryType() == LogEntryType.UPDATE:
                    val_restored = log.performUndo()
                    if val_restored is not None:
                        clr_id = self._writeAheadLog.getNextLogId()
                        clr_entry = LogEntry(
                            logId = clr_id,
                            transactionId = log.getTransactionId(),
                            timestamp = datetime.now(),
                            entryType = LogEntryType.COMPENSATION,
                            dataItem = log.getDataItem(),
                            newValue = val_restored
                        )

                        # write compensation log record (clr)
                        self._writeAheadLog.appendLog(clr_entry)

                        # write old value to storage (not done)

        for txn_id in activeTransactions:
            # write ABORT log for each active transactions undone
            abort_log_id = self._writeAheadLog.getNextLogId()
            abort_entry = LogEntry(
                logId = abort_log_id,
                transactionId = txn_id,
                timestamp = datetime.now(),
                entryType = LogEntryType.ABORT
            )
            self._writeAheadLog.appendLog(abort_entry)


def getFailureRecoveryManager() -> FailureRecoveryManager:
    return FailureRecoveryManager()

if __name__ == "__main__":
    frm = getFailureRecoveryManager()
    print("FailureRecoveryManager initialized")