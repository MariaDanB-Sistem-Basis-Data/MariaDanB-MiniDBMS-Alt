from typing import Dict, Optional, List, Any
from datetime import datetime
import warnings
import time
import os
import threading

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
        bufferSize: int = 134217728,
        checkpointIntervalSeconds: int = 300,
        flush_callback: Optional[callable] = None,
        read_callback: Optional[callable] = None
    ):
        if not hasattr(self, 'initialized'):
            self._writeAheadLog = WriteAheadLog(logFilePath)
            self._buffer: Buffer[Any] = Buffer(
                maxSize=bufferSize,
                emergencyFlushCallback=lambda: self._emergencyFlushBufferToDisk()
            )

            self._checkpointInterval = checkpointIntervalSeconds
            self._lastCheckpointTime = datetime.now()

            self._recoveryLock = threading.RLock()

            self._transactionLocks: Dict[int, threading.RLock] = {}
            self._transactionLocksLock = threading.RLock()

            self._routine = flush_callback
            self._readFromDisk = read_callback

            if self._routine is None or self._readFromDisk is None:
                if not os.environ.get('FRM_TEST_MODE'):
                    warnings.warn(
                        "FailureRecoveryManager initialized without Storage Manager callbacks. "
                        "Recovery operations may fail. Set callbacks using configure_storage_manager().",
                        RuntimeWarning
                    )

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

        query = info.query

        entry_type = LogEntryType.UPDATE
        if "BEGIN" in query.upper():
            entry_type = LogEntryType.START
        elif "COMMIT" in query.upper():
            entry_type = LogEntryType.COMMIT
        elif "ABORT" in query.upper() or "ROLLBACK" in query.upper():
            entry_type = LogEntryType.ABORT

        if entry_type != LogEntryType.UPDATE:
            next_log_id = self._writeAheadLog.getNextLogId()
            log_entry = LogEntry(
                logId=next_log_id,
                transactionId=info.transaction_id,
                timestamp=datetime.now(),
                entryType=entry_type
            )
            self._writeAheadLog.appendLog(log_entry)

            if self._writeAheadLog.needsFlush() or self._shouldCheckpoint():
                self.saveCheckpoint()
            return

        data = info.data
        update_details_list = []

        if isinstance(data, Rows):
            if data.data and len(data.data) > 0:
                update_details_list = data.data
        elif isinstance(data, int):
            return

        if not update_details_list:
            return

        for update_details in update_details_list:
            required_fields = ['table', 'column', 'id', 'old_value', 'new_value']
            missing_fields = [field for field in required_fields if field not in update_details]

            if missing_fields:
                error_msg = (
                    f"CRITICAL: UPDATE log missing required fields: {missing_fields}. "
                    f"Query Processor MUST provide all fields for recovery. "
                    f"Query: {query}. "
                    f"Update details: {update_details}"
                )
                print(f"[FRM CRITICAL ERROR] {error_msg}")
                raise ValueError(error_msg)

            table = update_details.get('table', '')
            col = update_details.get('column', '')
            row_id = update_details.get('id', '')

            if not table or not col or row_id is None:
                error_msg = (
                    f"CRITICAL: Invalid update details - "
                    f"table='{table}', column='{col}', id='{row_id}'. "
                    f"All must be non-empty!"
                )
                print(f"[FRM CRITICAL ERROR] {error_msg}")
                raise ValueError(error_msg)

            data_item = f"{table}.{col}[{row_id}]"

            try:
                test_table, test_rest = data_item.split('.')
                test_col, test_id = test_rest.split('[')
                test_id.rstrip(']')
            except Exception as e:
                error_msg = (
                    f"CRITICAL: Generated invalid data_item format: '{data_item}'. "
                    f"Parse error: {e}"
                )
                print(f"[FRM CRITICAL ERROR] {error_msg}")
                raise ValueError(error_msg)

            old_val = update_details.get('old_value')
            new_val = update_details.get('new_value')

            if old_val is None or new_val is None:
                error_msg = (
                    f"CRITICAL: old_value or new_value is None! "
                    f"old_value={old_val}, new_value={new_val}"
                )
                print(f"[FRM CRITICAL ERROR] {error_msg}")
                raise ValueError(error_msg)

            print(f"[FRM] Valid UPDATE log: {data_item} ({old_val} -> {new_val})")

            next_log_id = self._writeAheadLog.getNextLogId()
            log_entry = LogEntry(
                logId=next_log_id,
                transactionId=info.transaction_id,
                timestamp=datetime.now(),
                entryType=entry_type,
                dataItem=data_item,
                oldValue=old_val,
                newValue=new_val
            )

            self._writeAheadLog.appendLog(log_entry)

        if self._writeAheadLog.needsFlush() or self._shouldCheckpoint():
            self.saveCheckpoint()

    def setRoutine(self, routine: callable) -> None:
        self._routine = routine

    def setReadMethod(self, readMethod: callable) -> None:
        self._readFromDisk = readMethod

    def configure_storage_manager(self, flush_callback: callable, read_callback: callable) -> None:
        self._routine = flush_callback
        self._readFromDisk = read_callback
        print("[FRM] Storage Manager callbacks configured successfully")

    def _emergencyFlushBufferToDisk(self) -> None:
        if self._routine is not None:
            print("[FRM] Emergency flush: flushing dirty buffer to disk...")
            self._routine()
            print("[FRM] Emergency flush completed")
        else:
            raise RuntimeError(
                "Emergency flush failed: Storage Manager callback not configured. "
                "Call configure_storage_manager() first."
            )

    def _getTransactionLock(self, transaction_id: int) -> threading.RLock:
        with self._transactionLocksLock:
            if transaction_id not in self._transactionLocks:
                self._transactionLocks[transaction_id] = threading.RLock()
            return self._transactionLocks[transaction_id]

    def _releaseTransactionLock(self, transaction_id: int) -> None:
        with self._transactionLocksLock:
            if transaction_id in self._transactionLocks:
                del self._transactionLocks[transaction_id]
                print(f"[FRM] Released lock for T{transaction_id}")

    def get_dirty_buffer_entries(self) -> List[Dict[str, Any]]:
        return [
            {'key': entry.getKey(), 'data': entry.getData(), 'is_dirty': entry.isDirty()}
            for entry in self._buffer.getDirtyEntries()
        ]

    def put_buffer_entry(self, key: str, data: Any, is_dirty: bool = False) -> None:
        self._buffer.put(key, data, isDirty=is_dirty)

    def get_buffer_entry(self, key: str) -> Optional[Any]:
        return self._buffer.get(key)

    def flush_logs_to_disk(self) -> None:
        self._writeAheadLog.flushBuffer()

    def is_configured(self) -> bool:
        return self._routine is not None and self._readFromDisk is not None


    def saveCheckpoint(self, activeTransactions: Optional[List[int]] = None):
        try:
            print(f"[FRM] Starting checkpoint...")

            # Step 1: Flush WAL buffer FIRST (WAL protocol: log before data!)
            print(f"[FRM] Flushing WAL buffer (write-ahead log protocol)...")
            self._writeAheadLog.flushBuffer()
            print(f"[FRM] WAL flush completed")

            # Step 2: Flush data buffer to disk (ensure all changes are persisted)
            if self._routine is not None:
                print(f"[FRM] Flushing dirty buffer to disk...")
                try:
                    self._routine()  # Storage Manager's flush method
                    print(f"[FRM] Buffer flush completed")
                except Exception as e:
                    print(f"[FRM ERROR] Buffer flush failed: {e}")
                    print(f"[FRM] Cannot checkpoint safely - data may not be on disk")
                    return None
            else:
                print(f"[FRM WARNING] No flush routine set, checkpoint may be unsafe")

            # Step 3: Create checkpoint with active transaction list (provided by CCM)
            if activeTransactions is None:
                activeTransactions = []

            print(f"[FRM] Creating checkpoint with active transactions: {activeTransactions}")
            checkpoint = self._writeAheadLog.createCheckpoint(activeTransactions)

            # Step 4: Update last checkpoint time
            self._lastCheckpointTime = datetime.now()

            # Step 5: Truncate old log entries before checkpoint (log maintenance)
            print(f"[FRM] Truncating logs before checkpoint {checkpoint.getCheckpointId()}")
            try:
                self._writeAheadLog.truncateBeforeCheckpoint(checkpoint.getCheckpointId())
            except RuntimeError as e:
                print(f"[FRM WARNING] Checkpoint successful but truncate failed: {e}")
                print(f"[FRM WARNING] Old logs not deleted - manual cleanup may be needed")
                return checkpoint

            print(f"[FRM] Checkpoint {checkpoint.getCheckpointId()} completed successfully")
            return checkpoint

        except Exception as e:
            print(f"[FRM ERROR] Checkpoint failed: {e}")
            return None

    # cc: @Concurrency-Control-Manager
    def abort(self, transaction_id: int) -> bool:
        try:
            print(f"[FRM] Starting ARIES-compliant abort for transaction {transaction_id}")

            logs = self._writeAheadLog.getAllLogsBackward()
            for log in logs:
                if log.getTransactionId() == transaction_id:
                    if log.getEntryType() == LogEntryType.COMMIT:
                        print(f"[FRM ERROR] Cannot abort T{transaction_id}: already committed")
                        return False
                    if log.getEntryType() == LogEntryType.END:
                        print(f"[FRM ERROR] Cannot abort T{transaction_id}: already terminated")
                        return False
                    if log.getEntryType() == LogEntryType.START:
                        # Found START, transaction is valid for abort
                        break

            criteria = RecoveryCriteria(transactionId=transaction_id, timestamp=None)
            self.recover(criteria)

            print(f"[FRM] Transaction {transaction_id} aborted successfully via ARIES protocol")
            return True

        except Exception as e:
            print(f"[FRM ERROR] Failed to abort transaction {transaction_id}: {e}")
            import traceback
            traceback.print_exc()
            return False

    # cc: @Concurrency-Control-Manager / @Deadlock-Detector
    def recover(self, criteria: RecoveryCriteria) -> None:
        transaction_id = criteria.getTransactionId()

        # Acquire transaction-specific lock if transaction ID is specified
        if transaction_id:
            txn_lock = self._getTransactionLock(transaction_id)
        else:
            # Use global recovery lock if no specific transaction
            txn_lock = self._recoveryLock

        try:
            with txn_lock:
                logs = self._writeAheadLog.getAllLogsBackward()

                if transaction_id:
                    transaction_found = False
                    for log in logs:
                        if log.getTransactionId() == transaction_id:
                            transaction_found = True
                            if log.getEntryType() == LogEntryType.COMMIT:
                                error_msg = f"Cannot abort transaction {transaction_id}: already committed (DATA INTEGRITY VIOLATION)"
                                print(f"[FRM CRITICAL ERROR] {error_msg}")
                                raise RuntimeError(error_msg)
                            if log.getEntryType() == LogEntryType.END:
                                print(f"[FRM WARNING] Cannot abort transaction {transaction_id}: already terminated")
                                return
                            if log.getEntryType() == LogEntryType.ABORT:
                                print(f"[FRM WARNING] Transaction {transaction_id} already has ABORT log, continuing recovery...")
                                break
                            if log.getEntryType() == LogEntryType.START:
                                break

                    if not transaction_found:
                        print(f"[FRM WARNING] No logs found for transaction {transaction_id}")
                        return

                # Step 1: Write ABORT log (ARIES standard)
                print(f"[FRM] Writing ABORT log for transaction {transaction_id}")
                abort_log_id = self._writeAheadLog.getNextLogId()
                abort_entry = LogEntry(
                    logId=abort_log_id,
                    transactionId=transaction_id or 0,
                    timestamp=datetime.now(),
                    entryType=LogEntryType.ABORT
                )
                self._writeAheadLog.appendLog(abort_entry)

                # Step 2: Undo operations, writing CLR for each (ARIES standard)
                print(f"[FRM] Undoing operations and writing CLRs...")
                updates_to_undo = []
                for log in logs:
                    if criteria.getTimestamp() and log.getTimestamp() <= criteria.getTimestamp():
                        break
                    if transaction_id and log.getTransactionId() != transaction_id:
                        continue
                    if log.getEntryType() == LogEntryType.UPDATE:
                        updates_to_undo.append(log)
                    if log.getEntryType() == LogEntryType.START:
                        break

                # If no updates to undo, just write ABORT and END
                if not updates_to_undo:
                    print(f"[FRM] No updates to undo for T{transaction_id}")

                # Undo each update and write CLR
                for i, log in enumerate(updates_to_undo):
                    # Perform the undo
                    self._undoLogEntryWithRetry(log)

                    # Write CLR (Compensation Log Record)
                    clr_log_id = self._writeAheadLog.getNextLogId()

                    # undoNextLSN points to the log BEFORE this one
                    undo_next_lsn = updates_to_undo[i+1].getLogId() if i+1 < len(updates_to_undo) else None

                    clr_entry = LogEntry(
                        logId=clr_log_id,
                        transactionId=transaction_id or 0,
                        timestamp=datetime.now(),
                        entryType=LogEntryType.COMPENSATION,
                        dataItem=log.getDataItem(),
                        oldValue=log.getOldValue(),
                        newValue=log.getOldValue(),  # CLR redoes the undo (puts back old value)
                        undoNextLSN=undo_next_lsn
                    )
                    self._writeAheadLog.appendLog(clr_entry)
                    print(f"[FRM] CLR {clr_log_id}: Compensating UPDATE {log.getLogId()}, undoNext={undo_next_lsn}")

                # Batch flush CLRs (they don't need immediate flush like ABORT/END)
                self._writeAheadLog.flushBuffer()

                # Step 3: Write END log (ARIES standard)
                print(f"[FRM] Writing END log for transaction {transaction_id}")
                end_log_id = self._writeAheadLog.getNextLogId()
                end_entry = LogEntry(
                    logId=end_log_id,
                    transactionId=transaction_id or 0,
                    timestamp=datetime.now(),
                    entryType=LogEntryType.END
                )
                self._writeAheadLog.appendLog(end_entry)
                # FIX #13: Remove redundant flush (appendLog auto-flushes END)

                # Flush buffer to disk
                if self._routine is not None:
                    self._routine()

                print(f"[FRM] Transaction {transaction_id} abort completed with {len(updates_to_undo)} CLRs")
        finally:
            # FIX #9: Cleanup transaction lock after recovery completes
            if transaction_id:
                self._releaseTransactionLock(transaction_id)

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
            raise RuntimeError(
                f"UNDO FAILED: Invalid data_item format '{data_item}'. "
                f"Expected format: 'table.column[row_id]'. Error: {e}"
            )

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
                current_value = row.get(column)
                expected_new_value = log.getNewValue()

                if current_value != expected_new_value:
                    print(
                        f"[UNDO WARNING] Value mismatch for {table}.{column}[{row_id}]: "
                        f"expected {expected_new_value}, found {current_value}"
                    )

                print(f"[UNDO] Restoring {table}.{column}[{row_id}]: "
                      f"{current_value} -> {old_value}")
                row[column] = old_value

                row['_lsn'] = log.getLogId()
                row_found = True
                break

        if not row_found:
            print(f"[UNDO WARNING] Row {row_id} not found in table '{table}'")

        # FIX #7: Mark as dirty with error handling for buffer full
        try:
            self._buffer.put(table, table_data, isDirty=True)
        except RuntimeError as e:
            if "Buffer full" in str(e):
                print(f"[UNDO] Buffer full during undo, forcing emergency flush...")
                if self._routine is not None:
                    self._routine()  # Flush dirty entries
                    print(f"[UNDO] Emergency flush completed, retrying put...")

                self._buffer.put(table, table_data, isDirty=True)
            else:
                raise

    def _undoLogEntryWithRetry(self, log: LogEntry, max_retries: int = 3) -> None:
        for attempt in range(max_retries):
            try:
                self._undoLogEntry(log)
                return  # Success
            except RuntimeError as e:
                if attempt == max_retries - 1:
                    print(f"[UNDO ERROR] Failed after {max_retries} attempts: {e}")
                    raise
                else:
                    backoff = 0.1 * (2 ** attempt)  # 0.1s, 0.2s, 0.4s, ...
                    print(f"[UNDO] Attempt {attempt + 1} failed, retrying in {backoff}s...")
                    time.sleep(backoff)
            except Exception as e:
                print(f"[UNDO ERROR] Non-retryable error: {e}")
                raise

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

    def recoverFromSystemFailure(self) -> Dict[str, List[Dict[str, Any]]]:
        # will be called in start of DBMS :D, well, just for a note
        with self._recoveryLock:
            print(f"[ARIES] ========== SYSTEM RECOVERY STARTED ==========")

            # Phase 1: Analysis
            print(f"[ARIES] Phase 1: Analysis")
            committed, aborted_complete, losers, committed_no_end, last_checkpoint = self._analysisPhase()

            # Phase 2: Redo (repeat history)
            print(f"[ARIES] Phase 2: REDO (Repeating History)")
            redo_ops = self._redoPhase(last_checkpoint)

            # Phase 3: Undo (rollback losers with CLRs)
            print(f"[ARIES] Phase 3: UNDO (Rolling Back Losers)")
            undo_ops = self._undoPhase(losers)

            # Phase 4: Write END for committed transactions
            if committed_no_end:
                print(f"[ARIES] Phase 4: Writing END logs for {len(committed_no_end)} committed transactions")
                for txn_id in committed_no_end:
                    end_log_id = self._writeAheadLog.getNextLogId()
                    end_entry = LogEntry(
                        logId=end_log_id,
                        transactionId=txn_id,
                        timestamp=datetime.now(),
                        entryType=LogEntryType.END
                    )
                    self._writeAheadLog.appendLog(end_entry)
                    print(f"[ARIES] Wrote END log for committed T{txn_id}")

            # Flush WAL to disk (write-ahead log protocol)
            print(f"[ARIES] Flushing WAL to disk...")
            self._writeAheadLog.flushBuffer()

            # Flush buffer to disk to persist recovered state
            if self._routine is not None:
                print(f"[ARIES] Flushing buffer to disk...")
                self._routine()
                print(f"[ARIES] Buffer flush completed")

            print(f"[ARIES] ========== SYSTEM RECOVERY COMPLETED ==========")
            print(f"[ARIES] Summary:")
            print(f"[ARIES]   - Winners (committed): {list(committed)}")
            print(f"[ARIES]   - Committed without END (fixed): {list(committed_no_end)}")
            print(f"[ARIES]   - Completed aborts: {list(aborted_complete)}")
            print(f"[ARIES]   - Losers (rolled back): {list(losers)}")
            print(f"[ARIES]   - REDO operations: {len(redo_ops)}")
            print(f"[ARIES]   - UNDO operations: {len(undo_ops)}")

            return {
                'redo_operations': redo_ops,
                'undo_operations': undo_ops,
                'committed_transactions': list(committed),
                'committed_no_end': list(committed_no_end),
                'aborted_transactions': list(aborted_complete),
                'loser_transactions': list(losers)
            }

    def _analysisPhase(self) -> tuple:
        committed = set()
        committed_with_end = set()  # Track which committed txns have END
        aborted_incomplete = set()  # ABORT without END (losers)
        aborted_complete = set()    # ABORT with END (not losers)
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
                # Mark as aborted, but still a loser until END seen
                aborted_incomplete.add(txn_id)
            elif entry_type == LogEntryType.END:
                # Transaction terminated (either after COMMIT or ABORT)
                if txn_id in active:
                    active.remove(txn_id)

                # FIX #4: Track which committed transactions have END
                if txn_id in committed:
                    committed_with_end.add(txn_id)

                # If was in aborted_incomplete, move to aborted_complete
                if txn_id in aborted_incomplete:
                    aborted_incomplete.remove(txn_id)
                    aborted_complete.add(txn_id)

        # ARIES: Losers = active transactions + incomplete aborts
        losers = active | aborted_incomplete

        # Find committed transactions without END
        committed_no_end = committed - committed_with_end

        print(f"[ARIES Analysis] Winners (committed): {committed}")
        print(f"[ARIES Analysis] Committed with END: {committed_with_end}")
        print(f"[ARIES Analysis] Committed WITHOUT END (need END log): {committed_no_end}")
        print(f"[ARIES Analysis] Losers (need undo): {losers}")
        print(f"[ARIES Analysis] - Active (no commit/abort): {active}")
        print(f"[ARIES Analysis] - Incomplete abort (no END): {aborted_incomplete}")
        print(f"[ARIES Analysis] Completed aborts (with END): {aborted_complete}")

        return committed, aborted_complete, losers, committed_no_end, last_checkpoint

    def _redoPhase(self, checkpoint: Optional[Checkpoint]) -> List[Dict[str, Any]]:
        redo_operations = []
        start_log_id = checkpoint.getLastLogId() if checkpoint else 0
        logs_to_redo = self._writeAheadLog.getAllLogsBackward()[::-1]  # Forward order

        print(f"[ARIES REDO] Starting REDO phase from log ID {start_log_id}")

        for log in logs_to_redo:
            # Skip logs before checkpoint
            if log.getLogId() <= start_log_id:
                continue

            # ARIES: Redo UPDATE and CLR (COMPENSATION) logs
            if log.getEntryType() in (LogEntryType.UPDATE, LogEntryType.COMPENSATION):
                new_value = log.performRedo()

                if new_value is not None:
                    # Collect operation info
                    redo_operations.append({
                        'data_item': log.getDataItem(),
                        'new_value': new_value,
                        'transaction_id': log.getTransactionId()
                    })

                    self._applyRedoOperation(log, new_value)

        print(f"[ARIES REDO] REDO phase completed: {len(redo_operations)} operations applied")
        return redo_operations

    def _applyRedoOperation(self, log: LogEntry, new_value: Any) -> None:
        data_item = log.getDataItem()

        if not data_item:
            return

        try:
            # Parse data_item: "table.column[row_id]"
            table, rest = data_item.split('.')
            column, row_id = rest.split('[')
            row_id = row_id.rstrip(']')
        except Exception as e:
            print(f"[REDO ERROR] Failed to parse data_item '{data_item}': {e}")
            return

        # Get table data from buffer or disk
        table_data = self._buffer.get(table)

        if table_data is None:
            print(f"[REDO] Table '{table}' not in buffer, reading from disk...")
            try:
                table_data = self._readTableFromDisk(table)
                if table_data is None or len(table_data) == 0:
                    print(f"[REDO WARNING] Cannot read table '{table}' from disk")
                    return
            except Exception as e:
                print(f"[REDO ERROR] Exception while reading table '{table}': {e}")
                return

        # Find and update the row
        row_found = False
        for row in table_data:
            if str(row.get('id')) == str(row_id):
                # LSN-based idempotency check (ARIES algorithm)
                row_lsn = row.get('_lsn', 0)  # Default to 0 if no LSN
                log_lsn = log.getLogId()

                if row_lsn >= log_lsn:
                    # Row already has this update or a later one
                    print(f"[REDO] Skipping {table}.{column}[{row_id}]: "
                          f"row_lsn={row_lsn} >= log_lsn={log_lsn} (already applied)")
                    row_found = True
                    break

                # Apply redo operation
                print(f"[REDO] Applying {table}.{column}[{row_id}]: "
                      f"{row.get(column)} -> {new_value} (LSN: {row_lsn} -> {log_lsn})")
                row[column] = new_value

                # Update LSN to ensure idempotency
                row['_lsn'] = log_lsn
                row_found = True
                break

        if not row_found:
            print(f"[REDO WARNING] Row {row_id} not found in table '{table}'")

        # Mark as dirty so it gets written back to disk
        self._buffer.put(table, table_data, isDirty=True)

    def _undoPhase(self, loserTransactions: List[int]) -> List[Dict[str, Any]]:
        undo_operations = []
        logs_backward = self._writeAheadLog.getAllLogsBackward()

        print(f"[ARIES UNDO] Starting UNDO phase for losers: {loserTransactions}")

        # For each loser transaction
        for txn_id in loserTransactions:
            print(f"[ARIES UNDO] Processing loser transaction {txn_id}")

            # Find the last log for this transaction
            last_lsn = None
            for log in logs_backward:
                if log.getTransactionId() == txn_id:
                    last_lsn = log.getLogId()
                    break

            if last_lsn is None:
                print(f"[ARIES UNDO] No logs found for transaction {txn_id}")
                continue

            # Track if transaction already has ABORT record
            has_abort = any(
                log.getTransactionId() == txn_id and log.getEntryType() == LogEntryType.ABORT
                for log in logs_backward
            )

            # Undo operations backward, following CLR chain
            current_lsn = last_lsn
            updates_to_undo = []

            while current_lsn is not None:
                # Find log with current_lsn
                current_log = None
                for log in logs_backward:
                    if log.getLogId() == current_lsn and log.getTransactionId() == txn_id:
                        current_log = log
                        break

                if current_log is None:
                    break

                entry_type = current_log.getEntryType()

                if entry_type == LogEntryType.UPDATE:
                    # Need to undo this update
                    updates_to_undo.append(current_log)
                    # Move to previous log (scan backward)
                    current_lsn = self._getPrevLSN(logs_backward, txn_id, current_lsn)

                elif entry_type == LogEntryType.COMPENSATION:
                    # CLR: skip to undoNextLSN (already undone)
                    undo_next = current_log.getUndoNextLSN()
                    print(f"[ARIES UNDO] CLR {current_lsn}: Skipping to undoNext={undo_next}")
                    current_lsn = undo_next

                elif entry_type in (LogEntryType.START, LogEntryType.ABORT):
                    break

                else:
                    # Other log types, move backward
                    current_lsn = self._getPrevLSN(logs_backward, txn_id, current_lsn)

            # Perform undos and write CLRs
            print(f"[ARIES UNDO] Transaction {txn_id}: {len(updates_to_undo)} updates to undo")

            for i, log in enumerate(updates_to_undo):
                # Perform the undo
                self._undoLogEntryWithRetry(log)

                undo_operations.append({
                    'data_item': log.getDataItem(),
                    'old_value': log.getOldValue(),
                    'transaction_id': txn_id
                })

                # Write CLR
                clr_log_id = self._writeAheadLog.getNextLogId()
                undo_next_lsn = updates_to_undo[i+1].getLogId() if i+1 < len(updates_to_undo) else None

                clr_entry = LogEntry(
                    logId=clr_log_id,
                    transactionId=txn_id,
                    timestamp=datetime.now(),
                    entryType=LogEntryType.COMPENSATION,
                    dataItem=log.getDataItem(),
                    oldValue=log.getOldValue(),
                    newValue=log.getOldValue(),  # CLR redoes the undo
                    undoNextLSN=undo_next_lsn
                )
                self._writeAheadLog.appendLog(clr_entry)
                print(f"[ARIES UNDO] CLR {clr_log_id}: Compensating UPDATE {log.getLogId()}, undoNext={undo_next_lsn}")

            if not has_abort:
                print(f"[ARIES UNDO] Writing ABORT for transaction {txn_id}")
                abort_log_id = self._writeAheadLog.getNextLogId()
                abort_entry = LogEntry(
                    logId=abort_log_id,
                    transactionId=txn_id,
                    timestamp=datetime.now(),
                    entryType=LogEntryType.ABORT
                )
                self._writeAheadLog.appendLog(abort_entry)

            # Write END
            print(f"[ARIES UNDO] Writing END for transaction {txn_id}")
            end_log_id = self._writeAheadLog.getNextLogId()
            end_entry = LogEntry(
                logId=end_log_id,
                transactionId=txn_id,
                timestamp=datetime.now(),
                entryType=LogEntryType.END
            )
            self._writeAheadLog.appendLog(end_entry)

        self._writeAheadLog.flushBuffer()
        print(f"[ARIES UNDO] UNDO phase completed: {len(undo_operations)} operations applied")
        return undo_operations

    def _getPrevLSN(self, logs_backward: List[LogEntry], txn_id: int, current_lsn: int) -> Optional[int]:
        found_current = False
        for log in logs_backward:
            if log.getTransactionId() == txn_id:
                if found_current:
                    return log.getLogId()
                if log.getLogId() == current_lsn:
                    found_current = True
        return None


# idk if we need this or nah?
def getFailureRecoveryManager() -> FailureRecoveryManager:
    return FailureRecoveryManager()