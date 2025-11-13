from typing import List
from ..frm_model import LogEntry, LogEntryType, RecoveryCriteria


class RecoveryExecutor:
    def __init__(self):
        pass

    def executeUndo(self, logEntry: LogEntry) -> None:
        #TODO: Execute undo operation for the given log entry
        pass

    def executeRedo(self, logEntry: LogEntry) -> None:
        #TODO: Execute redo operation for the given log entry
        pass

    def performTransactionRollback(self, logs: List[LogEntry]) -> None:
        #TODO: Perform rollback for all operations in the transaction
        pass

    def matchesCriteria(self, logEntry: LogEntry, criteria: RecoveryCriteria) -> bool:
        return criteria.matchesEntry(
            entryTimestamp=logEntry.getTimestamp(),
            entryTransactionId=logEntry.getTransactionId()
        )
