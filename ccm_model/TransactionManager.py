from dataclasses import dataclass

from datetime import datetime
from ccm_model.Enums import TransactionStatus
from ccm_model.Transaction import Transaction

@dataclass
class TransactionManager:
    transactions: dict[int, 'Transaction'] = None
    initialized: bool = False
    
    def __post_init__(self):
        if self.transactions is None:
            self.transactions = {}
        self.initialized = True
    
    def clear(self) -> None:
        self.transactions.clear()

    def begin_transaction(self, tid) -> int:
        self.transactions[tid] = Transaction(
            transaction_id=tid , 
            status=TransactionStatus.ACTIVE,
            start_time=datetime.now()
        )
        return tid
    
    def get_transaction(self, transactionId: int) -> Transaction:
        if transactionId in self.transactions:
            return self.transactions[transactionId]

    def has_transaction(self, transactionId: int) -> bool:
        return transactionId in self.transactions

    def commit_transaction(self, transactionId: int) -> bool:
        if (self.has_transaction(transactionId)):
            transaction =  self.get_transaction(transactionId)
            transaction.status = TransactionStatus.COMMITTED
            return True

    def abort_transaction(self, transactionId: int) -> bool:
        if (self.has_transaction(transactionId)):
            transaction =  self.get_transaction(transactionId)
            if not transaction.can_be_aborted():
                return False
            transaction.status = TransactionStatus.ABORTED
            print(f"Transaction {transactionId} is {transaction.status.name}.")
            return True
    
    def terminate_transaction(self, transactionId: int) -> bool:
        if (self.has_transaction(transactionId)):
            transaction =  self.get_transaction(transactionId)
            transaction.status = TransactionStatus.TERMINATED
            print(f"Transaction {transactionId} is {transaction.status.name}.")
            return True

    def remove_transaction(self, transactionId: int) -> bool:
        if self.has_transaction(transactionId):
            del self.transactions[transactionId]
            return True
    
    def get_active_transactions(self) -> list[Transaction]:
        active_transactions = []
        for transaction in self.transactions.values():
            if transaction.is_active():
                active_transactions.append(transaction)
        return active_transactions
    
    def get_active_transaction_ids(self) -> list[int]:
        active_transactions_id = []
        for transaction in self.transactions.values():
            if transaction.is_active():
                active_transactions_id.append(transaction.get_transaction_id())
        return active_transactions_id

    def clearCompletedTransactions(self) -> int:
        id_to_remove = []
        for transaction in self.transactions.values():
            if transaction.is_committed() or transaction.is_aborted():
                id_to_remove.append(transaction.get_transaction_id())
        for transaction_id in id_to_remove:
            self.remove_transaction(transaction_id)

    def getTransactionCount(self) -> int:
        return len(self.transactions)

    def getActiveTransactionCount(self) -> int:
        active_transactions = self.get_active_transactions()
        return len(active_transactions)

    def getAllTransactions(self) -> dict[int, Transaction]:
        return dict(self.transactions)

    def getStatistics(self) -> dict[str, int]:
        stats: dict[str, int] = {state.name: 0 for state in TransactionStatus}
        for t in self.transactions.values():
            state_name = t.get_state().name
            stats[state_name] = stats.get(state_name, 0) + 1
        return stats

    
    