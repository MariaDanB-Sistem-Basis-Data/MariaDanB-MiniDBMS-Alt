from dataclasses import dataclass

from datetime import datetime
from ccm_model.Enums import TransactionStatus
from ccm_model.Transaction import Transaction

@dataclass
class TransactionManager:
    transactions: dict[int, 'Transaction'] = None
    initialized: bool = False
    next_tid: int = 1
    
    def __post_init__(self):
        if self.transactions is None:
            self.transactions = {}
        self.initialized = True
    
    def clear(self) -> None:
        self.transactions.clear()

    def begin_transaction(self) -> int:
        tid = self.next_tid
        self.transactions[tid] = Transaction(
            transaction_id=tid , 
            status=TransactionStatus.ACTIVE,
            start_time=datetime.now()
        )
        self.next_tid += 1
        return tid
    
    def get_transaction(self, transaction_id: int) -> Transaction:
        if self.has_transaction(transaction_id):
            return self.transactions[transaction_id]

    def has_transaction(self, transaction_id: int) -> bool:
        return transaction_id in self.transactions
    
    def commit_transaction(self, transaction_id: int) -> bool:
        if (self.has_transaction(transaction_id)):
            transaction =  self.get_transaction(transaction_id)
            transaction.status = TransactionStatus.COMMITTED
            return True

    def abort_transaction(self, transaction_id: int) -> bool:
        if (self.has_transaction(transaction_id)):
            transaction =  self.get_transaction(transaction_id)
            if not transaction.can_be_aborted():
                return False
            transaction.status = TransactionStatus.ABORTED
            print(f"Transaction {transaction_id} is {transaction.status.name}.")
            return True
    
    def terminate_transaction(self, transaction_id: int) -> bool:
        if (self.has_transaction(transaction_id)):
            transaction =  self.get_transaction(transaction_id)
            transaction.status = TransactionStatus.TERMINATED
            print(f"Transaction {transaction_id} is {transaction.status.name}.")
            return True

    def remove_transaction(self, transaction_id: int) -> bool:
        if self.has_transaction(transaction_id):
            del self.transactions[transaction_id]
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

    
    