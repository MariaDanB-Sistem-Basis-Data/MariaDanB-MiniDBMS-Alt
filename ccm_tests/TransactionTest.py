from datetime import datetime
from ccm_model.Transaction import Transaction
from ccm_model.Enums import TransactionStatus
from ccm_model.TransactionManager import TransactionManager

def run_transaction_tests():
    t1 = Transaction(1, TransactionStatus.ACTIVE)
    assert t1.get_transaction_id() == 1
    assert t1.is_active() is True
    assert t1.is_committed() is False
    assert t1.is_aborted() is False

    t1.status = TransactionStatus.COMMITTED
    assert t1.is_committed() is True
    assert t1.is_active() is False
    assert t1.is_aborted() is False

    t2 = Transaction(2, TransactionStatus.ACTIVE)
    t2.status = TransactionStatus.ABORTED
    assert t2.is_aborted() is True
    assert t2.is_active() is False

    t3 = Transaction(3, TransactionStatus.ACTIVE)
    assert t3.can_be_aborted() is True

    t4 = Transaction(4, TransactionStatus.PARTIALLY_COMMITTED)
    assert t4.can_be_aborted() is True

    t5 = Transaction(5, TransactionStatus.COMMITTED)
    assert t5.can_be_aborted() is False

    print("Transaction Test passed.")

def run_transaction_manager_tests():
    tm = TransactionManager()

    # Begin transaction
    tm.begin_transaction(1)
    assert tm.has_transaction(1)
    assert tm.get_transaction(1).status == TransactionStatus.ACTIVE

    # Commit transaction
    tm.begin_transaction(2)
    assert tm.commit_transaction(2) is True
    assert tm.get_transaction(2).status == TransactionStatus.COMMITTED

    # Abort transaction (success)
    tm.begin_transaction(3)
    assert tm.abort_transaction(3) is True
    assert tm.get_transaction(3).status == TransactionStatus.ABORTED

    # Abort transaction (fail - committed)
    tm.begin_transaction(4)
    tm.commit_transaction(4)
    assert tm.abort_transaction(4) is False

    # Remove transaction
    tm.begin_transaction(5)
    assert tm.remove_transaction(5) is True
    assert tm.has_transaction(5) is False

    # Active transaction list
    tm.clear()
    tm.begin_transaction(6)
    tm.begin_transaction(7)
    tm.commit_transaction(7)
    active_list = tm.get_active_transactions()
    print(active_list)
    assert len(active_list) == 1
    assert active_list[0].transaction_id == 6

    # Active transaction IDs
    active_ids = tm.get_active_transaction_ids()
    assert active_ids == [6]

    # Clear completed transactions
    tm.begin_transaction(8)
    tm.commit_transaction(8)
    tm.clearCompletedTransactions()
    assert tm.get_transaction(8) is None if hasattr(tm, "get_transaction") else True

    # Statistics
    tm.clear()
    tm.begin_transaction(10)
    tm.begin_transaction(11)
    tm.commit_transaction(11)
    stats = tm.getStatistics()

    assert stats["ACTIVE"] == 1
    assert stats["COMMITTED"] == 1

    print("Transaction Manager Test passed.")
