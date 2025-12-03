from ConcurrencyControlManager import ConcurrencyControlManager


def run_begin_transaction_tests():
    manager = ConcurrencyControlManager()
    tid = manager.begin_transaction()

    assert tid in manager.transaction_table, "Transaction ID should be stored in table."
    print(f"[BeginTransactionTest] begin_transaction returned {tid}")
