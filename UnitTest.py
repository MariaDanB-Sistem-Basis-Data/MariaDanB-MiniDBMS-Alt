from ConcurrencyControlManager import ConcurrencyControlManager


def test_begin_transaction():
    manager = ConcurrencyControlManager()
    tid = manager.begin_transaction()

    assert tid in manager.transaction_table
    print(f"begin_transaction returned {tid}")


if __name__ == "__main__":
    test_begin_transaction()
