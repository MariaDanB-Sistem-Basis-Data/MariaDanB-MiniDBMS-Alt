from TwoPhaseLocking import TwoPhaseLocking
from ccm_model.TransactionManager import TransactionManager
from ccm_model.Enums import Action, TransactionStatus
from ccm_helper.Row import Row


def _make_row(name: str):
    return Row(table_name=name, pk_value=1, data={"x": 10}, version=[0])


def test_shared_read_and_conflicting_write():
    tpl = TwoPhaseLocking()
    tpl.transaction_manager = TransactionManager()

    tpl.transaction_manager.begin_transaction(1)
    tpl.transaction_manager.begin_transaction(2)

    t2 = tpl.transaction_manager.get_transaction(2)
    rowA = _make_row("A")

    resp1 = tpl.validate_object(rowA, 1, Action.READ)
    assert resp1.success is True

    resp2 = tpl.validate_object(rowA, 2, Action.READ)
    assert resp2.success is True

    resp3 = tpl.validate_object(rowA, 2, Action.WRITE)
    assert resp3.success is False
    assert t2.status == TransactionStatus.ACTIVE

    resp4 = tpl.validate_object(rowA, 1, Action.READ)
    assert resp4.success is True


def test_wait_condition_with_three_transactions():
    tpl = TwoPhaseLocking()
    tpl.transaction_manager = TransactionManager()

    tpl.transaction_manager.begin_transaction(1)
    tpl.transaction_manager.begin_transaction(2)
    tpl.transaction_manager.begin_transaction(3)

    rowA = _make_row("A")
    rowB = _make_row("B")

    tpl.validate_object(rowA, 1, Action.WRITE)
    tpl.validate_object(rowB, 2, Action.WRITE)

    r_wait = tpl.validate_object(rowA, 3, Action.WRITE)
    assert r_wait.success is False

    t3 = tpl.transaction_manager.get_transaction(3)
    assert t3.status == TransactionStatus.ACTIVE


def test_deadlock_detection_two_transactions():
    tpl = TwoPhaseLocking()
    tpl.transaction_manager = TransactionManager()

    tpl.transaction_manager.begin_transaction(1)
    tpl.transaction_manager.begin_transaction(2)

    t1 = tpl.transaction_manager.get_transaction(1)
    t2 = tpl.transaction_manager.get_transaction(2)

    rowA = _make_row("A")
    rowB = _make_row("B")

    tpl.validate_object(rowA, 1, Action.WRITE)
    tpl.validate_object(rowB, 2, Action.WRITE)

    tpl.validate_object(rowB, 1, Action.WRITE)
    tpl.validate_object(rowA, 2, Action.WRITE)

    statuses = set()
    if tpl.transaction_manager.has_transaction(1):
        statuses.add(t1.status)
    if tpl.transaction_manager.has_transaction(2):
        statuses.add(t2.status)

    assert TransactionStatus.ABORTED in statuses


def test_deadlock_detection_three_transactions():
    tpl = TwoPhaseLocking()
    tpl.transaction_manager = TransactionManager()

    tpl.transaction_manager.begin_transaction(1)
    tpl.transaction_manager.begin_transaction(2)
    tpl.transaction_manager.begin_transaction(3)

    t1 = tpl.transaction_manager.get_transaction(1)
    t2 = tpl.transaction_manager.get_transaction(2)
    t3 = tpl.transaction_manager.get_transaction(3)

    rowA = _make_row("A")
    rowB = _make_row("B")
    rowC = _make_row("C")

    tpl.validate_object(rowA, 1, Action.WRITE)
    tpl.validate_object(rowB, 2, Action.WRITE)
    tpl.validate_object(rowC, 3, Action.WRITE)

    tpl.validate_object(rowB, 1, Action.WRITE)
    tpl.validate_object(rowC, 2, Action.WRITE)
    tpl.validate_object(rowA, 3, Action.WRITE)

    statuses = [
        t1.status if tpl.transaction_manager.has_transaction(1) else None,
        t2.status if tpl.transaction_manager.has_transaction(2) else None,
        t3.status if tpl.transaction_manager.has_transaction(3) else None,
    ]

    assert TransactionStatus.ABORTED in statuses


def test_end_transaction_releases_locks():
    tpl = TwoPhaseLocking()
    tpl.transaction_manager = TransactionManager()

    tpl.transaction_manager.begin_transaction(1)
    rowA = _make_row("A")

    tpl.validate_object(rowA, 1, Action.WRITE)
    end_resp = tpl.end_transaction(1)
    assert end_resp.success is True

    for res in tpl.lock_manager.resources.values():
        assert 1 not in res.lockedBy


def run_validator_tests():
    print("Running TwoPhaseLocking tests...")

    test_shared_read_and_conflicting_write()
    print("  [OK] shared read + conflicting write")

    test_wait_condition_with_three_transactions()
    print("  [OK] wait condition with 3 transactions")

    test_deadlock_detection_two_transactions()
    print("  [OK] deadlock detection (2 transactions)")

    test_deadlock_detection_three_transactions()
    print("  [OK] deadlock detection (3 transactions cycle)")

    test_end_transaction_releases_locks()
    print("  [OK] end_transaction releases locks")

    print("All TwoPhaseLocking tests passed.")