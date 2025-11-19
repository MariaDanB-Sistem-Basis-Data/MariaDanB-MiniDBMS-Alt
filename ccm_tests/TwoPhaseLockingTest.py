from TwoPhaseLocking import TwoPhaseLocking
from ccm_model.TransactionManager import TransactionManager
from ccm_model.Enums import Action, TransactionStatus
from ccm_helper.Row import Row

def run_two_phase_locking_tests():
    print("Running TwoPhaseLocking tests...")

    tpl = TwoPhaseLocking()
    tpl.transaction_manager = TransactionManager()  

    # Buat transaksi
    tpl.transaction_manager.begin_transaction(1)
    tpl.transaction_manager.begin_transaction(2)

    t1 = tpl.transaction_manager.get_transaction(1)
    t2 = tpl.transaction_manager.get_transaction(2)

    rowA = Row("A")
    rowA.resource_key = "A"  

    # T1 read A (shared lock)
    resp1 = tpl.validate_object(rowA, 1, Action.READ)
    assert resp1.success is True

    # T2 read A (shared lock)
    resp2 = tpl.validate_object(rowA, 2, Action.READ)
    assert resp2.success is True

    # T2 tries write A → must abort
    resp3 = tpl.validate_object(rowA, 2, Action.WRITE)
    assert resp3.success is False
    assert t2.status == TransactionStatus.ABORTED

    # T1 write A → should succeed after T2 aborted
    resp4 = tpl.validate_object(rowA, 1, Action.WRITE)
    assert resp4.success is True

    # End transaction T1
    tpl.lock_manager.release_locks(1)

    print("TwoPhaseLocking Test passed.\n")
