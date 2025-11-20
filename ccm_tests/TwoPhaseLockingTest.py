from ccm_methods.TwoPhaseLocking import TwoPhaseLocking
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

    rowA = Row(
        table_name="A", 
        pk_value=1, 
        data={"x": 10}, 
        version=[0]
    )

    # T1 read A (shared lock)
    resp1 = tpl.validate_object(rowA, 1, Action.READ)
    assert resp1.success is True

    # T2 read A (shared lock)
    resp2 = tpl.validate_object(rowA, 2, Action.READ)
    assert resp2.success is True

    # T2 tries write A → wait
    resp3 = tpl.validate_object(rowA, 2, Action.WRITE)
    assert resp3.success is False
    assert t2.status == TransactionStatus.ACTIVE # harusnnya nunggu gaksih dia, jangan abort

    # T1 write A → wound T2
    resp4 = tpl.validate_object(rowA, 1, Action.WRITE)
    assert resp4.success is False

    # End transaction T1
    tpl.lock_manager.release_locks(1)

    print("TwoPhaseLocking Test passed.\n")
