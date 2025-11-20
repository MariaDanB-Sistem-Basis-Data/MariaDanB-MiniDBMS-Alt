from ConcurrencyControlManager import ConcurrencyControlManager
from ccm_methods.TwoPhaseLocking import TwoPhaseLocking
from ccm_model.Enums import Action
from ccm_helper.Row import Row

def run_concurrency_control_manager_tests():
    print("Running ConcurrencyControlManager tests...")

    tpl = TwoPhaseLocking()
    ccm = ConcurrencyControlManager()
    ccm.set_method(tpl)

    # Mulai transaksi
    tid = ccm.begin_transaction(1)
    assert ccm.transaction_manager.has_transaction(tid)

    # Log object
    row = Row(table_name="A", pk_value=1, data={"x": 10}, version=[0])

    ccm.log_object(row, tid)

    # Validasi 
    resp = ccm.validate_object(row, tid, Action.READ)
    assert resp.success is True

    # Commit transaksi
    resp = ccm.commit_transaction(tid)
    assert resp.success is True

    print("ConcurrencyControlManager Test passed.\n")
