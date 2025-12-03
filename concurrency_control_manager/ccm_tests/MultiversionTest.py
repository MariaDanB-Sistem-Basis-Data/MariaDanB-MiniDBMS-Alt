"""
Basic test for Multiversion Timestamp Ordering implementation
Tests the core MVTO protocol with read and write operations
"""

from ccm_methods.Multiversion import Multiversion
from ccm_model.TransactionManager import TransactionManager
from ccm_model.Enums import Action
from ccm_helper.Row import Row


def test_mvto_basic():
    tm = TransactionManager()
    mvto = Multiversion()
    mvto.set_transaction_manager(tm)
    
    t1 = tm.begin_transaction()
    print(f"\n[TEST] Created Transaction T{t1}")
    
    row_a = Row(
        table_name="accounts",
        pk_value="A001",
        data={"balance": 1000},
        version=[1]
    )
    
    # T1 writes to row A
    result = mvto.validate_object(row_a, t1, Action.WRITE)
    assert result.success, "T1 write should succeed"
    
    # Create T2
    t2 = tm.begin_transaction()
    
    # T2 reads row A (should get T1's version)
    result = mvto.validate_object(row_a, t2, Action.READ)
    assert result.success, "T2 read should succeed"
    
    mvto.end_transaction(t1)
    mvto.end_transaction(t2)
    
    print("\nTEST 1 PASSED\n")


def test_mvto_conflict():    
    tm = TransactionManager()
    mvto = Multiversion()
    mvto.set_transaction_manager(tm)
    
    t1 = tm.begin_transaction()
    t2 = tm.begin_transaction()
    print(f"\n[TEST] Created T{t1} and T{t2}")
    
    row_b = Row(
        table_name="accounts",
        pk_value="B001",
        data={"balance": 2000},
        version=[1]
    )
    
    result = mvto.validate_object(row_b, t1, Action.READ)
    result = mvto.validate_object(row_b, t2, Action.READ)

    assert result.success, "T2 read should succeed"
    
    result = mvto.validate_object(row_b, t1, Action.WRITE)
    
    # This should fail because T1's timestamp < T2's read timestamp
    if not result.success:
        transaction = tm.get_transaction(t1)
    else:
        assert False, "T1 write should fail due to timestamp conflict"
    
    print("\nTEST 2 PASSED\n")


def test_mvto_multiple_versions():
    tm = TransactionManager()
    mvto = Multiversion()
    mvto.set_transaction_manager(tm)
    
    t1 = tm.begin_transaction()
    t2 = tm.begin_transaction()
    t3 = tm.begin_transaction()
    print(f"\n[TEST] Created T{t1}, T{t2}, T{t3}")
    
    row_c = Row(
        table_name="accounts",
        pk_value="C001",
        data={"balance": 3000},
        version=[1]
    )
    
    # T1 writes version 1
    result = mvto.validate_object(row_c, t1, Action.WRITE)
    assert result.success
    
    # T2 writes version 2
    row_c_v2 = Row(
        table_name="accounts",
        pk_value="C001",
        data={"balance": 3500},
        version=[2]
    )
    result = mvto.validate_object(row_c_v2, t2, Action.WRITE)
    assert result.success
    
    row_c_v3 = Row(
        table_name="accounts",
        pk_value="C001",
        data={"balance": 4000},
        version=[3]
    )
    result = mvto.validate_object(row_c_v3, t3, Action.WRITE)
    assert result.success
    
    versions = mvto.get_versions(row_c.resource_key)
    for i, v in enumerate(versions):
        print(f"  Version {i}: W-TS={v.write_timestamp}, R-TS={v.read_timestamp}")
    
    print("\nTEST 3 PASSED\n")

def testMultiVersion():
    test_mvto_basic()
    test_mvto_conflict()
    test_mvto_multiple_versions()