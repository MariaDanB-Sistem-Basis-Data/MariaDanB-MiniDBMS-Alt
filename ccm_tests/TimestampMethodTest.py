from ccm_methods.TimestampMethod import TimestampMethod
from ccm_model.TransactionManager import TransactionManager
from ccm_model.Enums import Action, TransactionStatus
from ccm_helper.Row import Row


def run_timestamp_method_tests():
    tm = TimestampMethod()
    txn_manager = TransactionManager()
    tm.set_transaction_manager(txn_manager)
    
    t1 = txn_manager.begin_transaction()
    t2 = txn_manager.begin_transaction()
    t3 = txn_manager.begin_transaction()    
    print(f"Created T{t1}, T{t2}, T{t3}")
    
    row_x = Row(table_name='Users', pk_value='X', data={'name': 'Alice'}, version=[1])
    response = tm.validate_object(row_x, t1, Action.READ)
    assert response.success is True, "T1 read pada X harus berhasil"
    
    row_x2 = Row(table_name='Users', pk_value='X', data={'name': 'Bob'}, version=[2])
    response = tm.validate_object(row_x2, t2, Action.WRITE)
    assert response.success is True, "T2 write pada X harus berhasil (timestamp lebih baru)"
    
    row_x3 = Row(table_name='Users', pk_value='X', data={'name': 'Charlie'}, version=[3])
    response = tm.validate_object(row_x3, t1, Action.READ)
    assert response.success is False, "T1 read setelah T2 write harus gagal"
    
    t1_status = txn_manager.get_transaction(t1).status
    assert t1_status == TransactionStatus.ABORTED, "T1 harus di-abort"
    
    row_y = Row(table_name='Users', pk_value='Y', data={'name': 'David'}, version=[1])
    tm.validate_object(row_y, t2, Action.WRITE)
    tm.log_object(row_y, t2)
    
    t2_transaction = txn_manager.get_transaction(t2)
    assert 'Users:Y' in t2_transaction.write_set, "Y harus ada di write set T2"
    
    response = tm.end_transaction(t2)
    assert response.success is True, "End transaction harus berhasil"
    
    t4 = txn_manager.begin_transaction()
    t5 = txn_manager.begin_transaction()    
    row_z = Row(table_name='Products', pk_value='Z', data={'price': 100}, version=[1])
    response = tm.validate_object(row_z, t5, Action.WRITE)
    assert response.success is True, "T5 write pertama harus berhasil"
    
    row_z2 = Row(table_name='Products', pk_value='Z', data={'price': 200}, version=[2])
    response = tm.validate_object(row_z2, t4, Action.WRITE)
    assert response.success is False, "T4 write dengan timestamp lama harus gagal"
    
    t6 = txn_manager.begin_transaction()
    t7 = txn_manager.begin_transaction()
    row_a = Row(table_name='Orders', pk_value='A', data={'status': 'pending'}, version=[1])
    response = tm.validate_object(row_a, t7, Action.READ)
    assert response.success is True, "T7 read harus berhasil"
    
    row_a2 = Row(table_name='Orders', pk_value='A', data={'status': 'completed'}, version=[2])
    response = tm.validate_object(row_a2, t6, Action.WRITE)
    assert response.success is False, "T6 write dengan timestamp < read_ts harus gagal"
    
    print('TimestampMethod Test passed.')


if __name__ == '__main__':
    run_timestamp_method_tests()
