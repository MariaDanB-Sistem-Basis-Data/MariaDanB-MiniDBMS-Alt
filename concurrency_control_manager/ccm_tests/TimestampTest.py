from datetime import datetime, timedelta
from ccm_model.Timestamp import TimestampManager, Timestamp
from ccm_helper.Operation import Operation


def run_timestamp_tests():
    tm = TimestampManager()

    # Create datetime timestamps
    base_time = datetime(2025, 1, 1, 12, 0, 0)
    t10 = base_time + timedelta(seconds=10)
    t15 = base_time + timedelta(seconds=15)
    t20 = base_time + timedelta(seconds=20)
    t25 = base_time + timedelta(seconds=25)
    t30 = base_time + timedelta(seconds=30)
    t35 = base_time + timedelta(seconds=35)
    t40 = base_time + timedelta(seconds=40)
    t50 = base_time + timedelta(seconds=50)
    t60 = base_time + timedelta(seconds=60)
    t100 = base_time + timedelta(seconds=100)
    t150 = base_time + timedelta(seconds=150)
    t200 = base_time + timedelta(seconds=200)
    t300 = base_time + timedelta(seconds=300)

    # Test 1: Read operation dengan timestamp baru (harus berhasil)
    op1 = Operation(1, 'read', 'X')
    result = tm.validate_operation(op1, tx_ts=t10)
    assert result.success is True
    assert tm.timestampTable['X'].read_ts == t10
    assert tm.timestampTable['X'].write_ts is None
    
    # Test 2: Write operation dengan timestamp lebih baru (harus berhasil)
    op2 = Operation(2, 'write', 'X')
    result = tm.validate_operation(op2, tx_ts=t20)
    assert result.success is True
    assert tm.timestampTable['X'].write_ts == t20
    assert tm.timestampTable['X'].read_ts == t10

    # Test 3: Read dengan timestamp lama setelah write baru (harus ABORT)
    op3 = Operation(3, 'read', 'X')
    result = tm.validate_operation(op3, tx_ts=t15)
    assert result.success is False
    assert "Read conflict" in result.message

    # Test 4: Write dengan timestamp < read_ts (harus ABORT)
    tm2 = TimestampManager()
    op4 = Operation(4, 'read', 'Y')
    tm2.validate_operation(op4, tx_ts=t30)
    
    op5 = Operation(5, 'write', 'Y')
    result = tm2.validate_operation(op5, tx_ts=t25)
    assert result.success is False
    assert "Write conflict" in result.message
    assert "read by a newer transaction" in result.message

    # Test 5: Write dengan timestamp < write_ts (harus ABORT)
    tm3 = TimestampManager()
    op6 = Operation(6, 'write', 'Z')
    tm3.validate_operation(op6, tx_ts=t40)
    
    op7 = Operation(7, 'write', 'Z')
    result = tm3.validate_operation(op7, tx_ts=t35)
    assert result.success is False
    assert "Write conflict" in result.message
    assert "written by a newer transaction" in result.message

    # Test 6: Operasi dengan format huruf kecil 'r' dan 'w'
    tm4 = TimestampManager()
    op8 = Operation(8, 'r', 'A')
    result = tm4.validate_operation(op8, tx_ts=t50)
    assert result.success is True
    
    op9 = Operation(9, 'w', 'A')
    result = tm4.validate_operation(op9, tx_ts=t60)
    assert result.success is True

    # Test 7: Multiple reads dengan timestamp berbeda
    tm5 = TimestampManager()
    op10 = Operation(10, 'read', 'B')
    tm5.validate_operation(op10, tx_ts=t100)
    
    op11 = Operation(11, 'read', 'B')
    result = tm5.validate_operation(op11, tx_ts=t150)
    assert result.success is True
    assert tm5.timestampTable['B'].read_ts == t150  # Updated ke timestamp tertinggi

    # Test 8: Sequential write operations dengan timestamp ascending
    tm6 = TimestampManager()
    op12 = Operation(12, 'write', 'C')
    result = tm6.validate_operation(op12, tx_ts=t200)
    assert result.success is True
    
    op13 = Operation(13, 'write', 'C')
    result = tm6.validate_operation(op13, tx_ts=t300)
    assert result.success is True
    assert tm6.timestampTable['C'].write_ts == t300

    # Test 9: Create timestamp test
    tm7 = TimestampManager()
    timestamp = tm7.create_timestamp('NEW_RESOURCE')
    assert timestamp.read_ts is None
    assert timestamp.write_ts is None
    assert 'NEW_RESOURCE' in tm7.timestampTable

    print('Timestamp Manager Test passed.')

if __name__ == '__main__':
    run_timestamp_tests()
