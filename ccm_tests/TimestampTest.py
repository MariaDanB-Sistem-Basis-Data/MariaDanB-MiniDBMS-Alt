from ccm_model.Timestamp import TimestampManager, Timestamp
from ccm_helper.Operation import Operation


def run_timestamp_tests():
    tm = TimestampManager()

    op1 = Operation(1, 'read', 'X')
    result = tm.validate_operation(op1, tx_ts=10)
    assert result.success is True
    assert tm.timestampTable['X'].read_ts == 10
    assert tm.timestampTable['X'].write_ts == 0
    
    op2 = Operation(2, 'write', 'X')
    result = tm.validate_operation(op2, tx_ts=20)
    assert result.success is True
    assert tm.timestampTable['X'].write_ts == 20
    assert tm.timestampTable['X'].read_ts == 10

    op3 = Operation(3, 'read', 'X')
    result = tm.validate_operation(op3, tx_ts=15)
    assert result.success is False
    assert "Read conflict" in result.message

    tm2 = TimestampManager()
    op4 = Operation(4, 'read', 'Y')
    tm2.validate_operation(op4, tx_ts=30)
    
    op5 = Operation(5, 'write', 'Y')
    result = tm2.validate_operation(op5, tx_ts=25)
    assert result.success is False
    assert "Write conflict" in result.message
    assert "read by a newer transaction" in result.message

    tm3 = TimestampManager()
    op6 = Operation(6, 'write', 'Z')
    tm3.validate_operation(op6, tx_ts=40)
    
    op7 = Operation(7, 'write', 'Z')
    result = tm3.validate_operation(op7, tx_ts=35)
    assert result.success is False
    assert "Write conflict" in result.message
    assert "written by a newer transaction" in result.message

    tm4 = TimestampManager()
    op8 = Operation(8, 'r', 'A')
    result = tm4.validate_operation(op8, tx_ts=50)
    assert result.success is True
    
    op9 = Operation(9, 'w', 'A')
    result = tm4.validate_operation(op9, tx_ts=60)
    assert result.success is True

    tm5 = TimestampManager()
    op10 = Operation(10, 'read', 'B')
    tm5.validate_operation(op10, tx_ts=100)
    
    op11 = Operation(11, 'read', 'B')
    result = tm5.validate_operation(op11, tx_ts=150)
    assert result.success is True
    assert tm5.timestampTable['B'].read_ts == 150 

    tm6 = TimestampManager()
    op12 = Operation(12, 'write', 'C')
    result = tm6.validate_operation(op12, tx_ts=200)
    assert result.success is True
    
    op13 = Operation(13, 'write', 'C')
    result = tm6.validate_operation(op13, tx_ts=300)
    assert result.success is True
    assert tm6.timestampTable['C'].write_ts == 300

    tm7 = TimestampManager()
    timestamp = tm7.create_timestamp('NEW_RESOURCE')
    assert timestamp.read_ts == 0
    assert timestamp.write_ts == 0
    assert 'NEW_RESOURCE' in tm7.timestampTable
    print('Timestamp Manager Test passed.')

if __name__ == '__main__':
    run_timestamp_tests()
