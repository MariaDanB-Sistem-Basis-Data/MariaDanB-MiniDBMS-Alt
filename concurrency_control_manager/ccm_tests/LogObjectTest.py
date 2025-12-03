# from ConcurrencyControlManager import log_object
from ccm_helper.Operation import Operation
from ccm_model.LockManager import LockManager

def run_log_object_tests():
    lm = LockManager()
    lm.request_lock(Operation(1, 'R', 'A'))
    lm.request_lock(Operation(2, 'W', 'B'))
    log = lm.log_object()
    assert isinstance(log, dict)
    print('Lock Manager log_object Test passed.')