from ccm_tests.BeginTransactionTest import run_begin_transaction_tests
from ccm_tests.LockManagerTest import run_lock_manager_tests
from ccm_tests.LogObjectTest import run_log_object_tests
from ccm_tests.TransactionTest import run_transaction_tests, run_transaction_manager_tests
from ccm_tests.ConcurrencyControlTest import run_concurrency_control_manager_tests
from ccm_tests.TwoPhaseLockingTest import run_two_phase_locking_tests
from ccm_tests.ValidatorTest import run_validator_tests

if __name__ == '__main__':
	# run_lock_manager_tests()
	# run_log_object_tests()
	# run_begin_transaction_tests()
	run_transaction_tests()
	run_transaction_manager_tests()
	run_two_phase_locking_tests()
	run_validator_tests()
	run_concurrency_control_manager_tests()