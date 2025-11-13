from model.LockManager import LockManager
from helper.Operation import Operation


def run_lock_manager_tests():
	lm = LockManager()

	# Shared lock: T1, T2 read resource A
	assert lm.request_lock(Operation('T1', 'R', 'A')) is True
	assert lm.request_lock(Operation('T2', 'read', 'A')) is True
	status = lm.resource_status('A')
	assert status['lockMode'] == 'S'
	assert status['lockedBy'] == {'T1', 'T2'}

	# Exclusive lock: T3 write A (fail)
	assert lm.request_lock(Operation('T3', 'W', 'A')) is False

	# Release T1 and T2
	lm.release_locks('T1')
	lm.release_locks('T2')
	# Exclusive lock: T3 write A (true)
	assert lm.request_lock(Operation('T3', 'W', 'A')) is True
	status = lm.resource_status('A')
	assert status['lockMode'] == 'X' and status['lockedBy'] == {'T3'}

	# ulangi lock
	assert lm.request_lock(Operation('T3', 'W', 'A')) is True

	# Exclusive lock: T4 read A (fail), T4 write A (fail)
	assert lm.request_lock(Operation('T4', 'R', 'A')) is False
	assert lm.request_lock(Operation('T4', 'W', 'A')) is False

	# Release exclusive
	lm.release_locks('T3')
	# T5 Read B
	assert lm.request_lock(Operation('T5', 'R', 'B')) is True
	# T5 Write B
	assert lm.request_lock(Operation('T5', 'W', 'B')) is True
	status = lm.resource_status('B')
	assert status['lockMode'] == 'X' and status['lockedBy'] == {'T5'}

	# Release Exclusive
	lm.release_locks('T5')
	# Multiple shared locks di C
	assert lm.request_lock(Operation('T6', 'R', 'C')) is True
	assert lm.request_lock(Operation('T7', 'R', 'C')) is True
	# T6 Write C (fail)
	assert lm.request_lock(Operation('T6', 'W', 'C')) is False

	print('Lock Manager Test passed.')