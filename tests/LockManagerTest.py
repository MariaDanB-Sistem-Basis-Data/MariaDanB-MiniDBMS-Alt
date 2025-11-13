from model.LockManager import LockManager
from helper.Operation import Operation


def run_lock_manager_tests():
	lm = LockManager()

	# Shared lock: T1, T2 read resource A
	assert lm.request_lock(Operation(1, 'R', 'A')) is True
	assert lm.request_lock(Operation(2, 'read', 'A')) is True
	status = lm._get_resource('A')
	assert status.lockMode == 'S'
	assert status.lockedBy == {1, 2}

	# Exclusive lock: T3 write A (fail)
	assert lm.request_lock(Operation(3, 'W', 'A')) is False

	# Release T1 and T2
	lm.release_locks(1)
	lm.release_locks(2)
	# Exclusive lock: T3 write A (true)
	assert lm.request_lock(Operation(3, 'W', 'A')) is True
	status = lm._get_resource('A')
	assert status.lockMode == 'X' and status.lockedBy == {3}

	# ulangi lock
	assert lm.request_lock(Operation(3, 'W', 'A')) is True

	# Exclusive lock: T4 read A (fail), T4 write A (fail)
	assert lm.request_lock(Operation(4, 'R', 'A')) is False
	assert lm.request_lock(Operation(4, 'W', 'A')) is False

	# Release exclusive
	lm.release_locks(3)
	# T5 Read B
	assert lm.request_lock(Operation(5, 'R', 'B')) is True
	# T5 Write B
	assert lm.request_lock(Operation(5, 'W', 'B')) is True
	status = lm._get_resource('B')
	assert status.lockMode == 'X' and status.lockedBy == {5}

	# Release Exclusive
	lm.release_locks(5)
	# Multiple shared locks di C
	assert lm.request_lock(Operation(6, 'R', 'C')) is True
	assert lm.request_lock(Operation(7, 'R', 'C')) is True
	# T6 Write C (fail)
	assert lm.request_lock(Operation(6, 'W', 'C')) is False

	print('Lock Manager Test passed.')
