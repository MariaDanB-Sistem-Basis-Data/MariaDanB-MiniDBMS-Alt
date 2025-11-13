from helper.Operation import Operation
from helper.Resource import Resource
class LockManager:
	def __init__(self):
		self.resources = {}

	def _get_resource(self, resource_id):
		# nambah resource ke list resoruce
		if resource_id not in self.resources:
			self.resources[resource_id] = Resource(resourceName=resource_id)
		return self.resources[resource_id]

	def request_lock(self, operation: Operation) -> bool:
		tx = operation.transaction_id
		r_id = operation.resource_id
		op = str(operation.operation_type).lower()
		res = self._get_resource(r_id)

        #Nanti diubah sesuai operationnya apa aja
		want_shared = op in ('r', 'read')
		want_exclusive = op in ('w', 'write')

		if res.lockMode == 'X' and tx in res.lockedBy:
			return True

		if want_shared:
			if res.lockMode in (None, 'S'):
				res.set_lock('S')
				res.add_locker(tx)
				return True
			return False

		if want_exclusive:
			if res.lockMode is None:
				res.set_lock('X')
				res.clear_locker()
				res.add_locker(tx)
				return True

			if res.lockMode == 'S':
				if res.lockedBy == {tx}:
					res.set_lock('X')
					res.clear_locker()
					res.add_locker(tx)
					return True
				else:
					return False
			return False
		return False

	def release_locks(self, transaction_id):
		for res in self.resources.values():
			if transaction_id in res.lockedBy:
				res.remove_locker(transaction_id)
				if not res.lockedBy:
					res.remove_lock()
				else:
					if res.lockMode == 'X' and len(res.lockedBy) > 1:
						res.set_lock('S')

	def resource_status(self, resource_id):
		res = self._get_resource(resource_id)
		return {
			'resource_id': resource_id,
			'lockMode': res.lockMode,
			'lockedBy': set(res.lockedBy)
		}

	def all_locks(self):
		return {rid: self.resource_status(rid) for rid in self.resources}

	def log_object(self):
		serial = {}
		for rid, res in self.resources.items():
			serial[rid] = {
				'lockMode': res.lockMode,
				'lockedBy': list(res.lockedBy)
			}
		return {'resources': serial}
