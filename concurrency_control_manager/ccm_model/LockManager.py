from ccm_helper.Operation import Operation
from ccm_helper.Resource import Resource
class LockManager:
	def __init__(self):
		self.resources: dict[str, Resource] = {}

	def _get_resource(self, resource_id: str) -> Resource:
		# nambah resource ke list resoruce
		if resource_id not in self.resources:
			self.resources[resource_id] = Resource(resourceName=resource_id)
		return self.resources[resource_id]

	def request_lock(self, operation: Operation) -> tuple[bool, set[int]]:
		tx = operation.transaction_id
		r_id = operation.resource_id
		op = str(operation.operation_type).lower()
		res = self._get_resource(r_id)

        #Nanti diubah sesuai operationnya apa aja
		want_shared = op in ('r', 'read')
		want_exclusive = op in ('w', 'write')

		if res.lockMode == 'X' and tx in res.lockedBy:
			return (True, set(res.lockedBy)) 

		if want_shared:
			if res.lockMode in (None, 'S'):
				res.set_lock('S')
				res.add_locker(tx)
				return (True, set(res.lockedBy)) 
			lock_holders = set(res.lockedBy)
			lock_holders.discard(tx)  
			return (False, lock_holders)

		if want_exclusive:
			if res.lockMode is None:
				res.set_lock('X')
				res.clear_locker()
				res.add_locker(tx)
				return (True, set(res.lockedBy)) 

			if res.lockMode == 'S':
				if res.lockedBy == {tx}:
					res.set_lock('X')
					res.clear_locker()
					res.add_locker(tx)
					return (True, set(res.lockedBy)) 
				else:
					lock_holders = set(res.lockedBy)
					lock_holders.discard(tx)  
					return (False, lock_holders)
			lock_holders = set(res.lockedBy)
			lock_holders.discard(tx)  
			return (False, lock_holders) 
		lock_holders = set(res.lockedBy)
		lock_holders.discard(tx)  
		return (False, lock_holders) 

	def release_locks(self, transaction_id:int) -> None:
		for res in self.resources.values():
			if transaction_id in res.lockedBy:
				res.remove_locker(transaction_id)
				if not res.lockedBy:
					res.remove_lock()
				else:
					if res.lockMode == 'X' and len(res.lockedBy) > 1:
						res.set_lock('S')


	def all_locks(self) -> dict[str, Resource]:
		return self.resources

	def log_object(self) -> dict[str, dict[str, Resource]]:
		serial = {}
		for rid, res in self.resources.items():
			serial[rid] = {res}
		return {'resources': serial}
