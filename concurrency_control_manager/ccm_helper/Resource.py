class Resource:
    def __init__(self, resourceName:str)-> None:
        # self.tableName = tableName
        self.resourceName = resourceName
        # lockMode: None (unlocked), 'S' (shared), 'X' (exclusive)
        self.lockMode = None
        # lockedBy: set of transaction ids holding locks on this resource
        self.lockedBy = set()

    def set_lock(self, lockMode: str) -> None:
        self.lockMode = lockMode
    def remove_lock(self) -> None:
        self.lockMode = None
    
    def add_locker(self, lockerName: int) -> None:
        self.lockedBy.add(lockerName)
    def remove_locker(self, lockerName: int) -> None:
        self.lockedBy.remove(lockerName)
    def clear_locker(self) -> None:
        self.lockedBy.clear()

