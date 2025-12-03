from enum import Enum, auto


class TransactionStatus(Enum):
    ACTIVE = auto()
    PARTIALLY_COMMITTED = auto()
    COMMITTED = auto()
    FAILED = auto()
    ABORTED = auto()
    TERMINATED = auto()

class Action(Enum):
    READ = auto()
    WRITE = auto()
    COMMIT = auto()
    ABORT = auto()
