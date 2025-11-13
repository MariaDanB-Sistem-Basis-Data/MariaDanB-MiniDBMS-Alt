from enum import Enum, auto


class TransactionStatus(Enum):
    ACTIVE = auto()
    COMMITTED = auto()
    ABORTED = auto()


class Action(Enum):
    READ = auto()
    WRITE = auto()
    COMMIT = auto()
    ABORT = auto()
