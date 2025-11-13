from dataclasses import dataclass, field
from typing import Set

from model.Enums import TransactionStatus


@dataclass
class Transaction:
    transaction_id: int
    status: TransactionStatus
    read_set: Set[str] = field(default_factory=set)
    write_set: Set[str] = field(default_factory=set)
    start_time: float = 0.0
