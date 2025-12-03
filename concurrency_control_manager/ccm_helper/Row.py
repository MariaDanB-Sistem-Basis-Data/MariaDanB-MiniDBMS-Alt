from dataclasses import dataclass
from typing import Union

@dataclass(frozen=True)
class Row:
    """
    Merepresentasikan satu baris data sebagai Resource yang dapat dikunci.
    Primary Key harus unik untuk mengidentifikasi Resource.
    """
    table_name: str
    pk_value: Union[int, str]
    data: dict 
    version: list[int]

    @property
    def resource_key(self) -> str:
        """Menghasilkan kunci unik string untuk LockManager."""
        return f"{self.table_name}:{self.pk_value}"
