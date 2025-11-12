from abc import ABC, abstractmethod
from typing import Dict, Any, TypeVar

T = TypeVar('T', bound='Serializable')


class Serializable(ABC):
    """Abstract base class for serialization to/from dictionaries."""

    @abstractmethod
    def toDict(self) -> Dict[str, Any]:
        pass

    @staticmethod
    @abstractmethod
    def fromDict(data: Dict[str, Any]) -> 'Serializable':
        pass
