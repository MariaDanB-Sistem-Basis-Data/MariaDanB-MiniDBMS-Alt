import threading
from typing import TypeVar, Type

T = TypeVar('T')


def singleton(cls: Type[T]) -> Type[T]:
    """Thread-safe singleton decorator."""
    instances = {}
    lock = threading.Lock()

    def get_instance(*args, **kwargs) -> T:
        if cls not in instances:
            with lock:
                if cls not in instances:
                    instances[cls] = cls(*args, **kwargs)
        return instances[cls]

    get_instance.__name__ = cls.__name__
    get_instance.__doc__ = cls.__doc__

    return get_instance
