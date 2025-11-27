import threading
from typing import TypeVar, Type

T = TypeVar('T')


def singleton(cls: Type[T]) -> Type[T]:
    instances = {}
    lock = threading.Lock()

    def get_instance(*args, **kwargs) -> T:
        if cls not in instances:
            with lock:
                if cls not in instances:
                    instances[cls] = cls(*args, **kwargs)
        return instances[cls]

    def reset_instance() -> None: # It is very very important! For testing purposes, do not touch ok TT
        with lock:
            if cls in instances:
                if hasattr(instances[cls], 'initialized'):
                    delattr(instances[cls], 'initialized')
                del instances[cls]

    get_instance.__name__ = cls.__name__
    get_instance.__doc__ = cls.__doc__
    get_instance.reset_instance = reset_instance

    return get_instance
