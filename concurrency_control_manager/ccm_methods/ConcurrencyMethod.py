from abc import ABC, abstractmethod
from ccm_helper.Row import Row
from ccm_model.Response import Response

class ConcurrencyMethod(ABC):

    @abstractmethod
    def set_transaction_manager(self, transaction_manager) -> None:
        pass
    
    @abstractmethod
    def log_object(self, obj, transaction_id: int) -> None:
        pass

    @abstractmethod
    def validate_object(self, obj: Row, transaction_id: int, action) -> Response:
        pass

    @abstractmethod
    def end_transaction(self, transaction_id: int) -> Response:
        pass
