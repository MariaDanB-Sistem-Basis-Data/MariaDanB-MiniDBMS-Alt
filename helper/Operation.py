class Operation:
    def __init__(self, transaction_id:int, operation_type:str, resource_id:str):
        self.transaction_id = transaction_id
        self.operation_type = operation_type  
        self.resource_id = resource_id