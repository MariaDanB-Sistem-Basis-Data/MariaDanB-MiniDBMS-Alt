class Operation:
    def __init__(self, transaction_id, operation_type, resource_id):
        self.transaction_id = transaction_id
        self.operation_type = operation_type  
        self.resource_id = resource_id