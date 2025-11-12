class DeadlockDetector:
    def __init__(self):
        print("Deadlock Detector")

    def validate_object(object, transaction_id, action):
        # memeriksa apakah action pada object dengan transaction_id bisa dilakukan
        print(f"Validate action {action} on object {object} for transaction_id {transaction_id}")
    
    def check_deadlock(self):
        # memeriksa apakah ada deadlock
        print("Checking for deadlock")
        return True