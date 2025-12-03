class Response :
    def __init__(self, success: bool, message: str) :
        self.success = success
        self.message = message
    
    def printResponse(self) :
        print(f"Success: {self.success}, Message: '{self.message}'")