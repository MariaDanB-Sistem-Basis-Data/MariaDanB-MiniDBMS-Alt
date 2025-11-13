# server.py - Main Query Processor Server
# routes query ke komponen yang tepat (optimizer, concurrency control, recovery)

from query_processor.QueryProcessor import QueryProcessor
from query_processor.model.ExecutionResult import ExecutionResult
from concurrency_control_manager.ConcurrencyControlManager import ConcurrencyControlManager
from failure_recovery_manager.FailureRecovery import FailureRecoveryManager
from storage_manager.StorageManager import StorageManager

class QueryProcessorServer:
    def __init__(self):
        self.query_processor = QueryProcessor()
        self.concurrency_manager = ConcurrencyControlManager()
        self.recovery_manager = FailureRecoveryManager()
        self.storage_manager = StorageManager()
        self.active_transactions = {}  # track active transactions: {tid -> transaction_info}
        self.current_transaction_id = None  # track current active transaction
    
    def execute_query(self, query: str) -> ExecutionResult:
        """main entry point untuk execute query"""
        query_stripped = query.strip()
        query_upper = query_stripped.upper()
        
        try:
            if query_upper.startswith("SELECT") or query_upper.startswith("UPDATE"):
                # jika ada active transaction, query harus dalam transaction
                if self.current_transaction_id:
                    print(f"  [Transaction {self.current_transaction_id}] Executing query...")
                
                # route ke query processor (yang menggunakan optimizer + executor)
                result = self.query_processor.execute_query(query_stripped)
                return result
            
            elif query_upper.startswith("BEGIN"):
                # jika sudah ada active transaction, reject
                if self.current_transaction_id:
                    return ExecutionResult(
                        transaction_id=None,
                        timestamp=None,
                        message="Error: Transaction already active. Commit or abort current transaction first.",
                        data=-1,
                        query=query_stripped
                    )
                
                # route ke concurrency control manager
                transaction_id = self.concurrency_manager.begin_transaction()
                self.active_transactions[transaction_id] = {
                    "status": "ACTIVE",
                    "query_count": 0,
                    "start_time": None
                }
                self.current_transaction_id = transaction_id
                print(f"  [Transaction {transaction_id}] Started")
                return ExecutionResult(
                    transaction_id=transaction_id,
                    timestamp=None,
                    message=f"BEGIN TRANSACTION successful. Transaction ID: {transaction_id}",
                    data=transaction_id,
                    query=query_stripped
                )
            
            elif query_upper.startswith("COMMIT"):
                # jika tidak ada active transaction, reject
                if not self.current_transaction_id:
                    return ExecutionResult(
                        transaction_id=None,
                        timestamp=None,
                        message="Error: No active transaction to commit",
                        data=-1,
                        query=query_stripped
                    )
                
                tid = self.current_transaction_id
                self.concurrency_manager.commit_transaction(tid)
                self.active_transactions.pop(tid, None)
                self.current_transaction_id = None
                print(f"  [Transaction {tid}] Committed successfully")
                return ExecutionResult(
                    transaction_id=tid,
                    timestamp=None,
                    message="COMMIT successful - all changes persisted",
                    data=1,
                    query=query_stripped
                )
            
            elif query_upper.startswith("ABORT"):
                # jika tidak ada active transaction, reject
                if not self.current_transaction_id:
                    return ExecutionResult(
                        transaction_id=None,
                        timestamp=None,
                        message="Error: No active transaction to abort",
                        data=-1,
                        query=query_stripped
                    )
                
                tid = self.current_transaction_id
                self.concurrency_manager.abort_transaction(tid)
                self.active_transactions.pop(tid, None)
                self.current_transaction_id = None
                print(f"  [Transaction {tid}] Aborted - all changes rolled back")
                return ExecutionResult(
                    transaction_id=tid,
                    timestamp=None,
                    message="ABORT successful - all changes rolled back",
                    data=1,
                    query=query_stripped
                )
            
            else:
                return ExecutionResult(
                    transaction_id=None,
                    timestamp=None,
                    message=f"Error: Unknown query type - {query_stripped[:30]}...",
                    data=-1,
                    query=query_stripped
                )
        
        except Exception as e:
            print(f"  [Error] {str(e)}")
            return ExecutionResult(
                transaction_id=self.current_transaction_id,
                timestamp=None,
                message=f"Error: {str(e)}",
                data=-1,
                query=query_stripped
            )
    
    def get_active_transactions(self):
        """return list of active transactions"""
        return self.active_transactions
    
    def shutdown(self):
        """gracefully shutdown server"""
        if self.current_transaction_id:
            tid = self.current_transaction_id
            print(f"\nWarning: Active transaction {tid} will be aborted on shutdown")
            self.concurrency_manager.abort_transaction(tid)
            self.active_transactions.pop(tid, None)
            self.current_transaction_id = None
        print("Server shutdown complete")

def main():
    """main function untuk run server"""
    server = QueryProcessorServer()
    
    print("=" * 60)
    print("  Query Processor Server - Integrated System")
    print("=" * 60)
    print("\nComponents:")
    print("  - Query Optimizer    [Available]")
    print("  - Query Executor     [Available]")
    print("  - Storage Manager    [Available - Using Dummy Data]")
    print("  - Concurrency Control[Available]")
    print("  - Recovery Manager   [Available]")
    print("\nCommands:")
    print("  SELECT ... - execute select query")
    print("  UPDATE ... - execute update query")
    print("  BEGIN TRANSACTION - start new transaction")
    print("  COMMIT - commit active transaction")
    print("  ABORT - rollback active transaction")
    print("  STATUS - show server status")
    print("  EXIT - shutdown server")
    print("=" * 60 + "\n")
    
    while True:
        try:
            # Show transaction status in prompt
            if server.current_transaction_id:
                prompt = f"[T{server.current_transaction_id}] Query> "
            else:
                prompt = "Query> "
            
            query = input(prompt).strip()
            
            if not query:
                continue
            
            if query.upper() == "EXIT":
                print("\nShutting down...")
                server.shutdown()
                break
            
            elif query.upper() == "STATUS":
                active = server.get_active_transactions()
                print(f"\nServer Status:")
                print(f"  Active transactions: {len(active)}")
                print(f"  Current transaction: {server.current_transaction_id or 'None'}")
                print()
                continue
            
            # execute query
            result = server.execute_query(query)
            
            # display result
            if isinstance(result, ExecutionResult):
                status = "SUCCESS" if result.data != -1 else "FAILED"
                print(f"  Status: {status}")
                print(f"  Message: {result.message}")
                if result.data and result.data != -1 and isinstance(result.data, (dict, list)):
                    print(f"  Rows: {result.data}")
                print()
            else:
                print(f"  Result: {result}\n")
        
        except KeyboardInterrupt:
            print("\n\nServer terminated by user")
            server.shutdown()
            break
        except Exception as e:
            print(f"  Unexpected error: {e}\n")

if __name__ == "__main__":
    main()
