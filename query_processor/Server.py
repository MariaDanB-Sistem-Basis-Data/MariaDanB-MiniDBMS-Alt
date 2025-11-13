# server.py - main Query Processor Server
# orchestrates query routing ke komponen yang tepat

from query_processor.QueryProcessor import QueryProcessor
from query_processor.model.ExecutionResult import ExecutionResult
from concurrency_control_manager.ConcurrencyControlManager import ConcurrencyControlManager
from failure_recovery_manager.FailureRecovery import FailureRecoveryManager
from storage_manager.StorageManager import StorageManager

class QueryProcessorServer:
    
    def __init__(self):
        """Initialize server dengan semua komponen yang diperlukan"""
        self.query_processor = QueryProcessor()
        self.concurrency_manager = ConcurrencyControlManager()
        self.recovery_manager = FailureRecoveryManager()
        self.storage_manager = StorageManager()
        # track transaction state via concurrency manager
        self._active_transaction = None
    
    def execute_query(self, query: str) -> ExecutionResult:
        """
        Main entry point untuk execute query.
        Delegate ke komponen yang sesuai berdasarkan query type.
        """
        query_stripped = query.strip()
        query_upper = query_stripped.upper()
        
        try:
            # query data (SELECT/UPDATE) - delegate ke QueryProcessor
            if query_upper.startswith("SELECT") or query_upper.startswith("UPDATE"):
                return self.query_processor.execute_query(query_stripped)
            
            # transaction control - delegate ke ConcurrencyControlManager
            elif query_upper.startswith("BEGIN"):
                return self._handle_begin_transaction(query_stripped)
            
            elif query_upper.startswith("COMMIT"):
                return self._handle_commit(query_stripped)
            
            elif query_upper.startswith("ABORT"):
                return self._handle_abort(query_stripped)
            
            else:
                return ExecutionResult(
                    transaction_id=None,
                    timestamp=None,
                    message=f"Error: Unsupported query type",
                    data=-1,
                    query=query_stripped
                )
        
        except Exception as e:
            print(f"[Server Error] {str(e)}")
            return ExecutionResult(
                transaction_id=self._active_transaction,
                timestamp=None,
                message=f"Error: {str(e)}",
                data=-1,
                query=query_stripped
            )
    
    def _handle_begin_transaction(self, query: str) -> ExecutionResult:
        """Handle BEGIN TRANSACTION - delegate ke ConcurrencyControlManager"""
        if self._active_transaction:
            return ExecutionResult(
                transaction_id=None,
                timestamp=None,
                message="Error: Transaction already active",
                data=-1,
                query=query
            )
        
        tid = self.concurrency_manager.begin_transaction()
        self._active_transaction = tid
        print(f"[BEGIN] Transaction {tid} started")
        
        return ExecutionResult(
            transaction_id=tid,
            timestamp=None,
            message=f"Transaction {tid} started",
            data=tid,
            query=query
        )
    
    def _handle_commit(self, query: str) -> ExecutionResult:
        """Handle COMMIT - delegate ke ConcurrencyControlManager"""
        if not self._active_transaction:
            return ExecutionResult(
                transaction_id=None,
                timestamp=None,
                message="Error: No active transaction",
                data=-1,
                query=query
            )
        
        tid = self._active_transaction
        self.concurrency_manager.commit_transaction(tid)
        self._active_transaction = None
        print(f"[COMMIT] Transaction {tid} committed")
        
        return ExecutionResult(
            transaction_id=tid,
            timestamp=None,
            message=f"Transaction {tid} committed",
            data=1,
            query=query
        )
    
    def _handle_abort(self, query: str) -> ExecutionResult:
        """Handle ABORT - delegate ke ConcurrencyControlManager"""
        if not self._active_transaction:
            return ExecutionResult(
                transaction_id=None,
                timestamp=None,
                message="Error: No active transaction",
                data=-1,
                query=query
            )
        
        tid = self._active_transaction
        self.concurrency_manager.abort_transaction(tid)
        self._active_transaction = None
        print(f"[ABORT] Transaction {tid} aborted")
        
        return ExecutionResult(
            transaction_id=tid,
            timestamp=None,
            message=f"Transaction {tid} aborted",
            data=1,
            query=query
        )
    
    def get_current_transaction(self):
        """Get current active transaction ID"""
        return self._active_transaction
    
    def shutdown(self):
        """Graceful shutdown - abort any active transaction"""
        if self._active_transaction:
            print(f"\n[Shutdown] Aborting active transaction {self._active_transaction}")
            self.concurrency_manager.abort_transaction(self._active_transaction)
            self._active_transaction = None
        print("[Shutdown] Server shutdown complete")


class CLIInterface:
    """
    CLI interface untuk interact dengan QueryProcessorServer.
    TODO: Implement by team member responsible for UI/CLI
    
    Harus provide:
    - display_banner() - tampilkan welcome message
    - display_result() - tampilkan hasil query
    - display_status() - tampilkan status server
    - get_prompt() - return formatted prompt
    - run() - main CLI loop
    """
    
    def __init__(self, server: QueryProcessorServer):
        """Initialize CLI dengan server instance"""
        self.server = server
        raise NotImplementedError("CLI Interface belum diimplementasikan - assign ke team member")
    
    def display_banner(self):
        """Display server banner dan available commands"""
        raise NotImplementedError("To be implemented")
    
    def display_result(self, result: ExecutionResult):
        """Display query result in formatted way"""
        raise NotImplementedError("To be implemented")
    
    def display_status(self):
        """Display current server status"""
        raise NotImplementedError("To be implemented")
    
    def get_prompt(self) -> str:
        """Get formatted input prompt"""
        raise NotImplementedError("To be implemented")
    
    def run(self):
        """Run CLI interface - main loop"""
        raise NotImplementedError("To be implemented")


def main():
    """Main entry point untuk run server"""
    server = QueryProcessorServer()
    cli = CLIInterface(server)
    cli.run()


if __name__ == "__main__":
    main()
