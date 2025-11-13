# server.py - main Query Processor Server
# orchestrates query routing ke komponen yang tepat

from query_processor.QueryProcessor import QueryProcessor
from query_processor.model.ExecutionResult import ExecutionResult

class QueryProcessorServer:
    
    def __init__(self):
        """Initialize server dengan QueryProcessor sebagai model utama"""
        self.query_processor = QueryProcessor()
    
    def execute_query(self, query: str) -> ExecutionResult:
        """
        Main entry point untuk execute query.
        Delegate ke komponen yang sesuai berdasarkan query type.
        """
        query_stripped = query.strip()
        query_upper = query_stripped.upper()
        
        try:
            # delegate all queries (data + transaction control) to QueryProcessor
            return self.query_processor.execute_query(query_stripped)
        
        except Exception as e:
            print(f"[Server Error] {str(e)}")
            return ExecutionResult(
                transaction_id=None,
                timestamp=None,
                message=f"Error: {str(e)}",
                data=-1,
                query=query_stripped
            )
    
    # transaction lifecycle is handled by QueryProcessor / QueryExecutor
    
    def get_current_transaction(self):
        """Server does not track current transaction; query processor/executor manage transactions."""
        return None
    
    def shutdown(self):
        """Graceful shutdown - server cleanup"""
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
