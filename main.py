
import sys
sys.path.append('./query_processor')
sys.path.append('./query_optimizer')
sys.path.append('./storage_manager')
sys.path.append('./concurrency_control_manager')
sys.path.append('./failure_recovery_manager')

# NOTE: karena ada folder yang sama di beberapa repo (kaya folder model di setiap repo kan ada, nanti bakal clash dan mungkin error)
# fixnya pas import pake nama foldernya, contoh: asalnya `from model.blabla import fungsi` jadi `from query_processor.model.blabla import fungsi`

from query_processor.QueryProcessor import QueryProcessor

def demo_select_queries():
    """Demo SELECT queries dengan integrasi Query Optimizer"""
    print("\n" + "="*70)
    print("DEMO: SELECT QUERIES - Integrated with Query Optimizer")
    print("="*70)
    
    qp = QueryProcessor()
    
    # Test 1: Basic SELECT
    print("\n1. Basic SELECT:")
    result = qp.execute_query("SELECT * FROM users;")
    print(f"   Query: SELECT * FROM users;")
    print(f"   Result: {result.message}")
    if result.data != -1:
        print(f"   Rows: {result.data.rows_count}")
        print(f"   Sample data: {result.data.data[:2] if result.data.rows_count > 0 else 'No data'}")
    
    # Test 2: SELECT with WHERE
    print("\n2. SELECT with WHERE:")
    result = qp.execute_query("SELECT * FROM users WHERE city = 'Jakarta';")
    print(f"   Query: SELECT * FROM users WHERE city = 'Jakarta';")
    print(f"   Result: {result.message}")
    if result.data != -1:
        print(f"   Rows: {result.data.rows_count}")
    
    # Test 3: SELECT with projection
    print("\n3. SELECT with Projection:")
    result = qp.execute_query("SELECT name, age FROM users WHERE age > 28;")
    print(f"   Query: SELECT name, age FROM users WHERE age > 28;")
    print(f"   Result: {result.message}")
    if result.data != -1:
        print(f"   Data: {result.data.data}")

def demo_update_queries():
    """Demo UPDATE queries"""
    print("\n" + "="*70)
    print("DEMO: UPDATE QUERIES")
    print("="*70)
    
    qp = QueryProcessor()
    
    # Test 1: Basic UPDATE
    print("\n1. Basic UPDATE:")
    result = qp.execute_query("UPDATE users SET age = 26 WHERE name = 'Alice';")
    print(f"   Query: UPDATE users SET age = 26 WHERE name = 'Alice';")
    print(f"   Result: {result.message}")
    if result.data != -1:
        print(f"   Info: {result.data.data}")
    
    # Test 2: UPDATE multiple columns
    print("\n2. UPDATE Multiple Columns:")
    result = qp.execute_query("UPDATE users SET age = 31, city = 'Jakarta' WHERE name = 'Bob';")
    print(f"   Query: UPDATE users SET age = 31, city = 'Jakarta' WHERE name = 'Bob';")
    print(f"   Result: {result.message}")
    if result.data != -1:
        print(f"   Info: {result.data.data}")

def demo_transaction_queries():
    print("\n" + "="*70)
    print("DEMO: TRANSACTION QUERIES (Placeholder)")
    print("="*70)
    
    qp = QueryProcessor()
    
    print("\n1. BEGIN TRANSACTION:")
    result = qp.execute_query("BEGIN TRANSACTION;")
    if result.data != -1:
        print(f"   Result: {result.data.data}")
    
    print("\n2. COMMIT:")
    result = qp.execute_query("COMMIT;")
    if result.data != -1:
        print(f"   Result: {result.data.data}")
    
    print("\n3. ABORT:")
    result = qp.execute_query("ABORT;")
    if result.data != -1:
        print(f"   Result: {result.data.data}")

if __name__ == "__main__":
    # demos
    demo_select_queries()
    demo_update_queries()
    demo_transaction_queries()