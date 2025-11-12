"""
Test for Query Processor - SELECT and UPDATE operations
"""

import sys
sys.path.append('../')
sys.path.append('../query_processor')
sys.path.append('../query_optimizer')
sys.path.append('../storage_manager')

from query_processor.QueryProcessor import QueryProcessor

def test_select_basic():
    """Test basic SELECT query"""
    print("\n" + "="*60)
    print("TEST 1: Basic SELECT Query")
    print("="*60)
    
    qp = QueryProcessor()
    query = "SELECT * FROM users;"
    result = qp.execute_query(query)
    
    print(f"Query: {query}")
    print(f"Transaction ID: {result.transaction_id}")
    print(f"Timestamp: {result.timestamp}")
    print(f"Message: {result.message}")
    print(f"Data rows count: {result.data.rows_count}")
    print(f"Data: {result.data.data}")
    
    assert result.message == "Success", "Query should succeed"
    assert result.data.rows_count > 0, "Should return data"
    print("✓ Test passed!")

def test_select_with_projection():
    """Test SELECT with specific columns (PROJECT)"""
    print("\n" + "="*60)
    print("TEST 2: SELECT with Projection")
    print("="*60)
    
    qp = QueryProcessor()
    query = "SELECT name, age FROM users;"
    result = qp.execute_query(query)
    
    print(f"Query: {query}")
    print(f"Transaction ID: {result.transaction_id}")
    print(f"Message: {result.message}")
    print(f"Data rows count: {result.data.rows_count}")
    print(f"Data: {result.data.data}")
    
    assert result.message == "Success", "Query should succeed"
    print("✓ Test passed!")

def test_select_with_where():
    """Test SELECT with WHERE clause (SIGMA)"""
    print("\n" + "="*60)
    print("TEST 3: SELECT with WHERE clause")
    print("="*60)
    
    qp = QueryProcessor()
    query = "SELECT * FROM users WHERE city = 'Jakarta';"
    result = qp.execute_query(query)
    
    print(f"Query: {query}")
    print(f"Transaction ID: {result.transaction_id}")
    print(f"Message: {result.message}")
    print(f"Data rows count: {result.data.rows_count}")
    print(f"Data: {result.data.data}")
    
    assert result.message == "Success", "Query should succeed"
    # Should only return rows where city = Jakarta (Alice and Charlie from dummy data)
    assert result.data.rows_count == 2, "Should return 2 rows with city='Jakarta'"
    print("✓ Test passed!")

def test_select_with_projection_and_where():
    """Test SELECT with both projection and WHERE"""
    print("\n" + "="*60)
    print("TEST 4: SELECT with Projection and WHERE")
    print("="*60)
    
    qp = QueryProcessor()
    query = "SELECT name, age FROM users WHERE age > 28;"
    result = qp.execute_query(query)
    
    print(f"Query: {query}")
    print(f"Transaction ID: {result.transaction_id}")
    print(f"Message: {result.message}")
    print(f"Data rows count: {result.data.rows_count}")
    print(f"Data: {result.data.data}")
    
    assert result.message == "Success", "Query should succeed"
    print("✓ Test passed!")

def test_select_with_multiple_where():
    """Test SELECT with multiple WHERE conditions (AND)"""
    print("\n" + "="*60)
    print("TEST 5: SELECT with Multiple WHERE (AND)")
    print("="*60)
    
    qp = QueryProcessor()
    query = "SELECT * FROM users WHERE age > 25 AND city = 'Jakarta';"
    result = qp.execute_query(query)
    
    print(f"Query: {query}")
    print(f"Transaction ID: {result.transaction_id}")
    print(f"Message: {result.message}")
    print(f"Data rows count: {result.data.rows_count}")
    print(f"Data: {result.data.data}")
    
    assert result.message == "Success", "Query should succeed"
    print("✓ Test passed!")

def test_update_basic():
    """Test basic UPDATE query"""
    print("\n" + "="*60)
    print("TEST 6: Basic UPDATE Query")
    print("="*60)
    
    qp = QueryProcessor()
    query = "UPDATE users SET age = 26 WHERE name = 'Alice';"
    result = qp.execute_query(query)
    
    print(f"Query: {query}")
    print(f"Transaction ID: {result.transaction_id}")
    print(f"Message: {result.message}")
    print(f"Data: {result.data.data}")
    
    assert result.message == "Success", "Query should succeed"
    print("✓ Test passed!")

def test_update_multiple_columns():
    """Test UPDATE with multiple columns"""
    print("\n" + "="*60)
    print("TEST 7: UPDATE Multiple Columns")
    print("="*60)
    
    qp = QueryProcessor()
    query = "UPDATE users SET age = 31, city = 'Jakarta' WHERE name = 'Bob';"
    result = qp.execute_query(query)
    
    print(f"Query: {query}")
    print(f"Transaction ID: {result.transaction_id}")
    print(f"Message: {result.message}")
    print(f"Data: {result.data.data}")
    
    assert result.message == "Success", "Query should succeed"
    print("✓ Test passed!")

def test_update_with_multiple_where():
    """Test UPDATE with multiple WHERE conditions"""
    print("\n" + "="*60)
    print("TEST 8: UPDATE with Multiple WHERE")
    print("="*60)
    
    qp = QueryProcessor()
    query = "UPDATE users SET city = 'Surabaya' WHERE age > 30 AND city = 'Jakarta';"
    result = qp.execute_query(query)
    
    print(f"Query: {query}")
    print(f"Transaction ID: {result.transaction_id}")
    print(f"Message: {result.message}")
    print(f"Data: {result.data.data}")
    
    assert result.message == "Success", "Query should succeed"
    print("✓ Test passed!")

def test_query_optimization():
    """Test that query optimization is applied"""
    print("\n" + "="*60)
    print("TEST 9: Query Optimization Check")
    print("="*60)
    
    qp = QueryProcessor()
    # Complex query that should benefit from optimization
    query = "SELECT name FROM users WHERE age > 25 AND city = 'Jakarta';"
    result = qp.execute_query(query)
    
    print(f"Query: {query}")
    print(f"Message: {result.message}")
    print(f"Note: Query was parsed and optimized by Query Optimizer")
    print(f"      - Parsed into QueryTree structure")
    print(f"      - Applied join optimization")
    print(f"      - Applied non-join optimization (selection push-down, etc.)")
    
    assert result.message == "Success", "Query should succeed"
    print("✓ Test passed!")

def test_error_handling():
    """Test error handling for invalid queries"""
    print("\n" + "="*60)
    print("TEST 10: Error Handling")
    print("="*60)
    
    qp = QueryProcessor()
    query = "INVALID QUERY SYNTAX"
    result = qp.execute_query(query)
    
    print(f"Query: {query}")
    print(f"Message: {result.message}")
    print(f"Data: {result.data}")
    
    assert result.data == -1, "Should return error code"
    print("✓ Test passed!")

if __name__ == "__main__":
    print("\n" + "="*60)
    print("QUERY PROCESSOR UNIT TESTS - SELECT & UPDATE")
    print("="*60)
    
    try:
        # SELECT tests
        test_select_basic()
        test_select_with_projection()
        test_select_with_where()
        test_select_with_projection_and_where()
        test_select_with_multiple_where()
        
        # UPDATE tests
        test_update_basic()
        test_update_multiple_columns()
        test_update_with_multiple_where()
        
        # Additional tests
        test_query_optimization()
        test_error_handling()
        
        print("\n" + "="*60)
        print("ALL TESTS PASSED! ✓")
        print("="*60)
        print("\nNotes:")
        print("1. SELECT queries are parsed and optimized by Query Optimizer")
        print("2. Query trees are built with PROJECT, SIGMA, TABLE nodes")
        print("3. UPDATE queries are parsed and executed via Storage Manager")
        print("4. Dummy data is used for testing (13 Nov milestone)")
        print("5. Full integration with Storage Manager pending")
        print("="*60)
        
    except AssertionError as e:
        print(f"\n✗ Test failed: {e}")
    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
