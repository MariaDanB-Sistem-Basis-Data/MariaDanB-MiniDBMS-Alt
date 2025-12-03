from qp_helper.demo_dependencies import build_query_processor

# TODO: masih belum sesuai

def test_select_basic():
    print("\n" + "="*60)
    print("TEST 1: Basic SELECT Query")
    print("="*60)
    
    qp = build_query_processor()
    query = "SELECT * FROM Student;"
    result = qp.execute_query(query)
    
    print(f"Query: {query}")
    print(f"Transaction ID: {result.transaction_id}")
    print(f"Timestamp: {result.timestamp}")
    print(f"Message: {result.message}")
    print(f"Data rows count: {result.data.rows_count}")
    print(f"Data: {result.data.data}")
    
    assert result.message == "Success", "Query should succeed"
    print("✓ Test passed!")

def test_select_with_projection():
    print("\n" + "="*60)
    print("TEST 2: SELECT with Projection")
    print("="*60)
    
    qp = build_query_processor()
    query = "SELECT StudentID, FullName FROM Student;"
    result = qp.execute_query(query)
    
    print(f"Query: {query}")
    print(f"Transaction ID: {result.transaction_id}")
    print(f"Message: {result.message}")
    print(f"Result: {result}")
    print(f"Data rows count: {result.data.rows_count}")
    print(f"Data: {result.data.data}")
    
    assert result.message == "Success", "Query should succeed"
    print("✓ Test passed!")

def test_select_with_where():
    print("\n" + "="*60)
    print("TEST 3: SELECT with WHERE clause")
    print("="*60)
    
    qp = build_query_processor()
    query = "SELECT * FROM Student WHERE StudentID > 25;"
    result = qp.execute_query(query)
    
    print(f"Query: {query}")
    print(f"Transaction ID: {result.transaction_id}")
    print(f"Message: {result.message}")
    print(f"Data rows count: {result.data.rows_count}")
    print(f"Data: {result.data.data}")
    
    assert result.message == "Success", "Query should succeed"
    print("✓ Test passed!")

def test_select_with_projection_and_where():
    print("\n" + "="*60)
    print("TEST 4: SELECT with Projection and WHERE")
    print("="*60)
    
    qp = build_query_processor()
    query = "SELECT StudentID, FullName FROM Student WHERE StudentID > 25;"
    result = qp.execute_query(query)
    
    print(f"Query: {query}")
    print(f"Transaction ID: {result.transaction_id}")
    print(f"Message: {result.message}")
    print(f"Data rows count: {result.data.rows_count}")
    print(f"Data: {result.data.data}")
    
    assert result.message == "Success", "Query should succeed"
    print("✓ Test passed!")

def test_update_basic():
    print("\n" + "="*60)
    print("TEST 5: Basic UPDATE Query")
    print("="*60)
    
    qp = build_query_processor()
    query = "UPDATE Student SET GPA = 3.95 WHERE StudentID = 3;"
    result = qp.execute_query(query)
    
    print(f"Query: {query}")
    print(f"Transaction ID: {result.transaction_id}")
    print(f"Message: {result.message}")
    print(f"Data: {result.data.data}")
    
    assert result.message == "Success", "Query should succeed"
    print("✓ Test passed!")

def test_error_handling():
    print("\n" + "="*60)
    print("TEST 6: Error Handling")
    print("="*60)
    
    qp = build_query_processor()
    query = "INVALID QUERY SYNTAX"
    result = qp.execute_query(query)
    
    print(f"Query: {query}")
    print(f"Message: {result.message}")
    print(f"Data: {result.data}")
    
    assert result.data == -1, "Should return error code"
    print("✓ Test passed!")

def test_select_with_limit():
    print("\n" + "="*60)
    print("TEST 7: SELECT with LIMIT")
    print("="*60)
    
    qp = build_query_processor()
    query = "SELECT * FROM Student LIMIT 5;"
    result = qp.execute_query(query)
    
    print(f"Query: {query}")
    print(f"Transaction ID: {result.transaction_id}")
    print(f"Message: {result.message}")
    print(f"Data rows count: {result.data.rows_count}")
    print(f"Data: {result.data.data}")
    
    assert result.message == "Success", "Query should succeed"
    assert result.data.rows_count <= 5, "Should return at most 5 rows"
    print("✓ Test passed!")

def test_select_projection_with_limit():
    print("\n" + "="*60)
    print("TEST 8: SELECT with Projection and LIMIT")
    print("="*60)
    
    qp = build_query_processor()
    query = "SELECT StudentID, FullName FROM Student LIMIT 10;"
    result = qp.execute_query(query)
    
    print(f"Query: {query}")
    print(f"Transaction ID: {result.transaction_id}")
    print(f"Message: {result.message}")
    print(f"Data rows count: {result.data.rows_count}")
    print(f"Data: {result.data.data}")
    
    assert result.message == "Success", "Query should succeed"
    assert result.data.rows_count <= 10, "Should return at most 10 rows"
    print("✓ Test passed!")

def test_select_where_with_limit():
    print("\n" + "="*60)
    print("TEST 9: SELECT with WHERE and LIMIT")
    print("="*60)
    
    qp = build_query_processor()
    query = "SELECT * FROM Student WHERE GPA > 3.0 LIMIT 3;"
    result = qp.execute_query(query)
    
    print(f"Query: {query}")
    print(f"Transaction ID: {result.transaction_id}")
    print(f"Message: {result.message}")
    print(f"Data rows count: {result.data.rows_count}")
    print(f"Data: {result.data.data}")
    
    assert result.message == "Success", "Query should succeed"
    assert result.data.rows_count <= 3, "Should return at most 3 rows"
    print("✓ Test passed!")

if __name__ == "__main__":
    print("\n" + "="*60)
    print("QUERY PROCESSOR UNIT TESTS - SELECT & UPDATE")
    print("="*60)
    
    try:
        test_select_basic()
        test_select_with_projection()
        test_select_with_where()
        test_select_with_projection_and_where()
        test_update_basic()
        test_error_handling()
        test_select_with_limit()
        test_select_projection_with_limit()
        test_select_where_with_limit()
        
        print("\n" + "="*60)
        print("ALL TESTS PASSED! ✓")
        print("="*60)
        
    except AssertionError as e:
        print(f"\n✗ Test failed: {e}")
    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
