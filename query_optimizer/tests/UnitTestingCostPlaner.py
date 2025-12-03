"""
Cara penggunaan cost planner
run command ini -> python tests/UnitTestingCostPlaner.py
"""

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from QueryOptimizer import OptimizationEngine
from helper.cost import CostPlanner

def test_simple_select():
    """Test 1: Simple SELECT dengan WHERE clause"""
    print("\n" + "="*70)
    print("TEST 1: Simple SELECT dengan WHERE")
    print("="*70)
    
    # Query: SELECT name FROM employees WHERE salary > 50000
    query_text = "SELECT name FROM employees WHERE salary > 50000;"
    
    # Parse query menggunakan OptimizationEngine
    optimizer = OptimizationEngine()
    parsed_query = optimizer.parse_query(query_text)
    
    # Plan dan print
    planner = CostPlanner()
    cost_plan = planner.plan_query(parsed_query)
    planner.print_cost_breakdown(cost_plan)


def test_join_query():
    """Test 2: JOIN query"""
    print("\n" + "="*70)
    print("TEST 2: JOIN Query")
    print("="*70)
    
    # Query: SELECT * FROM employees JOIN departments ON emp.dept_id = dept.id
    query_text = "SELECT * FROM employees JOIN departments ON employees.dept_id = departments.id;"
    
    # Parse query menggunakan OptimizationEngine
    optimizer = OptimizationEngine()
    parsed_query = optimizer.parse_query(query_text)
    
    # Plan dan print
    planner = CostPlanner()
    cost_plan = planner.plan_query(parsed_query)
    planner.print_cost_breakdown(cost_plan)


def test_complex_query():
    """Test 3: Complex query dengan JOIN, WHERE, ORDER BY"""
    print("\n" + "="*70)
    print("TEST 3: Complex Query (JOIN + WHERE + ORDER BY)")
    print("="*70)
    
    # Query: SELECT e.name, d.name 
    #        FROM employees e JOIN departments d ON e.dept_id = d.id
    #        WHERE e.salary > 50000
    #        ORDER BY e.name
    
    query_text = """
    SELECT e.name, d.name
    FROM employees e JOIN departments d ON e.dept_id = d.id
    WHERE e.salary > 50000
    ORDER BY e.name;
    """
    
    # Parse query menggunakan OptimizationEngine
    optimizer = OptimizationEngine()
    parsed_query = optimizer.parse_query(query_text)
    
    # Plan dan print
    planner = CostPlanner()
    cost_plan = planner.plan_query(parsed_query)
    planner.print_cost_breakdown(cost_plan)


def test_aggregation_query():
    """Test 4: Aggregation query dengan GROUP BY"""
    print("\n" + "="*70)
    print("TEST 4: Aggregation Query (GROUP BY)")
    print("="*70)
    
    # Query: SELECT dept_id, COUNT(*) FROM employees GROUP BY dept_id
    query_text = "SELECT dept_id, COUNT(*) FROM employees GROUP BY dept_id;"
    
    # Parse query menggunakan OptimizationEngine
    optimizer = OptimizationEngine()
    parsed_query = optimizer.parse_query(query_text)
    
    # Parse query menggunakan OptimizationEngine
    optimizer = OptimizationEngine()
    parsed_query = optimizer.parse_query(query_text)
    
    # Plan dan print
    planner = CostPlanner()
    cost_plan = planner.plan_query(parsed_query)
    planner.print_cost_breakdown(cost_plan)


def test_with_limit():
    """Test 5: Query dengan LIMIT"""
    print("\n" + "="*70)
    print("TEST 5: Query dengan LIMIT")
    print("="*70)
    
    # Query: SELECT * FROM orders WHERE status = 'completed' LIMIT 100
    query_text = "SELECT * FROM orders WHERE status = 'completed' LIMIT 100;"
    
    # Parse query menggunakan OptimizationEngine
    optimizer = OptimizationEngine()
    parsed_query = optimizer.parse_query(query_text)
    
    # Plan dan print
    planner = CostPlanner()
    cost_plan = planner.plan_query(parsed_query)
    planner.print_cost_breakdown(cost_plan)
    
    # Plan dan print
    planner = CostPlanner()
    cost_plan = planner.plan_query(parsed_query)
    planner.print_cost_breakdown(cost_plan)


def print_table_statistics():
    """Print statistik tabel yang digunakan"""
    print("\n" + "="*70)
    print("STATISTIK TABEL (Dummy Data)")
    print("="*70)
    
    planner = CostPlanner()
    
    tables = ["employees", "departments", "orders", "customers", "products"]
    
    print(f"{'Table':<15} {'Blocking Factor':<18} {'Total Blocks':<15} {'Total Records':<15}")
    print("-" * 70)
    
    for table in tables:
        stats = planner.get_table_stats(table)
        bf = stats.get('f_r', 0)
        blocks = stats.get('b_r', 0)
        records = stats.get('n_r', 0)
        print(f"{table:<15} {bf:<18} {blocks:<15} {records:<15}")
    
    print("="*70)


def test_get_cost_function():
    print("\n" + "="*70)
    print("TEST 6: Fungsi get_cost() - Simple Cost Retrieval")
    print("="*70)
    
    # Query: SELECT * FROM products
    query_text = "SELECT * FROM products;"
    
    # Parse query menggunakan OptimizationEngine
    optimizer = OptimizationEngine()
    parsed_query = optimizer.parse_query(query_text)
    
    # Test get_cost() function
    planner = CostPlanner()
    cost = planner.get_cost(parsed_query)
    
    print(f"Query: {query_text}")
    print(f"Cost (using get_cost()): {cost}")
    print(f"Type: {type(cost)}")
    print("\nFungsi get_cost() mengembalikan integer cost langsung!")
    print("="*70)


if __name__ == "__main__":
    print("\n" + "#"*70)
    print("# COST PLANNER - TEST SUITE")
    print("#"*70)
    
    # Print statistik dulu
    print_table_statistics()
    
    # Run all tests
    test_simple_select()
    test_join_query()
    test_complex_query()
    test_aggregation_query()
    test_with_limit()
    test_get_cost_function()
    
    print("\n" + "#"*70)
    print("# SEMUA TEST SELESAI")
    print("#"*70)
