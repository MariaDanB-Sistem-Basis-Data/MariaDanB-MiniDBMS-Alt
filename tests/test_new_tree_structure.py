"""
Test untuk memahami struktur tree baru dengan LogicalNode, ConditionNode, dan ColumnNode.

Tujuan: Print tree structure untuk berbagai query untuk memahami format baru.
"""

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from QueryOptimizer import OptimizationEngine
from model.query_tree import QueryTree, LogicalNode, ConditionNode, ColumnNode

def print_tree_recursive(node, indent=0, label=""):
    """
    Print query tree secara recursive dengan format yang jelas.
    """
    prefix = "  " * indent
    
    if isinstance(node, QueryTree):
        print(f"{prefix}{label}QueryTree(")
        print(f"{prefix}  type = '{node.type}'")
        
        # Print value
        if node.val is not None:
            print(f"{prefix}  val = ", end="")
            if isinstance(node.val, (LogicalNode, ConditionNode, ColumnNode)):
                print()
                print_tree_recursive(node.val, indent + 2)
            else:
                print(f"{node.val}")
        else:
            print(f"{prefix}  val = None")
        
        # Print children
        if node.childs:
            print(f"{prefix}  childs = [")
            for i, child in enumerate(node.childs):
                print_tree_recursive(child, indent + 2, f"[{i}] ")
            print(f"{prefix}  ]")
        else:
            print(f"{prefix}  childs = []")
        
        print(f"{prefix})")
    
    elif isinstance(node, LogicalNode):
        print(f"{prefix}{label}LogicalNode(")
        print(f"{prefix}  operator = '{node.operator}'")
        print(f"{prefix}  childs = [")
        for i, child in enumerate(node.childs):
            print_tree_recursive(child, indent + 2, f"[{i}] ")
        print(f"{prefix}  ]")
        print(f"{prefix})")
    
    elif isinstance(node, ConditionNode):
        print(f"{prefix}{label}ConditionNode(")
        print(f"{prefix}  attr = ", end="")
        if isinstance(node.attr, ColumnNode):
            print()
            print_tree_recursive(node.attr, indent + 2)
        else:
            print(f"{node.attr}")
        print(f"{prefix}  op = '{node.op}'")
        print(f"{prefix}  value = ", end="")
        if isinstance(node.value, ColumnNode):
            print()
            print_tree_recursive(node.value, indent + 2)
        else:
            print(f"{node.value}")
        print(f"{prefix})")
    
    elif isinstance(node, ColumnNode):
        table_str = f"table='{node.table}'" if node.table else "table=None"
        print(f"{prefix}{label}ColumnNode(column='{node.column}', {table_str})")
    
    else:
        print(f"{prefix}{label}{node}")


def test_simple_and():
    """Test: Simple AND condition"""
    print("=" * 80)
    print("TEST 1: Simple AND Condition")
    print("=" * 80)
    
    query = "SELECT * FROM students WHERE age > 18 AND gpa > 30;"
    print(f"Query: {query}\n")
    
    optimizer = OptimizationEngine()
    parsed = optimizer.parse_query(query)
    
    print("Tree Structure:")
    print("-" * 80)
    print_tree_recursive(parsed.query_tree)
    print("\n" + "=" * 80 + "\n")


def test_simple_or():
    """Test: Simple OR condition"""
    print("=" * 80)
    print("TEST 2: Simple OR Condition")
    print("=" * 80)
    
    query = "SELECT * FROM students WHERE gpa > 35 OR gpa < 20;"
    print(f"Query: {query}\n")
    
    optimizer = OptimizationEngine()
    parsed = optimizer.parse_query(query)
    
    print("Tree Structure:")
    print("-" * 80)
    print_tree_recursive(parsed.query_tree)
    print("\n" + "=" * 80 + "\n")


def test_complex_logical():
    """Test: Complex logical expression (AND + OR)"""
    print("=" * 80)
    print("TEST 3: Complex Logical Expression (AND + OR)")
    print("=" * 80)
    
    query = "SELECT * FROM students WHERE age > 18 AND (gpa > 35 OR gpa < 20);"
    print(f"Query: {query}\n")
    
    optimizer = OptimizationEngine()
    parsed = optimizer.parse_query(query)
    
    print("Tree Structure:")
    print("-" * 80)
    print_tree_recursive(parsed.query_tree)
    print("\n" + "=" * 80 + "\n")


def test_or_and_mixed():
    """Test: OR and AND mixed"""
    print("=" * 80)
    print("TEST 4: OR and AND Mixed")
    print("=" * 80)
    
    query = "SELECT * FROM t WHERE a = 1 AND b = 2 OR c = 3;"
    print(f"Query: {query}\n")
    
    optimizer = OptimizationEngine()
    parsed = optimizer.parse_query(query)
    
    print("Tree Structure:")
    print("-" * 80)
    print_tree_recursive(parsed.query_tree)
    print("\n" + "=" * 80 + "\n")


def test_join_with_condition():
    """Test: JOIN with conditions"""
    print("=" * 80)
    print("TEST 5: JOIN with Conditions")
    print("=" * 80)
    
    query = "SELECT * FROM students JOIN enrollments ON students.student_id = enrollments.student_id WHERE students.gpa > 35;"
    print(f"Query: {query}\n")
    
    optimizer = OptimizationEngine()
    parsed = optimizer.parse_query(query)
    
    print("Tree Structure:")
    print("-" * 80)
    print_tree_recursive(parsed.query_tree)
    print("\n" + "=" * 80 + "\n")


def test_equality_condition():
    """Test: Simple equality condition"""
    print("=" * 80)
    print("TEST 6: Simple Equality Condition")
    print("=" * 80)
    
    query = "SELECT * FROM students WHERE gpa = 35;"
    print(f"Query: {query}\n")
    
    optimizer = OptimizationEngine()
    parsed = optimizer.parse_query(query)
    
    print("Tree Structure:")
    print("-" * 80)
    print_tree_recursive(parsed.query_tree)
    print("\n" + "=" * 80 + "\n")


def test_multiple_or():
    """Test: Multiple OR conditions"""
    print("=" * 80)
    print("TEST 7: Multiple OR Conditions")
    print("=" * 80)
    
    query = "SELECT * FROM students WHERE age < 20 OR age > 25 OR gpa = 40;"
    print(f"Query: {query}\n")
    
    optimizer = OptimizationEngine()
    parsed = optimizer.parse_query(query)
    
    print("Tree Structure:")
    print("-" * 80)
    print_tree_recursive(parsed.query_tree)
    print("\n" + "=" * 80 + "\n")


if __name__ == "__main__":
    print("\n" + "#" * 80)
    print("# NEW TREE STRUCTURE TEST SUITE")
    print("# Testing LogicalNode, ConditionNode, ColumnNode")
    print("#" * 80 + "\n")
    
    # Run all tests
    test_simple_and()
    test_simple_or()
    test_complex_logical()
    test_or_and_mixed()
    test_join_with_condition()
    test_equality_condition()
    test_multiple_or()
    
    print("#" * 80)
    print("# ALL TREE STRUCTURE TESTS COMPLETED")
    print("#" * 80)
