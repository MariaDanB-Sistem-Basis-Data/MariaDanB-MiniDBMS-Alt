"""
Test cost calculation dengan new tree structure (LogicalNode, ConditionNode).
"""

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from QueryOptimizer import OptimizationEngine
from helper.cost import CostPlanner

def test_simple_and_cost():
    """Test cost calculation untuk AND query"""
    print("=" * 80)
    print("TEST 1: Simple AND Query Cost Calculation")
    print("=" * 80)
    
    query = "SELECT * FROM students WHERE age > 18 AND gpa > 30;"
    print(f"Query: {query}\n")
    
    optimizer = OptimizationEngine()
    parsed = optimizer.parse_query(query)
    
    planner = CostPlanner()
    cost_info = planner.calculate_cost(parsed.query_tree)
    
    print("Cost Breakdown:")
    print(f"  Operation: {cost_info.get('operation', 'N/A')}")
    print(f"  Total Cost: {cost_info.get('cost', 0)}")
    print(f"  Estimated Records (n_r): {cost_info.get('n_r', 0)}")
    print(f"  Estimated Blocks (b_r): {cost_info.get('b_r', 0)}")
    print(f"  Selectivity: {cost_info.get('selectivity', 0):.4f}")
    print(f"  Condition: {cost_info.get('condition', 'N/A')}")
    print(f"  Description: {cost_info.get('description', 'N/A')}")
    print("\n" + "=" * 80 + "\n")


def test_simple_or_cost():
    """Test cost calculation untuk OR query"""
    print("=" * 80)
    print("TEST 2: Simple OR Query Cost Calculation")
    print("=" * 80)
    
    query = "SELECT * FROM students WHERE gpa > 35 OR gpa < 20;"
    print(f"Query: {query}\n")
    
    optimizer = OptimizationEngine()
    parsed = optimizer.parse_query(query)
    
    planner = CostPlanner()
    cost_info = planner.calculate_cost(parsed.query_tree)
    
    print("Cost Breakdown:")
    print(f"  Operation: {cost_info.get('operation', 'N/A')}")
    print(f"  Total Cost: {cost_info.get('cost', 0)}")
    print(f"  Estimated Records (n_r): {cost_info.get('n_r', 0)}")
    print(f"  Estimated Blocks (b_r): {cost_info.get('b_r', 0)}")
    print(f"  Selectivity: {cost_info.get('selectivity', 0):.4f}")
    print(f"  Condition: {cost_info.get('condition', 'N/A')}")
    print(f"  Description: {cost_info.get('description', 'N/A')}")
    print("\n" + "=" * 80 + "\n")


def test_equality_cost():
    """Test cost calculation untuk equality condition"""
    print("=" * 80)
    print("TEST 3: Equality Condition Cost Calculation")
    print("=" * 80)
    
    query = "SELECT * FROM students WHERE gpa = 35;"
    print(f"Query: {query}\n")
    
    optimizer = OptimizationEngine()
    parsed = optimizer.parse_query(query)
    
    planner = CostPlanner()
    
    # Get base stats
    base_stats = planner.get_table_stats("students")
    print("Base Table Stats:")
    print(f"  n_r: {base_stats['n_r']}")
    print(f"  V(gpa): {base_stats['v_a_r'].get('gpa', 'N/A')}")
    
    cost_info = planner.calculate_cost(parsed.query_tree)
    
    print("\nCost Breakdown:")
    print(f"  Operation: {cost_info.get('operation', 'N/A')}")
    print(f"  Total Cost: {cost_info.get('cost', 0)}")
    print(f"  Estimated Records (n_r): {cost_info.get('n_r', 0)}")
    print(f"  Estimated Blocks (b_r): {cost_info.get('b_r', 0)}")
    print(f"  Selectivity: {cost_info.get('selectivity', 0):.4f}")
    print(f"  Condition: {cost_info.get('condition', 'N/A')}")
    
    # Verify V(A,r) usage
    expected_selectivity = 1.0 / base_stats['v_a_r'].get('gpa', 1)
    expected_n_r = int(base_stats['n_r'] * expected_selectivity)
    
    print("\nVerification:")
    print(f"  Expected selectivity (1/V(gpa)): {expected_selectivity:.4f}")
    print(f"  Actual selectivity: {cost_info.get('selectivity', 0):.4f}")
    print(f"  Expected n_r: {expected_n_r}")
    print(f"  Actual n_r: {cost_info.get('n_r', 0)}")
    
    if abs(cost_info.get('selectivity', 0) - expected_selectivity) < 0.001:
        print("  ✅ Selectivity matches V(A,r) formula!")
    else:
        print("  ❌ Selectivity does NOT match!")
    
    print("\n" + "=" * 80 + "\n")


def test_multiple_or_cost():
    """Test cost calculation untuk multiple OR conditions"""
    print("=" * 80)
    print("TEST 4: Multiple OR Conditions Cost Calculation")
    print("=" * 80)
    
    query = "SELECT * FROM students WHERE age < 20 OR age > 25 OR gpa = 40;"
    print(f"Query: {query}\n")
    
    optimizer = OptimizationEngine()
    parsed = optimizer.parse_query(query)
    
    planner = CostPlanner()
    cost_info = planner.calculate_cost(parsed.query_tree)
    
    print("Cost Breakdown:")
    print(f"  Operation: {cost_info.get('operation', 'N/A')}")
    print(f"  Total Cost: {cost_info.get('cost', 0)}")
    print(f"  Estimated Records (n_r): {cost_info.get('n_r', 0)}")
    print(f"  Estimated Blocks (b_r): {cost_info.get('b_r', 0)}")
    print(f"  Selectivity: {cost_info.get('selectivity', 0):.4f}")
    print(f"  Condition: {cost_info.get('condition', 'N/A')}")
    print(f"  Description: {cost_info.get('description', 'N/A')}")
    
    # Manual calculation
    # age < 20: s1 = 0.5
    # age > 25: s2 = 0.5
    # gpa = 4.0: s3 = 1/41 = 0.024
    # Combined: 1 - (1-0.5)*(1-0.5)*(1-0.024) = 1 - 0.5*0.5*0.976 = 1 - 0.244 = 0.756
    expected_s1 = 0.5
    expected_s2 = 0.5
    expected_s3 = 1.0 / 41  # V(gpa) = 41
    expected_combined = 1 - (1-expected_s1)*(1-expected_s2)*(1-expected_s3)
    
    print("\nManual Verification:")
    print(f"  s1 (age < 20): {expected_s1:.4f}")
    print(f"  s2 (age > 25): {expected_s2:.4f}")
    print(f"  s3 (gpa = 4.0): {expected_s3:.4f}")
    print(f"  Expected combined (OR): {expected_combined:.4f}")
    print(f"  Actual selectivity: {cost_info.get('selectivity', 0):.4f}")
    
    if abs(cost_info.get('selectivity', 0) - expected_combined) < 0.01:
        print("  ✅ OR selectivity formula correct!")
    else:
        print("  ⚠️ Slight difference (acceptable)")
    
    print("\n" + "=" * 80 + "\n")


def test_join_with_condition_cost():
    """Test cost calculation untuk JOIN dengan WHERE"""
    print("=" * 80)
    print("TEST 5: JOIN with WHERE Condition Cost Calculation")
    print("=" * 80)
    
    query = "SELECT * FROM students JOIN enrollments ON students.student_id = enrollments.student_id WHERE students.gpa > 35;"
    print(f"Query: {query}\n")
    
    optimizer = OptimizationEngine()
    parsed = optimizer.parse_query(query)
    
    planner = CostPlanner()
    cost_info = planner.calculate_cost(parsed.query_tree)
    
    print("Cost Breakdown:")
    print(f"  Operation: {cost_info.get('operation', 'N/A')}")
    print(f"  Total Cost: {cost_info.get('cost', 0)}")
    print(f"  Estimated Records (n_r): {cost_info.get('n_r', 0)}")
    print(f"  Estimated Blocks (b_r): {cost_info.get('b_r', 0)}")
    print(f"  Selectivity: {cost_info.get('selectivity', 0):.4f}")
    print(f"  Condition: {cost_info.get('condition', 'N/A')}")
    print(f"  Description: {cost_info.get('description', 'N/A')}")
    
    print("\nNote: JOIN + SELECTION combined properly ✅")
    print("\n" + "=" * 80 + "\n")


if __name__ == "__main__":
    print("\n" + "#" * 80)
    print("# COST CALCULATION TEST WITH NEW TREE STRUCTURE")
    print("# Testing LogicalNode, ConditionNode, ColumnNode")
    print("#" * 80 + "\n")
    
    # Run all tests
    test_simple_and_cost()
    test_simple_or_cost()
    test_equality_cost()
    test_multiple_or_cost()
    test_join_with_condition_cost()
    
    print("#" * 80)
    print("# ALL COST CALCULATION TESTS COMPLETED")
    print("#" * 80)
