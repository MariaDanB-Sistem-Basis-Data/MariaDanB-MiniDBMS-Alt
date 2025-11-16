"""
Test untuk OR query dengan V(A,r) support
"""

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from QueryOptimizer import OptimizationEngine
from helper.cost import CostPlanner

def test_or_query_with_v_a_r():
    """Test OR query dengan V(A,r) support"""
    print("="*80)
    print("TEST: OR Query dengan V(A,r) Support")
    print("="*80)
    print()
    
    # Query dengan multiple OR
    query = "SELECT * FROM students WHERE gpa > 3.5 OR gpa < 2.0 OR age > 30;"
    
    print(f"Query: {query}")
    print()
    
    # Parse query
    optimizer = OptimizationEngine()
    parsed = optimizer.parse_query(query)
    
    # Create cost planner
    planner = CostPlanner()
    
    # Get cost
    cost_info = planner.calculate_cost(parsed.query_tree)
    
    print("-"*80)
    print("COST BREAKDOWN")
    print("-"*80)
    print(f"Operation: {cost_info.get('operation', 'N/A')}")
    print(f"Total Cost: {cost_info.get('cost', 0)}")
    print(f"Estimated Records (n_r): {cost_info.get('n_r', 0)}")
    print(f"Estimated Blocks (b_r): {cost_info.get('b_r', 0)}")
    print(f"Blocking Factor (f_r): {cost_info.get('f_r', 0)}")
    print(f"Selectivity: {cost_info.get('selectivity', 0):.4f}")
    print()
    
    if 'conditions' in cost_info:
        print("Conditions:")
        for i, cond in enumerate(cost_info['conditions'], 1):
            print(f"  {i}. {cond}")
        print()
    
    if 'v_a_r' in cost_info:
        print("V(A,r) - Distinct Values:")
        for attr, count in cost_info['v_a_r'].items():
            print(f"  {attr}: {count}")
        print()
    
    print(f"Description: {cost_info.get('description', 'N/A')}")
    print()
    
    # Calculate selectivity manually untuk verify
    print("-"*80)
    print("SELECTIVITY VERIFICATION")
    print("-"*80)
    
    # Get base table stats
    base_stats = planner.get_table_stats("students")
    print(f"Base table: students")
    print(f"  n_r: {base_stats['n_r']}")
    print(f"  b_r: {base_stats['b_r']}")
    print(f"  V(A,r): {base_stats['v_a_r']}")
    print()
    
    # Individual selectivities
    conditions = ["gpa > 3.5", "gpa < 2.0", "age > 30"]
    print("Individual Selectivities:")
    for cond in conditions:
        s = planner.estimate_selectivity(cond, base_stats['v_a_r'])
        print(f"  {cond}: {s:.4f}")
    print()
    
    # Combined selectivity (disjunction)
    combined = planner.estimate_disjunction_selectivity(conditions, base_stats['v_a_r'])
    print(f"Combined Selectivity (OR): {combined:.4f}")
    print(f"Formula: 1 - (1-s1)*(1-s2)*(1-s3)")
    print(f"         = 1 - (1-{planner.estimate_selectivity(conditions[0], base_stats['v_a_r']):.4f})*" + 
          f"(1-{planner.estimate_selectivity(conditions[1], base_stats['v_a_r']):.4f})*" +
          f"(1-{planner.estimate_selectivity(conditions[2], base_stats['v_a_r']):.4f})")
    print(f"         = {combined:.4f}")
    print()
    
    # Expected output
    expected_n_r = int(base_stats['n_r'] * combined)
    print(f"Expected n_r: {base_stats['n_r']} * {combined:.4f} = {expected_n_r}")
    print(f"Actual n_r: {cost_info.get('n_r', 0)}")
    print()
    
    print("="*80)


def test_single_equality_with_v_a_r():
    """Test single equality condition dengan V(A,r)"""
    print()
    print("="*80)
    print("TEST: Single Equality dengan V(A,r)")
    print("="*80)
    print()
    
    query = "SELECT * FROM students WHERE gpa = 3.5;"
    print(f"Query: {query}")
    print()
    
    optimizer = OptimizationEngine()
    parsed = optimizer.parse_query(query)
    
    planner = CostPlanner()
    cost_info = planner.calculate_cost(parsed.query_tree)
    
    base_stats = planner.get_table_stats("students")
    
    print(f"Base table stats:")
    print(f"  n_r: {base_stats['n_r']}")
    print(f"  V(gpa, students): {base_stats['v_a_r'].get('gpa', 'N/A')}")
    print()
    
    # Expected selectivity for equality
    v_gpa = base_stats['v_a_r'].get('gpa', 1)
    expected_selectivity = 1.0 / v_gpa
    print(f"Expected selectivity: 1 / V(gpa) = 1 / {v_gpa} = {expected_selectivity:.4f}")
    print(f"Actual selectivity: {cost_info.get('selectivity', 0):.4f}")
    print()
    
    expected_n_r = int(base_stats['n_r'] * expected_selectivity)
    print(f"Expected n_r: {base_stats['n_r']} * {expected_selectivity:.4f} = {expected_n_r}")
    print(f"Actual n_r: {cost_info.get('n_r', 0)}")
    print()
    
    print("="*80)


def test_conjunction_with_v_a_r():
    """Test conjunction (AND) dengan V(A,r)"""
    print()
    print("="*80)
    print("TEST: Conjunction (AND) dengan V(A,r)")
    print("="*80)
    print()
    
    query = "SELECT * FROM students WHERE age > 18 AND gpa > 3.0;"
    print(f"Query: {query}")
    print()
    
    optimizer = OptimizationEngine()
    parsed = optimizer.parse_query(query)
    
    planner = CostPlanner()
    
    # Manual calculation
    base_stats = planner.get_table_stats("students")
    conditions = ["age > 18", "gpa > 3.0"]
    
    print("Individual selectivities:")
    for cond in conditions:
        s = planner.estimate_selectivity(cond, base_stats['v_a_r'])
        print(f"  {cond}: {s:.4f}")
    print()
    
    combined = planner.estimate_conjunction_selectivity(conditions, base_stats['v_a_r'])
    print(f"Combined selectivity (AND): {combined:.4f}")
    print(f"Formula: s1 * s2 = {planner.estimate_selectivity(conditions[0], base_stats['v_a_r']):.4f} * " +
          f"{planner.estimate_selectivity(conditions[1], base_stats['v_a_r']):.4f} = {combined:.4f}")
    print()
    
    # Now calculate full cost
    cost_info = planner.calculate_cost(parsed.query_tree)
    
    print(f"Estimated n_r: {cost_info.get('n_r', 0)}")
    print(f"Estimated b_r: {cost_info.get('b_r', 0)}")
    print(f"Total cost: {cost_info.get('cost', 0)}")
    print()
    
    print("="*80)


if __name__ == "__main__":
    test_or_query_with_v_a_r()
    test_single_equality_with_v_a_r()
    test_conjunction_with_v_a_r()
    
    print("\n" + "="*80)
    print("ALL TESTS COMPLETED")
    print("="*80)
