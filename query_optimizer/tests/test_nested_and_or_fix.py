"""
Test untuk verifikasi nested AND/OR handling di cost.py

Bug yang di-fix:
- (A AND B) OR (C AND D) seharusnya computed as:
  s_left = s_A * s_B (AND)
  s_right = s_C * s_D (AND)
  s_total = 1 - (1-s_left)*(1-s_right) (OR)

Bukan: 1 - (1-s_A)*(1-s_B)*(1-s_C)*(1-s_D) (OR of all 4)
"""

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from QueryOptimizer import OptimizationEngine
from helper.cost import CostPlanner

def test_nested_and_or():
    """Test: (age > 20 AND gpa > 30) OR (credits > 3 AND budget > 100000)"""
    print("="*80)
    print("TEST: Nested AND/OR Selectivity")
    print("="*80)
    print()
    
    query = """
        SELECT * FROM students 
        WHERE (age > 20 AND gpa > 30) OR (credits > 15 AND major = 'CS');
    """
    
    print(f"Query: {' '.join(query.split())}")
    print()
    
    optimizer = OptimizationEngine()
    parsed = optimizer.parse_query(query)
    
    planner = CostPlanner()
    cost_info = planner.calculate_cost(parsed.query_tree)
    
    print("COST BREAKDOWN:")
    print("-"*80)
    print(f"Operation: {cost_info.get('operation', 'N/A')}")
    print(f"Condition: {cost_info.get('condition', 'N/A')}")
    print(f"Selectivity: {cost_info.get('selectivity', 0):.4f}")
    print(f"Estimated Records: {cost_info.get('n_r', 0)}")
    print()
    
    print("MANUAL CALCULATION:")
    print("-"*80)
    base_stats = planner.get_table_stats("students")
    print(f"Base n_r: {base_stats['n_r']}")
    print(f"Base V(A,r): {base_stats['v_a_r']}")
    print()
    
    # Manual calculation
    print("Left branch (age > 20 AND gpa > 30):")
    s_age = 0.5  # comparison
    s_gpa = 0.5  # comparison
    s_left = s_age * s_gpa
    print(f"  s(age > 20) = {s_age:.4f}")
    print(f"  s(gpa > 30) = {s_gpa:.4f}")
    print(f"  s_left = {s_age:.4f} * {s_gpa:.4f} = {s_left:.4f}")
    print()
    
    print("Right branch (credits > 15 AND major = 'CS'):")
    s_credits = 0.5  # comparison
    s_major = 1.0 / 20  # equality: 1/V(major,students) = 1/20 = 0.05
    s_right = s_credits * s_major
    print(f"  s(credits > 15) = {s_credits:.4f}")
    print(f"  s(major = 'CS') = 1/V(major) = 1/20 = {s_major:.4f}")
    print(f"  s_right = {s_credits:.4f} * {s_major:.4f} = {s_right:.4f}")
    print()
    
    print("Combined (OR):")
    s_total = 1 - (1 - s_left) * (1 - s_right)
    print(f"  Formula: 1 - (1-s_left)*(1-s_right)")
    print(f"         = 1 - (1-{s_left:.4f})*(1-{s_right:.4f})")
    print(f"         = 1 - ({1-s_left:.4f})*({1-s_right:.4f})")
    print(f"         = 1 - {(1-s_left)*(1-s_right):.4f}")
    print(f"         = {s_total:.4f}")
    print()
    
    print("VERIFICATION:")
    print("-"*80)
    actual = cost_info.get('selectivity', 0)
    print(f"Expected selectivity: {s_total:.4f}")
    print(f"Actual selectivity:   {actual:.4f}")
    print(f"Difference:           {abs(s_total - actual):.6f}")
    
    if abs(s_total - actual) < 0.001:
        print("✅ PASS: Selectivity matches expected value!")
    else:
        print("❌ FAIL: Selectivity does NOT match!")
        print()
        print("If FAIL, it means nested AND/OR not handled correctly.")
        print("The bug would compute: 1 - (1-s_age)*(1-s_gpa)*(1-s_credits)*(1-s_major)")
        s_wrong = 1 - (1-s_age)*(1-s_gpa)*(1-s_credits)*(1-s_major)
        print(f"Wrong formula would give: {s_wrong:.4f}")
    
    print()
    print("="*80)
    print()


def test_simple_and():
    """Test baseline: Simple AND without nesting"""
    print("="*80)
    print("TEST BASELINE: Simple AND (age > 20 AND gpa > 30)")
    print("="*80)
    print()
    
    query = "SELECT * FROM students WHERE age > 20 AND gpa > 30;"
    print(f"Query: {query}")
    print()
    
    optimizer = OptimizationEngine()
    parsed = optimizer.parse_query(query)
    
    planner = CostPlanner()
    cost_info = planner.calculate_cost(parsed.query_tree)
    
    s_age = 0.5
    s_gpa = 0.5
    expected = s_age * s_gpa
    actual = cost_info.get('selectivity', 0)
    
    print(f"Expected: {s_age:.4f} * {s_gpa:.4f} = {expected:.4f}")
    print(f"Actual:   {actual:.4f}")
    
    if abs(expected - actual) < 0.001:
        print("✅ PASS")
    else:
        print("❌ FAIL")
    
    print()
    print("="*80)
    print()


def test_simple_or():
    """Test baseline: Simple OR without nesting"""
    print("="*80)
    print("TEST BASELINE: Simple OR (age > 20 OR gpa > 30)")
    print("="*80)
    print()
    
    query = "SELECT * FROM students WHERE age > 20 OR gpa > 30;"
    print(f"Query: {query}")
    print()
    
    optimizer = OptimizationEngine()
    parsed = optimizer.parse_query(query)
    
    planner = CostPlanner()
    cost_info = planner.calculate_cost(parsed.query_tree)
    
    s_age = 0.5
    s_gpa = 0.5
    expected = 1 - (1-s_age)*(1-s_gpa)
    actual = cost_info.get('selectivity', 0)
    
    print(f"Expected: 1 - (1-{s_age:.4f})*(1-{s_gpa:.4f}) = {expected:.4f}")
    print(f"Actual:   {actual:.4f}")
    
    if abs(expected - actual) < 0.001:
        print("✅ PASS")
    else:
        print("❌ FAIL")
    
    print()
    print("="*80)
    print()


def test_triple_and():
    """Test: Three AND conditions"""
    print("="*80)
    print("TEST: Triple AND (age > 20 AND gpa > 30 AND major = 'CS')")
    print("="*80)
    print()
    
    query = "SELECT * FROM students WHERE age > 20 AND gpa > 30 AND major = 'CS';"
    print(f"Query: {query}")
    print()
    
    optimizer = OptimizationEngine()
    parsed = optimizer.parse_query(query)
    
    planner = CostPlanner()
    cost_info = planner.calculate_cost(parsed.query_tree)
    
    s_age = 0.5
    s_gpa = 0.5
    s_major = 1.0 / 20
    expected = s_age * s_gpa * s_major
    actual = cost_info.get('selectivity', 0)
    
    print(f"Expected: {s_age:.4f} * {s_gpa:.4f} * {s_major:.4f} = {expected:.4f}")
    print(f"Actual:   {actual:.4f}")
    
    if abs(expected - actual) < 0.001:
        print("✅ PASS")
    else:
        print("❌ FAIL")
    
    print()
    print("="*80)
    print()


if __name__ == "__main__":
    print()
    print("#"*80)
    print("# NESTED AND/OR SELECTIVITY TEST")
    print("# Verifying fix for nested logical operators")
    print("#"*80)
    print()
    
    # Baseline tests
    test_simple_and()
    test_simple_or()
    test_triple_and()
    
    # The critical test
    test_nested_and_or()
    
    print("#"*80)
    print("# TESTS COMPLETED")
    print("#"*80)
