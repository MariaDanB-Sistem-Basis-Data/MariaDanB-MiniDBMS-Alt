"""
Test untuk V(A,r) estimation pada temporary tables (hasil selection dan join).

Berdasarkan slides "Estimation of Number of Distinct Values":
1. Selection: min(V(A,r), n·σ_θ(r))
2. Join: 
   - If all attributes in A are from r: min(V(A,r), n_r⋈s)
   - If A contains attributes from both r and s: more complex formula
"""

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from QueryOptimizer import OptimizationEngine
from helper.cost import CostPlanner

def print_v_a_r_comparison(label: str, base_v_a_r: dict, result_v_a_r: dict, output_n_r: int):
    """Helper untuk print comparison V(A,r) values"""
    print(f"\n{label}")
    print("-" * 70)
    print(f"{'Attribute':<15} {'Base V(A,r)':<15} {'Result V(A,r)':<18} {'Expected (min)':<15}")
    print("-" * 70)
    for attr in base_v_a_r:
        base_v = base_v_a_r[attr]
        result_v = result_v_a_r.get(attr, 'N/A')
        expected = min(base_v, output_n_r)
        match = "✅" if result_v == expected else "❌"
        print(f"{attr:<15} {base_v:<15} {result_v!s:<18} {expected:<15} {match}")
    print("-" * 70)


def test_selection_v_a_r():
    """
    Test V(A,r) estimation untuk SELECTION.
    
    Formula dari slide:
    "In all the other cases: use approximate estimate of min(V(A,r), n·σ_θ(r))"
    """
    print("=" * 80)
    print("TEST 1: V(A,r) Estimation untuk SELECTION")
    print("=" * 80)
    
    # Query dengan selection yang sangat selective
    query = "SELECT * FROM students WHERE gpa > 3.8;"
    
    print(f"Query: {query}\n")
    
    optimizer = OptimizationEngine()
    parsed = optimizer.parse_query(query)
    
    planner = CostPlanner()
    
    # Get base table stats
    base_stats = planner.get_table_stats("students")
    print("Base Table Stats (students):")
    print(f"  n_r: {base_stats['n_r']}")
    print(f"  b_r: {base_stats['b_r']}")
    print(f"  V(A,r): {base_stats['v_a_r']}")
    
    # Calculate cost (which includes V(A,r) estimation)
    cost_info = planner.calculate_cost(parsed.query_tree)
    
    print(f"\nSelection Result:")
    print(f"  Selectivity: {cost_info.get('selectivity', 0):.4f}")
    print(f"  Output n_r: {cost_info.get('n_r', 0)}")
    print(f"  Output b_r: {cost_info.get('b_r', 0)}")
    
    # Compare V(A,r)
    print_v_a_r_comparison(
        "V(A,r) Comparison:",
        base_stats['v_a_r'],
        cost_info.get('v_a_r', {}),
        cost_info.get('n_r', 0)
    )
    
    # Verify formula
    print("\nFormula Verification:")
    print("Expected: V(A, σ_θ(r)) = min(V(A,r), n_r(σ_θ(r)))")
    for attr, base_v in base_stats['v_a_r'].items():
        result_v = cost_info['v_a_r'].get(attr, 0)
        expected_v = min(base_v, cost_info['n_r'])
        status = "✅ CORRECT" if result_v == expected_v else "❌ WRONG"
        print(f"  {attr}: min({base_v}, {cost_info['n_r']}) = {expected_v}, got {result_v} {status}")
    
    print("\n" + "=" * 80)


def test_join_v_a_r():
    """
    Test V(A,r) estimation untuk JOIN.
    
    Formula dari slide:
    "If all attributes in A are from r: estimated V(A, r ⋈ s) = min(V(A,r), n_r⋈s)"
    """
    print("\n" + "=" * 80)
    print("TEST 2: V(A,r) Estimation untuk JOIN")
    print("=" * 80)
    
    # Query dengan join
    query = "SELECT * FROM students JOIN enrollments ON students.student_id = enrollments.student_id;"
    
    print(f"Query: {query}\n")
    
    optimizer = OptimizationEngine()
    parsed = optimizer.parse_query(query)
    
    planner = CostPlanner()
    
    # Get base table stats
    students_stats = planner.get_table_stats("students")
    enrollments_stats = planner.get_table_stats("enrollments")
    
    print("Base Table Stats:")
    print(f"\nStudents (R):")
    print(f"  n_r: {students_stats['n_r']}")
    print(f"  V(A,r): {students_stats['v_a_r']}")
    
    print(f"\nEnrollments (S):")
    print(f"  n_r: {enrollments_stats['n_r']}")
    print(f"  V(A,s): {enrollments_stats['v_a_r']}")
    
    # Calculate cost
    cost_info = planner.calculate_cost(parsed.query_tree)
    
    print(f"\nJoin Result:")
    print(f"  Output n_r: {cost_info.get('n_r', 0)}")
    print(f"  Output b_r: {cost_info.get('b_r', 0)}")
    print(f"  Join Cost: {cost_info.get('join_cost', 0)}")
    
    # Compare V(A,r) for attributes from students
    print("\n" + "=" * 70)
    print("V(A,r) for Attributes from STUDENTS (R):")
    print("=" * 70)
    print(f"{'Attribute':<15} {'V(A,R)':<12} {'V(A,R⋈S)':<12} {'Expected':<12} {'Status':<10}")
    print("-" * 70)
    
    join_n_r = cost_info.get('n_r', 0)
    join_v_a_r = cost_info.get('v_a_r', {})
    
    for attr in students_stats['v_a_r']:
        v_a_r = students_stats['v_a_r'][attr]
        v_a_join = join_v_a_r.get(attr, 'N/A')
        expected = min(v_a_r, join_n_r)
        status = "✅" if v_a_join == expected else "❌"
        print(f"{attr:<15} {v_a_r:<12} {v_a_join!s:<12} {expected:<12} {status}")
    
    # Compare V(A,r) for attributes from enrollments
    print("\n" + "=" * 70)
    print("V(A,r) for Attributes from ENROLLMENTS (S):")
    print("=" * 70)
    print(f"{'Attribute':<18} {'V(A,S)':<12} {'V(A,R⋈S)':<12} {'Expected':<12} {'Status':<10}")
    print("-" * 70)
    
    for attr in enrollments_stats['v_a_r']:
        v_a_s = enrollments_stats['v_a_r'][attr]
        v_a_join = join_v_a_r.get(attr, 'N/A')
        
        if attr in students_stats['v_a_r']:
            # Common attribute (join key)
            v_a_r = students_stats['v_a_r'][attr]
            expected = min(v_a_r, v_a_s, join_n_r)
            print(f"{attr:<18} {v_a_s:<12} {v_a_join!s:<12} {expected:<12} {'✅' if v_a_join == expected else '❌'}")
        else:
            # Attribute only from S
            expected = min(v_a_s, join_n_r)
            print(f"{attr:<18} {v_a_s:<12} {v_a_join!s:<12} {expected:<12} {'✅' if v_a_join == expected else '❌'}")
    
    print("\nFormula Used:")
    print("For attributes from R: V(A, r⋈s) = min(V(A,r), n_r⋈s)")
    print("For attributes from S: V(A, r⋈s) = min(V(A,s), n_r⋈s)")
    print("For common attributes: V(A, r⋈s) = min(V(A,r), V(A,s), n_r⋈s)")
    
    print("\n" + "=" * 80)


def test_chained_operations_v_a_r():
    """
    Test V(A,r) estimation untuk operasi berantai: JOIN -> SELECTION.
    
    Verify bahwa V(A,r) dipropagasi dengan benar melalui multiple operations.
    """
    print("\n" + "=" * 80)
    print("TEST 3: V(A,r) Estimation untuk Chained Operations (JOIN + SELECTION)")
    print("=" * 80)
    
    # Query dengan join dan selection
    query = "SELECT * FROM students JOIN enrollments ON students.student_id = enrollments.student_id WHERE students.gpa > 3.5;"
    
    print(f"Query: {query}\n")
    
    optimizer = OptimizationEngine()
    parsed = optimizer.parse_query(query)
    
    planner = CostPlanner()
    cost_info = planner.calculate_cost(parsed.query_tree)
    
    print("Final Result:")
    print(f"  Output n_r: {cost_info.get('n_r', 0)}")
    print(f"  Output b_r: {cost_info.get('b_r', 0)}")
    print(f"  Total Cost: {cost_info.get('cost', 0)}")
    
    print("\nV(A,r) in Final Result:")
    print("-" * 50)
    final_v_a_r = cost_info.get('v_a_r', {})
    for attr, v_val in final_v_a_r.items():
        print(f"  {attr}: {v_val}")
    
    print("\nObservation:")
    print("✅ V(A,r) should be bounded by output n_r at each stage")
    print(f"✅ All V(A,r) values <= {cost_info.get('n_r', 0)} (output n_r)")
    
    # Verify
    all_valid = all(v <= cost_info.get('n_r', float('inf')) for v in final_v_a_r.values())
    if all_valid:
        print("✅ All V(A,r) values are correctly bounded!")
    else:
        print("❌ Some V(A,r) values exceed output n_r!")
    
    print("\n" + "=" * 80)


def test_highly_selective_query():
    """
    Test dengan query yang sangat selective (small output).
    
    Verify V(A,r) tidak melebihi jumlah output tuples.
    """
    print("\n" + "=" * 80)
    print("TEST 4: V(A,r) dengan Highly Selective Query")
    print("=" * 80)
    
    # Query yang sangat selective
    query = "SELECT * FROM students WHERE gpa = 4.0 AND age = 22;"
    
    print(f"Query: {query}\n")
    
    optimizer = OptimizationEngine()
    parsed = optimizer.parse_query(query)
    
    planner = CostPlanner()
    
    # Base stats
    base_stats = planner.get_table_stats("students")
    print("Base Table (students):")
    print(f"  n_r: {base_stats['n_r']}")
    print(f"  V(gpa, students): {base_stats['v_a_r']['gpa']}")
    print(f"  V(age, students): {base_stats['v_a_r']['age']}")
    
    # Calculate
    cost_info = planner.calculate_cost(parsed.query_tree)
    
    print(f"\nQuery Result:")
    print(f"  Output n_r: {cost_info.get('n_r', 0)}")
    print(f"  Selectivity: {cost_info.get('selectivity', 0):.6f}")
    
    print(f"\nV(A,r) in Result:")
    result_v_a_r = cost_info.get('v_a_r', {})
    for attr, v_val in result_v_a_r.items():
        print(f"  V({attr}, result): {v_val}")
    
    # Critical check
    print("\nCritical Check:")
    print(f"Rule: V(A,r) cannot exceed n_r(output) = {cost_info.get('n_r', 0)}")
    print("-" * 50)
    
    violations = []
    for attr, v_val in result_v_a_r.items():
        exceeds = v_val > cost_info.get('n_r', 0)
        status = "❌ VIOLATION" if exceeds else "✅ OK"
        print(f"  V({attr}): {v_val} {status}")
        if exceeds:
            violations.append(attr)
    
    if violations:
        print(f"\n❌ FAILED: {len(violations)} attribute(s) violate the rule!")
    else:
        print(f"\n✅ PASSED: All V(A,r) values are valid!")
    
    print("\n" + "=" * 80)


if __name__ == "__main__":
    print("\n" + "#" * 80)
    print("# V(A,r) ESTIMATION TEST SUITE")
    print("#" * 80)
    print("\nFormulas from Database System Concepts slides:")
    print("1. Selection: V(A, σ_θ(r)) = min(V(A,r), n·σ_θ(r))")
    print("2. Join: V(A, r⋈s) = min(V(A,r), n_r⋈s) for attributes from r")
    print("3. Join: V(A, r⋈s) = min(V(A,s), n_r⋈s) for attributes from s")
    print("#" * 80)
    
    # Run all tests
    test_selection_v_a_r()
    test_join_v_a_r()
    test_chained_operations_v_a_r()
    test_highly_selective_query()
    
    print("\n" + "#" * 80)
    print("# ALL V(A,r) ESTIMATION TESTS COMPLETED")
    print("#" * 80)
