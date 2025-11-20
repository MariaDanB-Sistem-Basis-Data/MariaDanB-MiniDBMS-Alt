"""
Test untuk memahami struktur tree yang kompleks dengan multiple JOINs dan SELECTIONs.

Scenario: Query dengan 3 JOINs dan beberapa WHERE conditions
"""

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from QueryOptimizer import OptimizationEngine
from model.query_tree import QueryTree, LogicalNode, ConditionNode, ColumnNode

def print_tree_recursive(node, indent=0, label="", show_line=True):
    """
    Print query tree secara recursive dengan format yang jelas.
    Tambah visual guides untuk easier reading.
    """
    prefix = "  " * indent
    
    if show_line and indent > 0:
        line_prefix = "  " * (indent - 1) + "|-- "
    else:
        line_prefix = prefix
    
    if isinstance(node, QueryTree):
        print(f"{line_prefix}{label}QueryTree(")
        print(f"{prefix}  type = '{node.type}'")
        
        # Print value
        if node.val is not None:
            print(f"{prefix}  val = ", end="")
            if isinstance(node.val, (LogicalNode, ConditionNode, ColumnNode)):
                print()
                print_tree_recursive(node.val, indent + 2, "", False)
            else:
                print(f"{node.val}")
        else:
            print(f"{prefix}  val = None")
        
        # Print children
        if node.childs:
            print(f"{prefix}  childs = [")
            for i, child in enumerate(node.childs):
                is_last = (i == len(node.childs) - 1)
                child_prefix = "`-- " if is_last else "|-- "
                print_tree_recursive(child, indent + 2, f"[{i}] ", True)
            print(f"{prefix}  ]")
        else:
            print(f"{prefix}  childs = []")
        
        print(f"{prefix})")
    
    elif isinstance(node, LogicalNode):
        print(f"{line_prefix}{label}LogicalNode(")
        print(f"{prefix}  operator = '{node.operator}'")
        print(f"{prefix}  childs = [")
        for i, child in enumerate(node.childs):
            print_tree_recursive(child, indent + 2, f"[{i}] ", True)
        print(f"{prefix}  ]")
        print(f"{prefix})")
    
    elif isinstance(node, ConditionNode):
        print(f"{line_prefix}{label}ConditionNode(")
        print(f"{prefix}  attr = ", end="")
        if isinstance(node.attr, ColumnNode):
            print()
            print_tree_recursive(node.attr, indent + 2, "", False)
        else:
            print(f"{node.attr}")
        print(f"{prefix}  op = '{node.op}'")
        print(f"{prefix}  value = ", end="")
        if isinstance(node.value, ColumnNode):
            print()
            print_tree_recursive(node.value, indent + 2, "", False)
        else:
            print(f"{node.value}")
        print(f"{prefix})")
    
    elif isinstance(node, ColumnNode):
        table_str = f"table='{node.table}'" if node.table else "table=None"
        print(f"{line_prefix}{label}ColumnNode(column='{node.column}', {table_str})")
    
    else:
        print(f"{line_prefix}{label}{node}")


def print_tree_summary(node, indent=0):
    """
    Print simplified tree summary untuk quick overview.
    """
    prefix = "  " * indent
    
    if isinstance(node, QueryTree):
        val_desc = ""
        if node.val:
            if isinstance(node.val, LogicalNode):
                val_desc = f" [{node.val.operator}]"
            elif isinstance(node.val, ConditionNode):
                attr_name = node.val.attr.column if isinstance(node.val.attr, ColumnNode) else str(node.val.attr)
                val_name = node.val.value.column if isinstance(node.val.value, ColumnNode) else str(node.val.value)
                val_desc = f" [{attr_name} {node.val.op} {val_name}]"
            else:
                val_desc = f" [{node.val}]"
        
        print(f"{prefix}{node.type}{val_desc}")
        
        for child in node.childs:
            print_tree_summary(child, indent + 1)


def test_single_join():
    """Test: Simple single JOIN"""
    print("="*80)
    print("TEST 1: Single JOIN")
    print("="*80)
    print()
    
    query = """
        SELECT * 
        FROM students 
        JOIN enrollments ON students.student_id = enrollments.student_id;
    """
    
    print(f"Query: {' '.join(query.split())}")
    print()
    
    optimizer = OptimizationEngine()
    parsed = optimizer.parse_query(query)
    
    print("TREE SUMMARY:")
    print("-"*80)
    print_tree_summary(parsed.query_tree)
    print()
    
    print("DETAILED TREE STRUCTURE:")
    print("-"*80)
    print_tree_recursive(parsed.query_tree)
    print()
    print("="*80)
    print()


def test_double_join():
    """Test: Two JOINs"""
    print("="*80)
    print("TEST 2: Double JOIN (3 tables)")
    print("="*80)
    print()
    
    query = """
        SELECT * 
        FROM students 
        JOIN enrollments ON students.student_id = enrollments.student_id
        JOIN courses ON enrollments.course_id = courses.course_id;
    """
    
    print(f"Query: {' '.join(query.split())}")
    print()
    
    optimizer = OptimizationEngine()
    parsed = optimizer.parse_query(query)
    
    print("TREE SUMMARY:")
    print("-"*80)
    print_tree_summary(parsed.query_tree)
    print()
    
    print("DETAILED TREE STRUCTURE:")
    print("-"*80)
    print_tree_recursive(parsed.query_tree)
    print()
    
    print("EXPLANATION:")
    print("-"*80)
    print("Struktur: JOIN(JOIN(students, enrollments), courses)")
    print("- Bottom: students JOIN enrollments")
    print("- Top: result di-JOIN dengan courses")
    print("- Tree grows dari bottom-up!")
    print()
    print("="*80)
    print()


def test_triple_join():
    """Test: Three JOINs"""
    print("="*80)
    print("TEST 3: Triple JOIN (4 tables)")
    print("="*80)
    print()
    
    query = """
        SELECT * 
        FROM students 
        JOIN enrollments ON students.student_id = enrollments.student_id
        JOIN courses ON enrollments.course_id = courses.course_id
        JOIN departments ON courses.dept_id = departments.dept_id;
    """
    
    print(f"Query: {' '.join(query.split())}")
    print()
    
    optimizer = OptimizationEngine()
    parsed = optimizer.parse_query(query)
    
    print("TREE SUMMARY:")
    print("-"*80)
    print_tree_summary(parsed.query_tree)
    print()
    
    print("DETAILED TREE STRUCTURE:")
    print("-"*80)
    print_tree_recursive(parsed.query_tree)
    print()
    
    print("EXPLANATION:")
    print("-"*80)
    print("Struktur: JOIN(JOIN(JOIN(students, enrollments), courses), departments)")
    print()
    print("Bottom-up construction:")
    print("  Level 1: students JOIN enrollments")
    print("  Level 2: (students JOIN enrollments) JOIN courses")
    print("  Level 3: ((students JOIN enrollments) JOIN courses) JOIN departments")
    print()
    print("Tree depth = number of joins!")
    print()
    print("="*80)
    print()


def test_triple_join_with_where():
    """Test: Three JOINs + WHERE conditions"""
    print("="*80)
    print("TEST 4: Triple JOIN + WHERE (COMPLEX!)")
    print("="*80)
    print()
    
    query = """
        SELECT * 
        FROM students 
        JOIN enrollments ON students.student_id = enrollments.student_id
        JOIN courses ON enrollments.course_id = courses.course_id
        JOIN departments ON courses.dept_id = departments.dept_id
        WHERE students.age > 20 AND courses.credits > 3;
    """
    
    print(f"Query: {' '.join(query.split())}")
    print()
    
    optimizer = OptimizationEngine()
    parsed = optimizer.parse_query(query)
    
    print("TREE SUMMARY:")
    print("-"*80)
    print_tree_summary(parsed.query_tree)
    print()
    
    print("DETAILED TREE STRUCTURE:")
    print("-"*80)
    print_tree_recursive(parsed.query_tree)
    print()
    
    print("EXPLANATION:")
    print("-"*80)
    print("Top level: SIGMA (WHERE clause)")
    print("  +-- condition: students.age > 20 AND courses.credits > 3")
    print()
    print("Second level: Triple JOIN")
    print("  +-- JOIN 3: (...) JOIN departments")
    print("      +-- JOIN 2: (...) JOIN courses")
    print("          +-- JOIN 1: students JOIN enrollments")
    print()
    print("SIGMA wraps the entire JOIN tree!")
    print("WHERE filters applied AFTER all joins complete.")
    print()
    print("="*80)
    print()


def test_triple_join_complex_where():
    """Test: Triple JOIN + Complex WHERE (AND + OR)"""
    print("="*80)
    print("TEST 5: Triple JOIN + Complex WHERE (AND + OR)")
    print("="*80)
    print()
    
    query = """
        SELECT * 
        FROM students 
        JOIN enrollments ON students.student_id = enrollments.student_id
        JOIN courses ON enrollments.course_id = courses.course_id
        JOIN departments ON courses.dept_id = departments.dept_id
        WHERE (students.age > 20 AND students.gpa > 30) 
           OR (courses.credits > 3 AND departments.budget > 100000);
    """
    
    print(f"Query: {' '.join(query.split())}")
    print()
    
    optimizer = OptimizationEngine()
    parsed = optimizer.parse_query(query)
    
    print("TREE SUMMARY:")
    print("-"*80)
    print_tree_summary(parsed.query_tree)
    print()
    
    print("DETAILED TREE STRUCTURE:")
    print("-"*80)
    print_tree_recursive(parsed.query_tree)
    print()
    
    print("EXPLANATION:")
    print("-"*80)
    print("Top: SIGMA with LogicalNode")
    print("  +-- OR operator (top level)")
    print("      |-- AND operator (left branch)")
    print("      |   |-- students.age > 20")
    print("      |   +-- students.gpa > 30")
    print("      +-- AND operator (right branch)")
    print("          |-- courses.credits > 3")
    print("          +-- departments.budget > 100000")
    print()
    print("Bottom: Triple JOIN (same as before)")
    print()
    print("NESTED LogicalNode structure:")
    print("  LogicalNode(OR) contains LogicalNode(AND) children!")
    print()
    print("="*80)
    print()


def test_join_with_selection_pushdown():
    """Test: JOIN dengan selection pada specific table"""
    print("="*80)
    print("TEST 6: JOIN with Table-Specific WHERE")
    print("="*80)
    print()
    
    query = """
        SELECT * 
        FROM students 
        JOIN enrollments ON students.student_id = enrollments.student_id
        WHERE students.age > 20 OR studnets.gpa < 3 AND enrollments.grade = 'A';
    """
    
    print(f"Query: {' '.join(query.split())}")
    print()
    
    optimizer = OptimizationEngine()
    parsed = optimizer.parse_query(query)
    
    print("TREE SUMMARY:")
    print("-"*80)
    print_tree_summary(parsed.query_tree)
    print()
    
    print("DETAILED TREE STRUCTURE:")
    print("-"*80)
    print_tree_recursive(parsed.query_tree)
    print()
    
    print("EXPLANATION:")
    print("-"*80)
    print("SIGMA on top of JOIN")
    print("Filter: students.age > 20 (single table condition)")
    print()
    print("Note: Optimizer MIGHT push this selection down to students table")
    print("      (selection pushdown optimization)")
    print("But in current tree, it's applied AFTER join.")
    print()
    print("="*80)
    print()


def print_final_summary():
    """Print summary of all patterns"""
    print()
    print("#"*80)
    print("# SUMMARY: TREE STRUCTURE PATTERNS")
    print("#"*80)
    print()
    
    print("KEY PATTERNS:")
    print("="*80)
    print()
    
    print("1. SINGLE JOIN:")
    print("   QueryTree(JOIN)")
    print("   ├─ childs[0]: QueryTree(TABLE) - left table")
    print("   └─ childs[1]: QueryTree(TABLE) - right table")
    print()
    
    print("2. DOUBLE JOIN (3 tables):")
    print("   QueryTree(JOIN) - outer join")
    print("   |-- childs[0]: QueryTree(JOIN) - inner join")
    print("   |   |-- childs[0]: QueryTree(TABLE) - students")
    print("   |   +-- childs[1]: QueryTree(TABLE) - enrollments")
    print("   +-- childs[1]: QueryTree(TABLE) - courses")
    print()
    
    print("3. TRIPLE JOIN (4 tables):")
    print("   QueryTree(JOIN) - outermost")
    print("   |-- childs[0]: QueryTree(JOIN) - middle")
    print("   |   |-- childs[0]: QueryTree(JOIN) - innermost")
    print("   |   |   |-- childs[0]: QueryTree(TABLE) - students")
    print("   |   |   +-- childs[1]: QueryTree(TABLE) - enrollments")
    print("   |   +-- childs[1]: QueryTree(TABLE) - courses")
    print("   +-- childs[1]: QueryTree(TABLE) - departments")
    print()
    
    print("4. TRIPLE JOIN + WHERE:")
    print("   QueryTree(SIGMA) - top level")
    print("   |-- val: LogicalNode or ConditionNode")
    print("   +-- childs[0]: QueryTree(JOIN) - entire join tree")
    print("       +-- (same structure as pattern 3)")
    print()
    
    print("5. COMPLEX WHERE (nested AND/OR):")
    print("   QueryTree(SIGMA)")
    print("   |-- val: LogicalNode(OR)")
    print("   |   |-- childs[0]: LogicalNode(AND) - nested!")
    print("   |   |   |-- ConditionNode")
    print("   |   |   +-- ConditionNode")
    print("   |   +-- childs[1]: LogicalNode(AND) - nested!")
    print("   |      |-- ConditionNode")
    print("   |      +-- ConditionNode")
    print("   +-- childs[0]: QueryTree(JOIN)")
    print()
    
    print("="*80)
    print()
    
    print("IMPORTANT OBSERVATIONS:")
    print("="*80)
    print("• JOINs build LEFT-ASSOCIATIVE: ((A JOIN B) JOIN C) JOIN D")
    print("• Each JOIN has EXACTLY 2 children (binary operator)")
    print("• Tree DEPTH = number of JOINs")
    print("• SIGMA (WHERE) always wraps the JOIN tree at top")
    print("• LogicalNode can be NESTED (for complex conditions)")
    print("• ConditionNode.value can be ColumnNode (join) or literal (filter)")
    print()
    
    print("#"*80)
    print()


if __name__ == "__main__":
    print()
    print("#"*80)
    print("# COMPLEX MULTI-JOIN TREE STRUCTURE TEST")
    print("# Understanding how multiple JOINs and SELECTIONs form a tree")
    print("#"*80)
    print()
    
    test_single_join()
    test_double_join()
    test_triple_join()
    test_triple_join_with_where()
    test_triple_join_complex_where()
    test_join_with_selection_pushdown()
    
    print_final_summary()
    
    print("#"*80)
    print("# ALL COMPLEX TESTS COMPLETED")
    print("#"*80)
