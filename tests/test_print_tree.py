"""
Test untuk print tree structure dengan detail lengkap
"""

import sys
import os

# Tambahkan parent directory ke sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from QueryOptimizer import OptimizationEngine
from model.query_tree import QueryTree


def print_tree(node: QueryTree, indent: int = 0, prefix: str = ""):
    """
    Print query tree dengan format yang mudah dibaca.
    Menampilkan type, value, dan struktur hierarki.
    
    Args:
        node: QueryTree node untuk di-print
        indent: Level indentasi (untuk recursive calls)
        prefix: Prefix untuk menunjukkan struktur tree
    """
    if node is None:
        print(f"{' ' * indent}{prefix}None")
        return
    
    # Print current node
    indent_str = ' ' * indent
    node_info = f"{prefix}Node(type='{node.type}', val='{node.val}')"
    
    # Tambahkan info jumlah children
    if node.childs:
        node_info += f" [{len(node.childs)} child(ren)]"
    else:
        node_info += " [leaf]"
    
    print(f"{indent_str}{node_info}")
    
    # Print children
    if node.childs:
        for i, child in enumerate(node.childs):
            is_last = (i == len(node.childs) - 1)
            
            if is_last:
                child_prefix = "└── "
                next_indent = indent + 4
                next_prefix = "    "
            else:
                child_prefix = "├── "
                next_indent = indent + 4
                next_prefix = "│   "
            
            print_tree(child, next_indent, child_prefix)


def print_tree_detailed(node: QueryTree, level: int = 0):
    """
    Print tree dengan detail lengkap dalam format tabel.
    
    Args:
        node: QueryTree node
        level: Level kedalaman tree
    """
    if node is None:
        return
    
    # Print current node dengan indentasi
    indent = "  " * level
    print(f"{indent}Level {level}:")
    print(f"{indent}  ├─ Type  : {node.type}")
    print(f"{indent}  ├─ Value : {node.val if node.val else '(empty)'}")
    print(f"{indent}  ├─ Parent: {node.parent.type if node.parent else 'None (root)'}")
    print(f"{indent}  └─ Children: {len(node.childs)} node(s)")
    
    # Recursively print children
    if node.childs:
        print(f"{indent}     └─ Children details:")
        for i, child in enumerate(node.childs):
            print(f"{indent}        Child #{i+1}:")
            print_tree_detailed(child, level + 2)
    
    if level == 0:
        print()


def print_tree_simple(node: QueryTree, indent: int = 0):
    """
    Print tree dalam format simple dan compact.
    """
    if node is None:
        return
    
    print("  " * indent + f"[{node.type}] {node.val if node.val else '(no value)'}")
    
    for child in node.childs:
        print_tree_simple(child, indent + 1)


def test_select_with_where_multiple_or():
    """
    Test case dari UnitTestingParseQuery.py
    Query: SELECT * FROM students WHERE gpa > 3.5 OR gpa < 2.0 OR age > 30;
    """
    print("=" * 80)
    print("TEST: SELECT with WHERE multiple OR")
    print("=" * 80)
    print()
    
    optimizer = OptimizationEngine()
    query = "SELECT * FROM students WHERE gpa > 3.5 OR gpa < 2.0 OR age > 30;"
    
    print(f"Query: {query}")
    print()
    
    parsed = optimizer.parse_query(query)
    
    print("-" * 80)
    print("TREE STRUCTURE (Hierarchical View)")
    print("-" * 80)
    print_tree(parsed.query_tree)
    
    print()
    print("-" * 80)
    print("TREE STRUCTURE (Simple View)")
    print("-" * 80)
    print_tree_simple(parsed.query_tree)
    
    print()
    print("-" * 80)
    print("TREE STRUCTURE (Detailed View)")
    print("-" * 80)
    print_tree_detailed(parsed.query_tree)
    
    print()
    print("=" * 80)
    print("ANALYSIS")
    print("=" * 80)
    
    # Analisis struktur
    current = parsed.query_tree
    depth = 0
    
    print(f"Root node type: {current.type}")
    print(f"Root node value: {current.val if current.val else '(empty)'}")
    print()
    
    # Navigate to OR node
    while current and current.type != "OR" and current.childs:
        print(f"Depth {depth}: {current.type} -> {current.val if current.val else '(empty)'}")
        current = current.childs[0] if current.childs else None
        depth += 1
    
    if current and current.type == "OR":
        print(f"\nFound OR node at depth {depth}")
        print(f"OR node has {len(current.childs)} children:")
        print()
        
        for i, child in enumerate(current.childs):
            print(f"  Child #{i+1}:")
            print(f"    Type  : {child.type}")
            print(f"    Value : {child.val}")
            
            if child.childs:
                print(f"    Has {len(child.childs)} child(ren)")
                for j, grandchild in enumerate(child.childs):
                    print(f"      Grandchild #{j+1}: {grandchild.type} - {grandchild.val if grandchild.val else '(empty)'}")
    
    print()
    print("=" * 80)


def test_other_queries():
    """
    Test beberapa query lain untuk perbandingan
    """
    optimizer = OptimizationEngine()
    
    test_cases = [
        {
            "name": "Simple SELECT with single WHERE",
            "query": "SELECT * FROM students WHERE age > 18;"
        },
        {
            "name": "SELECT with WHERE AND",
            "query": "SELECT * FROM students WHERE age > 18 AND gpa > 3.0;"
        },
        {
            "name": "SELECT with JOIN",
            "query": "SELECT * FROM students JOIN courses ON students.course_id = courses.id;"
        },
        {
            "name": "SELECT with ORDER BY and LIMIT",
            "query": "SELECT name FROM students ORDER BY age DESC LIMIT 10;"
        }
    ]
    
    for test_case in test_cases:
        print()
        print("=" * 80)
        print(f"TEST: {test_case['name']}")
        print("=" * 80)
        print(f"Query: {test_case['query']}")
        print()
        
        parsed = optimizer.parse_query(test_case['query'])
        print_tree(parsed.query_tree)
        print()


if __name__ == "__main__":
    # Test utama: query dengan multiple OR
    test_select_with_where_multiple_or()
    
    # Test query lainnya untuk perbandingan
    print("\n\n")
    print("#" * 80)
    print("# ADDITIONAL TESTS - Other Query Structures")
    print("#" * 80)
    test_other_queries()
