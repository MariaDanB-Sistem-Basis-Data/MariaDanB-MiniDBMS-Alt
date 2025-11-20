"""
CARA RUN
cd tests; python -c "import sys; sys.path.insert(0, '..'); from test_tree_comparison import *; test_tree_comparison()"

Test untuk membandingkan cost dari tree yang berbeda untuk query yang sama.

Skenario: Query dengan 3 JOIN dan WHERE conditions
Query logis: SELECT * FROM A, B, C WHERE A.x = B.x AND B.y = C.y AND A.z > 10 AND C.w = 'value'

Tree berbeda:
1. Tree 1: Selection dulu sebelum JOIN (push selection down)
   - σ_A.z>10(A) ⋈ B ⋈ σ_C.w='value'(C)
   
2. Tree 2: JOIN semua dulu, baru selection
   - σ_(A.z>10 AND C.w='value')(A ⋈ B ⋈ C)
   
3. Tree 3: Mixed - selection sebagian, join, selection lagi
   - σ_C.w='value'(A ⋈ σ_A.z>10(A ⋈ B) ⋈ C)
"""

from helper.cost import CostPlanner
from model.query_tree import QueryTree, ConditionNode, LogicalNode, ColumnNode
from model.parsed_query import ParsedQuery

def print_separator():
    print("=" * 100)

def print_tree_structure(tree, indent=0):
    """Helper untuk print tree structure"""
    prefix = "  " * indent
    if tree.type == "TABLE":
        print(f"{prefix}TABLE: {tree.val}")
    elif tree.type == "SIGMA":
        if isinstance(tree.val, ConditionNode):
            attr = tree.val.attr.column if isinstance(tree.val.attr, ColumnNode) else str(tree.val.attr)
            print(f"{prefix}SELECTION: {attr} {tree.val.op} {tree.val.value}")
        elif isinstance(tree.val, LogicalNode):
            print(f"{prefix}SELECTION: LogicalNode({tree.val.operator})")
        else:
            print(f"{prefix}SELECTION: {tree.val}")
        for child in tree.childs:
            print_tree_structure(child, indent + 1)
    elif tree.type == "JOIN":
        print(f"{prefix}JOIN")
        for child in tree.childs:
            print_tree_structure(child, indent + 1)
    else:
        print(f"{prefix}{tree.type}")
        for child in tree.childs:
            print_tree_structure(child, indent + 1)

def create_tree_1_selection_first():
    """
    Tree 1: Selection push-down (optimal)
    Structure:
        JOIN
        ├── JOIN
        │   ├── SIGMA (A.z > 10)
        │   │   └── TABLE A
        │   └── TABLE B
        └── SIGMA (C.w = 'value')
            └── TABLE C
    
    Formula: σ_A.z>10(A) ⋈_A.x=B.x B ⋈_B.y=C.y σ_C.w='value'(C)
    """
    # Table A
    table_a = QueryTree(type="TABLE", val="table_a")
    
    # Selection on A: A.z > 10
    cond_a = ConditionNode(
        attr=ColumnNode(column='z', table='table_a'),
        op='>',
        value=10
    )
    select_a = QueryTree(type="SIGMA", val=cond_a, childs=[table_a])
    
    # Table B
    table_b = QueryTree(type="TABLE", val="table_b")
    
    # First JOIN: σ_A.z>10(A) ⋈ B
    join_ab = QueryTree(type="JOIN", val="INNER", childs=[select_a, table_b])
    
    # Table C
    table_c = QueryTree(type="TABLE", val="table_c")
    
    # Selection on C: C.w = 'value'
    cond_c = ConditionNode(
        attr=ColumnNode(column='w', table='table_c'),
        op='=',
        value='value'
    )
    select_c = QueryTree(type="SIGMA", val=cond_c, childs=[table_c])
    
    # Final JOIN: (A ⋈ B) ⋈ σ_C.w='value'(C)
    final_join = QueryTree(type="JOIN", val="INNER", childs=[join_ab, select_c])
    
    return final_join

def create_tree_2_join_first():
    """
    Tree 2: Join semua dulu, selection di akhir (worst case)
    Structure:
        SIGMA (A.z > 10 AND C.w = 'value')
        └── JOIN
            ├── JOIN
            │   ├── TABLE A
            │   └── TABLE B
            └── TABLE C
    
    Formula: σ_(A.z>10 AND C.w='value')(A ⋈ B ⋈ C)
    """
    # Table A
    table_a = QueryTree(type="TABLE", val="table_a")
    
    # Table B
    table_b = QueryTree(type="TABLE", val="table_b")
    
    # First JOIN: A ⋈ B
    join_ab = QueryTree(type="JOIN", val="INNER", childs=[table_a, table_b])
    
    # Table C
    table_c = QueryTree(type="TABLE", val="table_c")
    
    # Second JOIN: (A ⋈ B) ⋈ C
    join_abc = QueryTree(type="JOIN", val="INNER", childs=[join_ab, table_c])
    
    # Selection conditions: A.z > 10 AND C.w = 'value'
    cond_a = ConditionNode(
        attr=ColumnNode(column='z', table='table_a'),
        op='>',
        value=10
    )
    cond_c = ConditionNode(
        attr=ColumnNode(column='w', table='table_c'),
        op='=',
        value='value'
    )
    
    # Combine with AND
    logical_cond = LogicalNode(operator="AND", childs=[cond_a, cond_c])
    
    # Final selection
    final_select = QueryTree(type="SIGMA", val=logical_cond, childs=[join_abc])
    
    return final_select

def create_tree_3_mixed():
    """
    Tree 3: Mixed strategy - selection sebagian
    Structure:
        SIGMA (C.w = 'value')
        └── JOIN
            ├── JOIN
            │   ├── TABLE A
            │   └── SIGMA (A.z > 10)
            │       └── TABLE B
            └── TABLE C
    
    Formula: σ_C.w='value'((A ⋈ σ_A.z>10(B)) ⋈ C)
    """
    # Table A
    table_a = QueryTree(type="TABLE", val="table_a")
    
    # Table B
    table_b = QueryTree(type="TABLE", val="table_b")
    
    # Selection on B: A.z > 10 (applied after reading B, but semantically on A)
    # Note: This is intentionally sub-optimal - applying selection late
    cond_a = ConditionNode(
        attr=ColumnNode(column='z', table='table_a'),
        op='>',
        value=10
    )
    select_b = QueryTree(type="SIGMA", val=cond_a, childs=[table_b])
    
    # First JOIN: A ⋈ σ(B)
    join_ab = QueryTree(type="JOIN", val="INNER", childs=[table_a, select_b])
    
    # Table C
    table_c = QueryTree(type="TABLE", val="table_c")
    
    # Second JOIN: (A ⋈ B) ⋈ C
    join_abc = QueryTree(type="JOIN", val="INNER", childs=[join_ab, table_c])
    
    # Selection on C: C.w = 'value'
    cond_c = ConditionNode(
        attr=ColumnNode(column='w', table='table_c'),
        op='=',
        value='value'
    )
    final_select = QueryTree(type="SIGMA", val=cond_c, childs=[join_abc])
    
    return final_select

def test_tree_comparison():
    """Test dan bandingkan cost dari 3 tree berbeda"""
    print_separator()
    print("TREE COMPARISON TEST - Query Cost Analysis")
    print("Query: SELECT * FROM A, B, C WHERE A.x = B.x AND B.y = C.y AND A.z > 10 AND C.w = 'value'")
    print_separator()
    print()
    
    # Setup dummy stats untuk tables
    planner = CostPlanner()
    
    # Override dummy stats untuk tables yang dipakai
    planner.temp_table_stats = {}
    
    # Table A: 10,000 records, 1000 blocks
    planner.temp_table_stats['table_a'] = {
        'n_r': 10000,
        'b_r': 1000,
        'l_r': 100,
        'f_r': 10,
        'v_a_r': {
            'x': 5000,  # join key
            'z': 100    # selection attribute
        }
    }
    
    # Table B: 5,000 records, 500 blocks
    planner.temp_table_stats['table_b'] = {
        'n_r': 5000,
        'b_r': 500,
        'l_r': 80,
        'f_r': 10,
        'v_a_r': {
            'x': 5000,  # join key with A
            'y': 2000   # join key with C
        }
    }
    
    # Table C: 20,000 records, 2000 blocks
    planner.temp_table_stats['table_c'] = {
        'n_r': 20000,
        'b_r': 2000,
        'l_r': 120,
        'f_r': 10,
        'v_a_r': {
            'y': 2000,  # join key with B
            'w': 50     # selection attribute
        }
    }
    
    print("TABLE STATISTICS:")
    print("-" * 100)
    print(f"{'Table':<10} {'Records':<10} {'Blocks':<10} {'Key Attributes':<30} {'Selection Attrs':<30}")
    print("-" * 100)
    print(f"{'table_a':<10} {'10,000':<10} {'1,000':<10} {'x: 5000 distinct':<30} {'z: 100 distinct':<30}")
    print(f"{'table_b':<10} {'5,000':<10} {'500':<10} {'x: 5000, y: 2000':<30} {'-':<30}")
    print(f"{'table_c':<10} {'20,000':<10} {'2,000':<10} {'y: 2000 distinct':<30} {'w: 50 distinct':<30}")
    print("-" * 100)
    print()
    
    # Create trees
    tree1 = create_tree_1_selection_first()
    tree2 = create_tree_2_join_first()
    tree3 = create_tree_3_mixed()
    
    # Test Tree 1
    print_separator()
    print("TREE 1: SELECTION PUSH-DOWN (Optimal Strategy)")
    print_separator()
    print("Strategy: Apply selections BEFORE joins to reduce intermediate result size")
    print("Formula: σ_A.z>10(A) ⋈ B ⋈ σ_C.w='value'(C)")
    print()
    print("Tree Structure:")
    print_tree_structure(tree1)
    print()
    
    parsed_query1 = ParsedQuery(query="", query_tree=tree1)
    cost_info1 = planner.calculate_cost(tree1)
    
    print(f"Total Cost: {cost_info1['cost']:,.2f} block I/Os")
    print(f"Final Result: {cost_info1['n_r']:,} records, {cost_info1['b_r']:,} blocks")
    print()
    print("Cost Breakdown:")
    print(f"  - Selection on A (z > 10): reduces 10,000 → {int(10000 * 0.5):,} records (selectivity = 0.5)")
    print(f"  - Selection on C (w = 'value'): reduces 20,000 → {int(20000 / 50):,} records (selectivity = 1/50)")
    print(f"  - First JOIN cost: reduced data sizes minimize intermediate results")
    print()
    
    # Test Tree 2
    print_separator()
    print("TREE 2: JOIN-FIRST STRATEGY (Worst Case)")
    print_separator()
    print("Strategy: Join all tables first, then apply selections")
    print("Formula: σ_(A.z>10 AND C.w='value')(A ⋈ B ⋈ C)")
    print()
    print("Tree Structure:")
    print_tree_structure(tree2)
    print()
    
    parsed_query2 = ParsedQuery(query="", query_tree=tree2)
    cost_info2 = planner.calculate_cost(tree2)
    
    print(f"Total Cost: {cost_info2['cost']:,.2f} block I/Os")
    print(f"Final Result: {cost_info2['n_r']:,} records, {cost_info2['b_r']:,} blocks")
    print()
    print("Cost Breakdown:")
    print(f"  - First JOIN (A ⋈ B): 10,000 × 5,000 = {(10000 * 5000) // max(5000, 5000):,} intermediate records")
    print(f"  - Second JOIN with C: Large intermediate result × 20,000 records")
    print(f"  - Selections applied AFTER all joins (too late!)")
    print()
    
    # Test Tree 3
    print_separator()
    print("TREE 3: MIXED STRATEGY")
    print_separator()
    print("Strategy: Partial selection optimization")
    print("Formula: σ_C.w='value'((A ⋈ σ_A.z>10(B)) ⋈ C)")
    print()
    print("Tree Structure:")
    print_tree_structure(tree3)
    print()
    
    parsed_query3 = ParsedQuery(query="", query_tree=tree3)
    cost_info3 = planner.calculate_cost(tree3)
    
    print(f"Total Cost: {cost_info3['cost']:,.2f} block I/Os")
    print(f"Final Result: {cost_info3['n_r']:,} records, {cost_info3['b_r']:,} blocks")
    print()
    print("Cost Breakdown:")
    print(f"  - Some selections applied early, some late")
    print(f"  - Better than Tree 2, but not optimal")
    print()
    
    # Comparison Summary
    print_separator()
    print("COST COMPARISON SUMMARY")
    print_separator()
    print()
    
    costs = [
        ("Tree 1: Selection Push-Down (Optimal)", cost_info1['cost']),
        ("Tree 2: Join-First (Worst)", cost_info2['cost']),
        ("Tree 3: Mixed Strategy", cost_info3['cost'])
    ]
    
    costs_sorted = sorted(costs, key=lambda x: x[1])
    
    print(f"{'Strategy':<45} {'Total Cost':<20} {'Ranking':<10}")
    print("-" * 100)
    
    for i, (name, cost) in enumerate(costs_sorted, 1):
        if i == 1:
            ranking = "⭐ BEST"
        elif i == len(costs_sorted):
            ranking = "❌ WORST"
        else:
            ranking = f"#{i}"
        print(f"{name:<45} {cost:>15,.2f} I/Os     {ranking:<10}")
    
    print("-" * 100)
    print()
    
    # Performance improvement
    best_cost = costs_sorted[0][1]
    worst_cost = costs_sorted[-1][1]
    improvement = ((worst_cost - best_cost) / worst_cost) * 100
    
    print("KEY INSIGHTS:")
    print(f"  ✓ Best strategy (Tree 1) is {improvement:.1f}% faster than worst (Tree 2)")
    print(f"  ✓ Improvement: {worst_cost - best_cost:,.2f} fewer block I/Os")
    print(f"  ✓ Lesson: Selection push-down significantly reduces intermediate result sizes")
    print()
    
    print_separator()
    print("CONCLUSION")
    print_separator()
    print()
    print("Tree optimization order matters significantly!")
    print()
    print("Best Practice:")
    print("  1. Apply selections as early as possible (push-down)")
    print("  2. Join smaller tables first")
    print("  3. Reduce data size before expensive operations")
    print()
    print_separator()

if __name__ == "__main__":
    test_tree_comparison()
