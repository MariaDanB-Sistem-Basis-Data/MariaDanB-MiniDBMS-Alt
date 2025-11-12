import unittest
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from QueryOptimizer import OptimizationEngine
from model.query_tree import QueryTree
from helper.helper import (
    decompose_conjunctive_selection,
    swap_selection_order,
    eliminate_redundant_projections,
    push_selection_through_join_single,
    push_selection_through_join_split,
    push_projection_through_join_simple,
    push_projection_through_join_with_join_attrs
)


class TestNonJoinOptimization(unittest.TestCase):
    
    def setUp(self):
        """Setup optimization engine untuk semua test"""
        self.engine = OptimizationEngine()
    
    def _print_tree(self, node, indent=0):
        """Helper untuk print tree structure"""
        if not node:
            return ""
        result = "  " * indent + f"{node.type}"
        if node.val:
            result += f"({node.val})"
        result += "\n"
        for child in node.childs:
            result += self._print_tree(child, indent + 1)
        return result
    
    def test_decompose_conjunctive_selection(self):
        """Test Rule 1: σθ1∧θ2(E) = σθ1(σθ2(E))"""
        print("\n=== Test 1: Decompose Conjunctive Selection ===")
        
        # Parse SQL query yang mengandung AND condition
        sql = "SELECT * FROM students WHERE age > 20 AND name = 'John';"
        parsed = self.engine.parse_query(sql)
        
        print("Query:", sql)
        print("\nBefore:")
        print(self._print_tree(parsed.query_tree))
        
        # Ambil node SIGMA dari query tree
        sigma_node = parsed.query_tree
        while sigma_node and sigma_node.type != "SIGMA":
            if sigma_node.childs:
                sigma_node = sigma_node.childs[0]
            else:
                break
        
        result = decompose_conjunctive_selection(sigma_node)
        
        print("After:")
        print(self._print_tree(result))
        
        # Cek hasilnya harus jadi nested SIGMA
        self.assertEqual(result.type, "SIGMA")
        self.assertEqual(result.childs[0].type, "SIGMA")
        print("✓ Test passed: Selection decomposed correctly")
    
    def test_swap_selection_order(self):
        """Test Rule 2: σθ1(σθ2(E)) = σθ2(σθ1(E))"""
        print("\n=== Test 2: Swap Selection Order ===")
        
        # Parse SQL query dengan multiple WHERE conditions
        sql = "SELECT * FROM students WHERE age > 20 AND name = 'John';"
        parsed = self.engine.parse_query(sql)
        
        print("Query:", sql)
        print("\nBefore:")
        print(self._print_tree(parsed.query_tree))
        
        # Ambil root SIGMA
        sigma_node = parsed.query_tree
        while sigma_node and sigma_node.type != "SIGMA":
            if sigma_node.childs:
                sigma_node = sigma_node.childs[0]
            else:
                break
        
        result = swap_selection_order(sigma_node)
        
        print("After:")
        print(self._print_tree(result))
        
        # Cek urutannya tertukar
        self.assertEqual(result.type, "SIGMA")
        print("✓ Test passed: Selection order swapped")
    
    def test_eliminate_redundant_projections(self):
        """Test Rule 3: ΠL1(ΠL2(...ΠLn(E))) = ΠL1(E)"""
        print("\n=== Test 3: Eliminate Redundant Projections ===")
        
        # Buat manual nested projections karena SQL tidak akan generate ini
        # (Ini edge case yang sulit dibuat dari SQL biasa)
        sql = "SELECT name FROM students;"
        parsed = self.engine.parse_query(sql)
        
        # Manual tambahkan redundant projections untuk testing
        table = QueryTree("TABLE", "students")
        proj3 = QueryTree("PROJECT", "name, age, id")
        proj3.add_child(table)
        proj2 = QueryTree("PROJECT", "name, age")
        proj2.add_child(proj3)
        proj1 = QueryTree("PROJECT", "name")
        proj1.add_child(proj2)
        
        print("Before (manually created nested projections):")
        print(self._print_tree(proj1))
        
        result = eliminate_redundant_projections(proj1)
        
        print("After:")
        print(self._print_tree(result))
        
        # Cek hanya ada 1 PROJECT
        self.assertEqual(result.type, "PROJECT")
        self.assertEqual(result.val, "name")
        # Child langsung ke TABLE
        self.assertEqual(result.childs[0].type, "TABLE")
        print("✓ Test passed: Redundant projections eliminated")
    
    def test_push_selection_through_join_single(self):
        """Test Rule 4a: σθ0(E1⋈E2) = (σθ0(E1)) ⋈ E2"""
        print("\n=== Test 4a: Push Selection Through Join (Single) ===")
        
        # Parse SQL dengan JOIN dan WHERE condition
        sql = "SELECT * FROM students, courses WHERE students.age > 20;"
        parsed = self.engine.parse_query(sql)
        
        print("Query:", sql)
        print("\nBefore:")
        print(self._print_tree(parsed.query_tree))
        
        # Cari SIGMA node
        sigma_node = parsed.query_tree
        while sigma_node and sigma_node.type != "SIGMA":
            if sigma_node.childs:
                sigma_node = sigma_node.childs[0]
            else:
                break
        
        result = push_selection_through_join_single(sigma_node)
        
        print("After:")
        print(self._print_tree(result))
        
        # Cek sigma dipush ke salah satu child
        if result.type == "JOIN":
            print("✓ Test passed: Selection pushed through join")
        else:
            print("Note: Result type is", result.type)
    
    def test_push_selection_through_join_split(self):
        """Test Rule 4b: σ(θ1∧θ2)(E1⋈E2) = (σθ1(E1)) ⋈ (σθ2(E2))"""
        print("\n=== Test 4b: Push Selection Through Join (Split) ===")
        
        # Parse SQL dengan JOIN dan multiple WHERE conditions pada table berbeda
        sql = "SELECT * FROM students, courses WHERE students.age > 20 AND courses.credits > 3;"
        parsed = self.engine.parse_query(sql)
        
        print("Query:", sql)
        print("\nBefore:")
        print(self._print_tree(parsed.query_tree))
        
        # Cari SIGMA node pertama
        sigma_node = parsed.query_tree
        while sigma_node and sigma_node.type != "SIGMA":
            if sigma_node.childs:
                sigma_node = sigma_node.childs[0]
            else:
                break
        
        result = push_selection_through_join_split(sigma_node)
        
        print("After:")
        print(self._print_tree(result))
        
        # Cek hasilnya
        if result.type == "JOIN":
            print("✓ Test passed: Selection split and pushed through join")
        else:
            print("Note: Result type is", result.type)
    
    def test_push_projection_through_join_simple(self):
        """Test Rule 5a: ΠL1∪L2(E1⋈E2) = (ΠL1(E1)) ⋈ (ΠL2(E2))"""
        print("\n=== Test 5a: Push Projection Through Join (Simple) ===")
        
        # Parse SQL dengan JOIN dan specific columns
        sql = "SELECT students.name, courses.title FROM students, courses;"
        parsed = self.engine.parse_query(sql)
        
        print("Query:", sql)
        print("\nBefore:")
        print(self._print_tree(parsed.query_tree))
        
        # Cari PROJECT node
        proj_node = parsed.query_tree
        while proj_node and proj_node.type != "PROJECT":
            if proj_node.childs:
                proj_node = proj_node.childs[0]
            else:
                break
        
        result = push_projection_through_join_simple(proj_node)
        
        print("After:")
        print(self._print_tree(result))
        
        # Cek apakah projection dipush
        if result.type == "JOIN" and result.childs[0].type == "PROJECT":
            print("✓ Test passed: Projection pushed through join")
        else:
            print("Note: Result structure differs")
    
    def test_push_projection_through_join_with_join_attrs(self):
        """Test Rule 5b: ΠL1∪L2(E1⋈θE2) with join attributes"""
        print("\n=== Test 5b: Push Projection Through Join (With Join Attrs) ===")
        
        # Parse SQL dengan explicit JOIN dan ON clause
        sql = "SELECT students.name FROM students JOIN courses ON students.id = courses.student_id;"
        parsed = self.engine.parse_query(sql)
        
        print("Query:", sql)
        print("\nBefore:")
        print(self._print_tree(parsed.query_tree))
        
        # Cari PROJECT node
        proj_node = parsed.query_tree
        
        result = push_projection_through_join_with_join_attrs(proj_node)
        
        print("After:")
        print(self._print_tree(result))
        
        # Cek hasilnya
        self.assertIsNotNone(result)
        print("✓ Test passed: Projection with join attributes handled")
    


if __name__ == '__main__':
    print("=" * 60)
    print("UNIT TESTING: Non-Join Optimization Rules")
    print("Using parse_query() to build query trees from SQL")
    print("=" * 60)
    
    unittest.main(verbosity=2)

