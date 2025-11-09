# python -m unittest tests/UnitTestingJoinOrderer.py

import unittest

from QueryOptimizer import OptimizationEngine
from model.query_tree import QueryTree

from helper.helper import (
    extract_join_tables,
    generate_permutations,
    build_join_tree,
    heuristic_cost,
    choose_best,
)

class TestJoinOrderer(unittest.TestCase):
    def setUp(self):
        self.engine = OptimizationEngine()
    
    def test_parse_query_from(self):
        pq = self.engine.parse_query("SELECT * FROM A")
        root = pq.query_tree

        self.assertEqual(root.type, "SELECT")
        self.assertEqual(root.childs[0].type, "FROM")
        self.assertEqual(root.childs[0].childs[0].type, "TABLE")
        self.assertEqual(root.childs[0].childs[0].val, "A")
    
    def test_extract_join_tables(self):
        root = QueryTree("SELECT")
        from_node = QueryTree("FROM")
        t1 = QueryTree("TABLE", val="A")
        t2 = QueryTree("TABLE", val="B")
        from_node.childs = [t1, t2]
        root.childs = [from_node]

        tables = extract_join_tables(root)
        self.assertEqual(set(tables), {"A", "B"})
    
    def test_generate_permutations(self):
        arr = ["A", "B", "C"]
        perms = generate_permutations(arr)
        self.assertEqual(len(perms), 6)
        self.assertIn(["A", "B", "C"], perms)
        self.assertIn(["C", "B", "A"], perms)
    
    def test_build_join_tree(self):
        order = ["A", "B", "C"]
        tree = build_join_tree(order)

        self.assertEqual(tree.type, "JOIN")
        self.assertEqual(tree.childs[0].type, "JOIN")
        self.assertEqual(tree.childs[1].type, "TABLE")
        self.assertEqual(tree.childs[1].val, "C")
    
    def test_heuristic_cost(self):
        order = ["A", "B"]
        tree = build_join_tree(order)
        cost = heuristic_cost(tree)
        self.assertEqual(cost, 3)
    
    def test_choose_best(self):
        t1 = build_join_tree(["A", "B", "C"])
        t2 = build_join_tree(["A", "B"])

        best = choose_best([t1, t2], heuristic_cost)
        self.assertEqual(heuristic_cost(best), heuristic_cost(t2))
    
    def test_optimize_query_join(self):
        pq = self.engine.parse_query("SELECT * FROM A")
        pq.query_tree.childs[0].childs = [
            QueryTree("TABLE", val="A"),
            QueryTree("TABLE", val="B"),
            QueryTree("TABLE", val="C"),
        ]

        opt = self.engine.optimize_query(pq)

        self.assertEqual(opt.query_tree.type, "JOIN")
        self.assertNotEqual(opt.query_tree.type, "TABLE")

        collected = []

        def dfs(n):
            if n.type == "TABLE":
                collected.append(n.val)
            for c in n.childs:
                dfs(c)

        dfs(opt.query_tree)
        self.assertEqual(set(collected), {"A", "B", "C"})


if __name__ == "__main__":
    unittest.main()