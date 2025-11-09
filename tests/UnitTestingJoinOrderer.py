import unittest
from model.query_tree import QueryTree
from model.parsed_query import ParsedQuery
from QueryOptimizer import OptimizationEngine 

class TestOptimizationEngine(unittest.TestCase):

    def setUp(self):
        self.engine = OptimizationEngine()
    def test_parse_select(self):
        pq = self.engine.parse_query("SELECT * FROM A")
        self.assertIsInstance(pq, ParsedQuery)
        self.assertEqual(pq.query_tree.type, "SELECT")
        self.assertEqual(len(pq.query_tree.childs), 1)
        self.assertEqual(pq.query_tree.childs[0].type, "FROM")
        self.assertEqual(pq.query_tree.childs[0].childs[0].val, "A")

    def test_parse_update(self):
        pq = self.engine.parse_query("UPDATE employee SET x=1")
        self.assertEqual(pq.query_tree.type, "SELECT")  

    def test_optimize_basic(self):
        pq = self.engine.parse_query("SELECT * FROM A")
        out = self.engine.optimize_query(pq)
        self.assertIsInstance(out, ParsedQuery)
        self.assertIsNotNone(out.query_tree)

    def test_optimize_three_tables(self):

        A = QueryTree("TABLE", val="A")
        B = QueryTree("TABLE", val="B")
        C = QueryTree("TABLE", val="C")

        j1 = QueryTree("JOIN", childs=[A, B])
        A.parent = j1
        B.parent = j1

        j2 = QueryTree("JOIN", childs=[j1, C])
        j1.parent = j2
        C.parent = j2

        root = QueryTree("SELECT", val="dummy")
        root.add_child(j2)

        pq = ParsedQuery("SELECT * FROM A,B,C", root)

        out = self.engine.optimize_query(pq)

        self.assertEqual(out.query_tree.type, "JOIN")

        self.assertEqual(out.query_tree.childs[0].type, "JOIN")

    def test_cost_positive(self):
        pq = self.engine.parse_query("SELECT * FROM A")
        optimized = self.engine.optimize_query(pq)
        cost = self.engine.get_cost(optimized)

        self.assertTrue(isinstance(cost, int))
        self.assertGreaterEqual(cost, 0)

    def test_full_pipeline(self):
        pq = self.engine.parse_query("SELECT * FROM A, B, C")
        out = self.engine.optimize_query(pq)
        cost = self.engine.get_cost(out)

        self.assertIsNotNone(out.query_tree)
        self.assertGreaterEqual(cost, 0)


if __name__ == "__main__":
    unittest.main()
