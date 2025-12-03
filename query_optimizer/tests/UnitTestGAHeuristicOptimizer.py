import unittest
from QueryOptimizer import OptimizationEngine

class TestGAComparison(unittest.TestCase):
    def setUp(self):
        self.engine = OptimizationEngine()
        self.engine.use_ga = True

    def _run_query(self, query):
        parsed = self.engine.parse_query(query)
        optimized = self.engine.optimize_query(parsed)
        info = self.engine.get_optimization_info()
        return info

    def test_ga_better_than_heuristic(self):
        query = """
        SELECT m.title, r.rating, d.name, ac.name, aw.award_name
        FROM movies m
        JOIN reviews r ON m.movie_id = r.movie_id
        JOIN movie_directors md ON md.movie_id = m.movie_id
        JOIN directors d ON md.director_id = d.director_id
        JOIN awards aw ON aw.movie_id = m.movie_id
        WHERE m.genre = 'Drama' AND r.rating > 7;
        """

        info = self._run_query(query)

        print("\n=== COST REPORT ===")
        print("Heuristic Cost:", info.get("heuristic_cost"))
        print("GA Cost:", info.get("ga_cost"))
        print("GA Chosen?", info.get("method") == "genetic_algorithm")

        self.assertLessEqual(info["ga_cost"], info["heuristic_cost"])

if __name__ == "__main__":
    unittest.main()
