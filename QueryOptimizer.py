from model.parsed_query import ParsedQuery
from model.query_tree import QueryTree
from helper.helper import (
    fold_selection_with_cartesian,
    merge_selection_into_join,
    make_join_commutative,
    associate_natural_join,
    associate_theta_join,
    choose_best,
    build_join_tree,
    plan_cost,
    _tables_under,
    _some_permutations,
)

from helper.stats import get_stats

class OptimizationEngine:
    
    def parse_query(self, query: str) -> ParsedQuery:
        q = query.strip().upper()
        root = QueryTree(type="SELECT", val=query)

        if "FROM" in q:
            idx = q.index("FROM") + 4
            after = q[idx:].strip()
            table_name = after.split()[0]

            from_node = QueryTree(type="FROM")
            table_node = QueryTree(type="TABLE", val=table_name)

            from_node.add_child(table_node)
            root.add_child(from_node)

        return ParsedQuery(query=query, query_tree=root)

    def optimize_query(self, parsed_query: ParsedQuery) -> ParsedQuery:
        root = parsed_query.query_tree

        # aturan logis
        root = fold_selection_with_cartesian(root)
        root = merge_selection_into_join(root)
        root = make_join_commutative(root)
        root = associate_natural_join(root)
        root = associate_theta_join(root)

        # ekstrak tabel dari query tree
        tables = list(_tables_under(root))
        
        # jika hanya 1 tabel atau tidak ada join, return as is
        if len(tables) <= 1:
            return ParsedQuery(parsed_query.query, root)
        
        # generate beberapa kandidat urutan join
        orders = _some_permutations(tables, max_count=5)
        
        # map kondisi join (untuk saat ini kosong, bisa diperluas)
        join_conditions = {}
        
        # build join trees untuk setiap urutan
        plans = []
        for order in orders:
            plan = build_join_tree(order, join_conditions)
            if plan:
                plans.append(plan)
        
        # jika tidak ada plan yang dihasilkan, return root asli
        if not plans:
            return ParsedQuery(parsed_query.query, root)
        
        # pilih plan terbaik berdasarkan cost
        stats = get_stats()
        best = choose_best(plans, stats)

        return ParsedQuery(parsed_query.query, best)

    def get_cost(self, parsed_query: ParsedQuery) -> int:
        root = parsed_query.query_tree
        stats = get_stats()
        return plan_cost(root, stats)