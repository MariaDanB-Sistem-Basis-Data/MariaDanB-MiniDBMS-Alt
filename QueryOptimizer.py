from model.parsed_query import ParsedQuery
from model.query_tree import QueryTree
from helper.helper import join_order_optimize, heuristic_cost

class OptimizationEngine:
    # def parse_query(self, query: str) -> ParsedQuery:
    #     q = query.strip().upper()

    #     # jenis query
    #     if q.startswith("SELECT"):
    #         node = QueryTree(type="SELECT", val=query)
    #     elif q.startswith("UPDATE"):
    #         node = QueryTree(type="UPDATE", val=query)
    #     elif q.startswith("DELETE"):
    #         node = QueryTree(type="DELETE", val=query)
    #     elif q.startswith("INSERT"):
    #         node = QueryTree(type="INSERT", val=query)
    #     elif q.startswith("CREATE TABLE"):
    #         node = QueryTree(type="CREATE TABLE", val=query)
    #     elif q.startswith("DROP TABLE"):
    #         node = QueryTree(type="DROP TABLE", val=query)
    #     elif q.startswith("BEGIN TRANSACTION"):
    #         node = QueryTree(type="BEGIN TRANSACTION", val=query)
    #     elif q.startswith("COMMIT"):
    #         node = QueryTree(type="COMMIT", val=query)
    #     elif q.startswith("ABORT"):
    #         node = QueryTree(type="ABORT", val=query)
    #     else:
    #         node = QueryTree(type="UNKNOWN", val=query)

    #     return ParsedQuery(query=query, query_tree=node)

    def parse_query(self, query: str) -> ParsedQuery:
        q = query.strip().upper()

        root = QueryTree(type="SELECT", val=query)

        # Dummy FROM parser (milestone belum perlu lengkap)
        if "FROM" in q:
            from_idx = q.index("FROM")
            after = q[from_idx+4:].strip()
            table_name = after.split()[0]

            from_node = QueryTree(type="FROM", val="")
            table_node = QueryTree(type="TABLE", val=table_name)

            from_node.add_child(table_node)
            root.add_child(from_node)

        return ParsedQuery(query=query, query_tree=root)

    def optimize_query(self, parsed_query: ParsedQuery) -> ParsedQuery:
        return join_order_optimize(parsed_query)

    def get_cost(self, parsed_query: ParsedQuery) -> int:
        return heuristic_cost(parsed_query.query_tree)