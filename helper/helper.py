from model.query_tree import QueryTree
from model.parsed_query import ParsedQuery

def generate_permutations(arr):
    results = []
    used = [False] * len(arr)

    def backtrack(path):
        if len(path) == len(arr):
            results.append(path[:])
            return
        for i in range(len(arr)):
            if not used[i]:
                used[i] = True
                path.append(arr[i])
                backtrack(path)
                path.pop()
                used[i] = False

    backtrack([])
    return results


def extract_join_tables(node):
    tables = []

    def dfs(n):
        if n is None:
            return
        if n.type == "TABLE":
            tables.append(n.val)
        for c in n.childs:
            dfs(c)

    dfs(node)
    return tables


def build_join_tree(order):
    """
    OUTPUT: (Tree)
        JOIN(
            JOIN(TABLE(A), TABLE(B)),
            TABLE(C)
        )
    """
    root = QueryTree("TABLE", val=order[0])
    curr = root

    for tbl in order[1:]:
        right = QueryTree("TABLE", val=tbl)
        new_join = QueryTree("JOIN", val="")
        new_join.childs = [curr, right]
        curr.parent = new_join
        right.parent = new_join
        curr = new_join
    return curr


def heuristic_cost(tree):
    count = 0

    def dfs(n):
        nonlocal count
        count += 1
        for c in n.childs:
            dfs(c)

    dfs(tree)
    return count


def choose_best(plans, cost_fn):
    best_tree = None
    best_cost = 10**18

    for p in plans:
        c = cost_fn(p)
        if c < best_cost:
            best_cost = c
            best_tree = p
    return best_tree


def join_order_optimize(parsed_query: ParsedQuery):
    root = parsed_query.query_tree

    tables = extract_join_tables(root)

    # Jika tidak ada join
    if len(tables) <= 1:
        return parsed_query

    perms = generate_permutations(tables)

    candidate_plans = [build_join_tree(order) for order in perms]

    best = choose_best(candidate_plans, heuristic_cost)

    parsed_query.query_tree = best
    return parsed_query