from model.query_tree import QueryTree
import re

# util kecil
def _is_cartesian(join_node: QueryTree) -> bool:
    return join_node.type == "JOIN" and (join_node.val == "" or join_node.val.upper() == "CARTESIAN")

def _is_theta(join_node: QueryTree) -> bool:
    return join_node.type == "JOIN" and join_node.val.upper().startswith("THETA:")

def _is_natural(join_node: QueryTree) -> bool:
    return join_node.type == "JOIN" and join_node.val.upper() == "NATURAL"

def _theta_pred(join_node: QueryTree) -> str:
    if not _is_theta(join_node): return ""
    return join_node.val.split(":", 1)[1].strip()

def _mk_theta(pred: str) -> str:
    return f"THETA:{pred.strip()}"

def _tables_under(node: QueryTree):
    """Extract all table names from a query tree"""
    out = []
    def dfs(n):
        if n.type == "TABLE":
            out.append(n.val)
        for c in n.childs:
            dfs(c)
    dfs(node)
    return set(out)

# σθ(E1 × E2)  ⇒  E1 ⋈θ E2
def fold_selection_with_cartesian(node: QueryTree):
    if node.type == "SIGMA" and node.childs and _is_cartesian(node.childs[0]):
        join = node.childs[0]
        pred = node.val  # seluruh predicate disimpan di val
        join.val = _mk_theta(pred)
        # ganti sigma dengan join
        if node.parent:
            node.parent.replace_child(node, join)
            join.parent = node.parent
        else:
            join.parent = None
        return join
    return node

# σθ(E1 ⋈θ2 E2)  ⇒  E1 ⋈(θ ∧ θ2) E2
def merge_selection_into_join(node: QueryTree):
    if node.type == "SIGMA" and node.childs and _is_theta(node.childs[0]):
        join = node.childs[0]
        p_old = _theta_pred(join)
        p_new = node.val
        merged = p_new if not p_old else f"{p_new} AND {p_old}"
        join.val = _mk_theta(merged)
        # angkat join, hilangkan sigma
        if node.parent:
            node.parent.replace_child(node, join)
            join.parent = node.parent
        else:
            join.parent = None
        return join
    return node

# Komutatif: E1 ⋈ E2 = E2 ⋈ E1
def make_join_commutative(join_node: QueryTree):
    if join_node.type == "JOIN" and len(join_node.childs) == 2:
        join_node.childs[0], join_node.childs[1] = join_node.childs[1], join_node.childs[0]
        join_node.childs[0].parent = join_node
        join_node.childs[1].parent = join_node
    return join_node

# Natural join asosiatif: (E1 ⋈ E2) ⋈ E3 = E1 ⋈ (E2 ⋈ E3)
def associate_natural_join(node: QueryTree) -> QueryTree:
    if node.type == "JOIN" and _is_natural(node):
        L, R = node.childs
        if L.type == "JOIN" and _is_natural(L):
            # (A ⋈ B) ⋈ C  =>  A ⋈ (B ⋈ C)
            A = L.childs[0]; B = L.childs[1]; C = R
            inner = QueryTree("JOIN", "NATURAL", [B, C]); B.parent = inner; C.parent = inner
            rot = QueryTree("JOIN", "NATURAL", [A, inner]); A.parent = rot; inner.parent = rot
            return rot
        if R.type == "JOIN" and _is_natural(R):
            # A ⋈ (B ⋈ C)  =>  (A ⋈ B) ⋈ C   (bentuk lain, tapi kita sediakan juga)
            B = R.childs[0]; C = R.childs[1]; A = L
            inner = QueryTree("JOIN", "NATURAL", [A, B]); A.parent = inner; B.parent = inner
            rot = QueryTree("JOIN", "NATURAL", [inner, C]); inner.parent = rot; C.parent = rot
            return rot
    return node

# Theta join asosiatif (syarat θ2 hanya atribut E2 dan E3)
# (E1 ⋈θ1 E2) ⋈θ12 E3 = E1 ⋈θ12 (E2 ⋈θ2 E3)
def associate_theta_join(node: QueryTree) -> QueryTree:
    if node.type != "JOIN" or not _is_theta(node):
        return node
    L, R = node.childs
    # kasus (E1⋈θ1E2) ⋈θ12 E3
    if L.type == "JOIN" and _is_theta(L):
        A, B = L.childs
        C = R
        inner = QueryTree("JOIN", L.val, [B, C]); B.parent = inner; C.parent = inner
        rot = QueryTree("JOIN", node.val, [A, inner]); A.parent = rot; inner.parent = rot
        return rot
    # kasus E1 ⋈θ12 (E2⋈θ2E3)
    if R.type == "JOIN" and _is_theta(R):
        A = L
        B, C = R.childs
        inner = QueryTree("JOIN", R.val, [B, C]); B.parent = inner; C.parent = inner
        rot = QueryTree("JOIN", node.val, [A, inner]); A.parent = rot; inner.parent = rot
        return rot
    return node

def plan_cost(node: QueryTree, stats: dict) -> int:
    """Biaya sederhana: 
       TABLE: b_r
       JOIN:  cost(left)+cost(right) + left_rows * right_blocks + left_blocks
       (mengarah ke left-deep yang memanfaatkan tabel kecil/seleksi awal)"""
    if node.type == "TABLE":
        t = node.val
        return stats.get(t, {}).get("b_r", 1000)

    if node.type == "SIGMA":
        return plan_cost(node.childs[0], stats) if node.childs else 0

    if node.type == "JOIN":
        L, R = node.childs
        cl = plan_cost(L, stats)
        cr = plan_cost(R, stats)
        def rows(n):
            if n.type == "TABLE":
                return stats.get(n.val, {}).get("n_r", 1000)
            if n.type == "JOIN":
                return max(rows(n.childs[0]), rows(n.childs[1]))
            if n.type == "SIGMA":
                return max(1, rows(n.childs[0]) // 2)
            return 1000
        def blocks(n):
            if n.type == "TABLE":
                return stats.get(n.val, {}).get("b_r", 100)
            if n.type == "JOIN":
                return blocks(n.childs[0]) + blocks(n.childs[1])
            if n.type == "SIGMA":
                return max(1, blocks(n.childs[0]) // 2)
            return 100
        nl = rows(L) * blocks(R) + blocks(L)  # nested-loop approx
        return cl + cr + nl

    # node lain: jumlahkan anak
    return sum(plan_cost(c, stats) for c in node.childs)

def choose_best(plans, stats: dict) -> QueryTree:
    best = None
    best_cost = None
    for p in plans:
        c = plan_cost(p, stats)
        if best is None or c < best_cost:
            best, best_cost = p, c
    return best

def build_join_tree(order, join_conditions: dict = None) -> QueryTree:
    """order: ['A','B','C']
       join_conditions: {frozenset({'A','B'}): 'A.x=B.y', ...}
       Buat left-deep: (((A ⋈ B) ⋈ C) ...)
       Gunakan THETA jika ada predicate, selain itu CARTESIAN."""
    if join_conditions is None:
        join_conditions = {}
    
    if not order:
        return None
    
    cur = QueryTree("TABLE", order[0])
    for i in range(1, len(order)):
        name = order[i]
        right = QueryTree("TABLE", name)
        # cari predicate join
        key = frozenset({order[0], name})
        pred = join_conditions.get(key, "")
        val = _mk_theta(pred) if pred else "CARTESIAN"
        cur = QueryTree("JOIN", val, [cur, right])
        cur.childs[0].parent = cur
        right.parent = cur
    return cur

def _first_table(node: QueryTree) -> str:
    if node.type == "TABLE": return node.val
    return _first_table(node.childs[0])

# Pipeline: dari ParsedQuery → best join plan
def join_order_optimize(pq, stats: dict):
    """Ambil tabel dari pohon (dummy SELECT/ FROM) lalu buat 3-5 kandidat urutan,
       pilih yang biaya terendah."""
    tables = list(_tables_under(pq.query_tree))
    if len(tables) <= 1:
        return pq  # tidak ada join

    # buat beberapa kandidat (tanpa itertools)
    orders = _some_permutations(tables, max_count=5)
    # map kondisi join dummy (bisa diisi dari parser logis jika sudah)
    join_map = {}  # {frozenset({'A','B'}): 'A.x=B.y', ...}
    plans = [build_join_tree(o[:], join_map) for o in orders]
    best = choose_best(plans, stats)
    return pq.__class__(pq.query, best)

def _some_permutations(items, max_count=5):
    res = []
    used = [False]*len(items)
    cur = []
    def bt():
        if len(cur) == len(items):
            res.append(cur[:])
            return
        if len(res) >= max_count: return
        for i in range(len(items)):
            if not used[i]:
                used[i] = True
                cur.append(items[i])
                bt()
                cur.pop()
                used[i] = False
    bt()
    return res if res else [items]

def validate_query(query: str) -> tuple:

    query = query.strip()
    
    # Check semicolon
    if not query.endswith(";"):
        return False, "Query must end with a semicolon."
    
    q_clean = query.rstrip(';').strip()
    if not q_clean:
        return False, "Query is empty."
    
    
    # SELECT pattern 
    select_pattern = re.compile(
        r'^\s*SELECT\s+.+?\s+FROM\s+.+?' 
        r'(\s+JOIN\s+.+?\s+ON\s+.+?)?'
        r'(\s+NATURAL\s+JOIN\s+.+?)?'
        r'(\s+WHERE\s+.+?)?' 
        r'(\s+GROUP\s+BY\s+.+?)?'
        r'(\s+ORDER\s+BY\s+.+?)?'
        r'(\s+LIMIT\s+\d+)?' 
        r'\s*;$',
        re.IGNORECASE | re.DOTALL
    )
    
    # Other query patterns
    other_patterns = {
        "UPDATE": re.compile(
            r'^\s*UPDATE\s+\w+\s+SET\s+.+?(\s+WHERE\s+.+?)?\s*;$',
            re.IGNORECASE | re.DOTALL
        ),
        "DELETE": re.compile(
            r'^\s*DELETE\s+FROM\s+\w+(\s+WHERE\s+.+?)?\s*;$',
            re.IGNORECASE
        ),
        "INSERT": re.compile(
            r'^\s*INSERT\s+INTO\s+\w+\s*\(.+?\)\s+VALUES\s*\(.+?\)\s*;$',
            re.IGNORECASE
        ),
        "CREATE": re.compile(
            r'^\s*CREATE\s+TABLE\s+\w+\s*\(.+?\)\s*;$',
            re.IGNORECASE
        ),
        "DROP": re.compile(
            r'^\s*DROP\s+TABLE\s+\w+\s*;$',
            re.IGNORECASE
        ),
        "BEGIN": re.compile(
            r'^\s*BEGIN\s+TRANSACTION\s*;$',
            re.IGNORECASE
        ),
        "COMMIT": re.compile(
            r'^\s*COMMIT\s*;$',
            re.IGNORECASE
        ),
        "ROLLBACK": re.compile(
            r'^\s*ROLLBACK\s*;$',
            re.IGNORECASE
        )
    }
    
    # Detect query type
    query_type = q_clean.split(maxsplit=1)[0].upper()
    
    # Validate SELECT query
    if query_type == "SELECT":
        if select_pattern.match(query):
            # Check clause order
            clause_order = ["WHERE", "GROUP BY", "ORDER BY", "LIMIT"]
            last_seen_index = -1
            
            for clause in clause_order:
                # Handle multi-word clauses
                if clause == "GROUP BY":
                    clause_pos = query.upper().find("GROUP BY")
                elif clause == "ORDER BY":
                    clause_pos = query.upper().find("ORDER BY")
                else:
                    clause_pos = query.upper().find(clause)
                    
                if clause_pos != -1:
                    if clause_pos < last_seen_index:
                        return False, f"Invalid clause order: {clause} appears out of sequence."
                    last_seen_index = clause_pos
            
            return True, "Valid SELECT query."
        else:
            return False, "Invalid SELECT query syntax."
    
    # Validate other query types
    if query_type in other_patterns:
        if other_patterns[query_type].match(query):
            return True, f"Valid {query_type} query."
        else:
            return False, f"Invalid {query_type} query syntax."
    
    return False, f"Unsupported query type: {query_type}"

# SELECT Helpers
def _get_columns_from_select(query: str) -> str:
    q_upper = query.upper()
    select_idx = q_upper.find("SELECT") + 6
    from_idx = q_upper.find("FROM")
    
    if from_idx == -1:
        columns = query[select_idx:].strip()
    else:
        columns = query[select_idx:from_idx].strip()
    
    return columns

def _get_from_table(query: str) -> str:
    q_upper = query.upper()
    from_idx = q_upper.find("FROM") + 4
    
    # Find next keyword
    end_keywords = ["WHERE", "GROUP BY", "ORDER BY", "LIMIT"]
    end_idx = len(query)
    
    for keyword in end_keywords:
        if keyword == "GROUP BY":
            idx = q_upper.find("GROUP BY", from_idx)
        elif keyword == "ORDER BY":
            idx = q_upper.find("ORDER BY", from_idx)
        else:
            idx = q_upper.find(keyword, from_idx)
        
        if idx != -1 and idx < end_idx:
            end_idx = idx
    
    return query[from_idx:end_idx].strip()

def _get_condition_from_where(query: str) -> str:
    q_upper = query.upper()
    where_idx = q_upper.find("WHERE")
    
    if where_idx == -1:
        return ""
    
    where_idx += 5  # len("WHERE")
    
    # Find next keyword
    end_keywords = ["GROUP BY", "ORDER BY", "LIMIT"]
    end_idx = len(query)
    
    for keyword in end_keywords:
        if keyword == "GROUP BY":
            idx = q_upper.find("GROUP BY", where_idx)
        elif keyword == "ORDER BY":
            idx = q_upper.find("ORDER BY", where_idx)
        else:
            idx = q_upper.find(keyword, where_idx)
        
        if idx != -1 and idx < end_idx:
            end_idx = idx
    
    return query[where_idx:end_idx].strip()

def _get_limit(query: str) -> int:
    q_upper = query.upper()
    limit_idx = q_upper.find("LIMIT") + 5
    
    limit_str = query[limit_idx:].strip().split()[0]
    return int(limit_str)

def _get_column_from_order_by(query: str) -> str:
    q_upper = query.upper()
    order_idx = q_upper.find("ORDER BY") + 8
    
    # Find next keyword
    end_keywords = ["LIMIT"]
    end_idx = len(query)
    
    for keyword in end_keywords:
        idx = q_upper.find(keyword, order_idx)
        if idx != -1:
            end_idx = idx
            break
    
    return query[order_idx:end_idx].strip()

def _get_column_from_group_by(query: str) -> str:
    q_upper = query.upper()
    group_idx = q_upper.find("GROUP BY") + 8
    
    # Find next keyword
    end_keywords = ["ORDER BY", "LIMIT"]
    end_idx = len(query)
    
    for keyword in end_keywords:
        if keyword == "ORDER BY":
            idx = q_upper.find("ORDER BY", group_idx)
        else:
            idx = q_upper.find(keyword, group_idx)
        
        if idx != -1:
            end_idx = idx
            break
    
    return query[group_idx:end_idx].strip()

def _parse_from_clause(query: str) -> QueryTree:
    from_tables = _get_from_table(query)
    q_upper = from_tables.upper()
    
    # Case 1: NATURAL JOIN
    if "NATURAL JOIN" in q_upper:
        join_split = re.split(r'\s+NATURAL\s+JOIN\s+', from_tables, flags=re.IGNORECASE)
        
        # Parse first table (may have alias)
        left_table = _parse_table_with_alias(join_split[0].strip())
        
        # Chain NATURAL JOIN nodes
        for right_table_str in join_split[1:]:
            right_table = _parse_table_with_alias(right_table_str.strip())
            
            join_node = QueryTree(type="JOIN", val="NATURAL")
            join_node.add_child(left_table)
            join_node.add_child(right_table)
            left_table = join_node
        
        return left_table
    
    # Case 2: Regular JOIN with ON
    elif "JOIN" in q_upper and "ON" in q_upper:
        join_split = re.split(r'\s+JOIN\s+', from_tables, flags=re.IGNORECASE)
        
        # Parse first table (may have alias)
        left_table = _parse_table_with_alias(join_split[0].strip())
        
        # Process each JOIN
        for join_part in join_split[1:]:
            temp = re.split(r'\s+ON\s+', join_part, flags=re.IGNORECASE)
            right_table_str = temp[0].strip()
            join_condition = temp[1].strip() if len(temp) > 1 else ""
            
            # Parse right table (may have alias)
            right_table = _parse_table_with_alias(right_table_str)
            
            # Clean condition
            join_condition = join_condition.replace("(", "").replace(")", "")
            
            # Create JOIN node with THETA format
            join_node = QueryTree(type="JOIN", val=f"THETA:{join_condition}")
            join_node.add_child(left_table)
            join_node.add_child(right_table)
            left_table = join_node
        
        return left_table
    
    # Case 3: Comma-separated tables (Cartesian product)
    elif "," in from_tables:
        tables = [t.strip() for t in from_tables.split(",")]
        
        # Parse first table (may have alias)
        left_table = _parse_table_with_alias(tables[0])
        
        # Chain CARTESIAN JOIN nodes
        for table_str in tables[1:]:
            right_table = _parse_table_with_alias(table_str)
            
            join_node = QueryTree(type="JOIN", val="CARTESIAN")
            join_node.add_child(left_table)
            join_node.add_child(right_table)
            left_table = join_node
        
        return left_table
    
    # Case 4: Single table (may have alias)
    else:
        return _parse_table_with_alias(from_tables.strip())

def _parse_table_with_alias (table_str: str) -> QueryTree:
    # Check for AS keyword
    if " AS " in table_str.upper():
        parts = re.split(r'\s+AS\s+', table_str, flags=re.IGNORECASE)
        table_name = parts[0].strip()
        alias = parts[1].strip()
        # Store as "table_name AS alias"
        return QueryTree(type="TABLE", val=f"{table_name} AS {alias}")
    else:
        # No alias, just table name
        return QueryTree(type="TABLE", val=table_str)

## UPDATE Helpers
def _extract_set_conditions(query: str) -> list:
    q_upper = query.upper()
    set_idx = q_upper.find("SET") + 3
    where_idx = q_upper.find("WHERE")
    
    if where_idx == -1:
        set_part = query[set_idx:].strip()
    else:
        set_part = query[set_idx:where_idx].strip()
    
    # Split by comma
    conditions = [c.strip() for c in set_part.split(",")]
    return conditions

def _extract_table_update(query: str) -> str:
    q_upper = query.upper()
    update_idx = q_upper.find("UPDATE") + 6
    set_idx = q_upper.find("SET")
    
    return query[update_idx:set_idx].strip()

## DELETE Helpers
def _extract_table_delete(query: str) -> str:
    q_upper = query.upper()
    from_idx = q_upper.find("FROM") + 4
    where_idx = q_upper.find("WHERE")
    
    if where_idx == -1:
        return query[from_idx:].strip()
    else:
        return query[from_idx:where_idx].strip()

## INSERT Helpers
def _extract_table_insert(query: str) -> str:
    q_upper = query.upper()
    into_idx = q_upper.find("INTO") + 4
    
    # Find opening parenthesis for columns
    paren_idx = query.find("(", into_idx)
    
    return query[into_idx:paren_idx].strip()

def _extract_columns_insert(query: str) -> str:
    # Find first parenthesis (columns)
    start_idx = query.find("(")
    end_idx = query.find(")", start_idx)
    
    columns = query[start_idx:end_idx+1]  # Include parentheses
    return columns

def _extract_values_insert(query: str) -> str:
    q_upper = query.upper()
    values_idx = q_upper.find("VALUES")
    
    if values_idx == -1:
        raise Exception("INSERT query must contain VALUES clause")
    
    # Find parenthesis after VALUES
    start_idx = query.find("(", values_idx)
    end_idx = query.find(")", start_idx)
    
    values = query[start_idx:end_idx+1]  # Include parentheses
    return values