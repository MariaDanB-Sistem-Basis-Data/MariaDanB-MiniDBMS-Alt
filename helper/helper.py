from model.query_tree import QueryTree

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