"""
File ini tabel-tabelnya masih pakai dummy, jalankan python tests/UnitTestingCostPlaner.py atau python tests/test_or_with_v_a_r.py
- n_r: number of tuples in relation r
- b_r: number of blocks containing tuples of r
- l_r: size of a tuple of r
- f_r: blocking factor of r (number of tuples that fit in one block)
- V(A,r): number of distinct values for attribute A in relation r
"""

from model.query_tree import QueryTree
from model.parsed_query import ParsedQuery
import math

class CostPlanner:
    def __init__(self, storage_manager=None):
        self.storage_manager = storage_manager

        # TODO ==================== [HAPUS SAAT INTEGRASI] ====================
        self.BLOCK_SIZE = 4096 
        self.PAGE_SIZE = 4096 
         # ====================================================================
        
        # Cache untuk menyimpan statistik temporary tables (hasil join, selection, dll)
        # Key: identifier string, Value: dict dengan n_r, b_r, f_r, v_a_r
        self.temp_table_stats = {}
        
    # =================== HELPER FUNCTIONS UNTUK MENDAPATKAN STATISTIK ===================
    
    def get_table_stats(self, table_name: str) -> dict:
        """
        Mendapatkan statistik tabel dari Storage Manager atau temporary cache.
        Return: dict dengan keys: n_r, b_r, l_r, f_r, v_a_r
        
        ============================================================
        INTEGRASI DENGAN SM: Ketika SM ready, HAPUS semua 
        bagian yang ditandai [HAPUS SAAT INTEGRASI] dan 
        UNCOMMENT bagian [UNCOMMENT SAAT INTEGRASI]
        ============================================================
        """
        # Cek apakah ini temporary table (hasil join/selection)
        if table_name in self.temp_table_stats:
            return self.temp_table_stats[table_name]
        
        # TODO ==================== [UNCOMMENT SAAT INTEGRASI] ====================
        # Ketika SM  ready, UNCOMMENT blok di bawah ini:
        # memakai get_stats dari Storage Manager
        # 
        # if self.storage_manager:
        #     stats = self.storage_manager.get_stats(table_name)
        #     return {
        #         'n_r': stats.n_r,
        #         'b_r': stats.b_r,
        #         'l_r': stats.l_r,
        #         'f_r': stats.f_r,
        #         'v_a_r': stats.v_a_r  # dict: {column_name: distinct_count}
        #     }
        # 
        # # Jika table tidak ditemukan di SM
        # raise ValueError(f"Table '{table_name}' not found in Storage Manager")
        # ====================================================================
        
        # TODO ==================== [HAPUS SAAT INTEGRASI] ====================
        # Seluruh bagian dummy_stats di bawah ini HARUS DIHAPUS saat integrasi
        # Dummy statistics (untuk testing tanpa SM)
        dummy_stats = {
            "students": {
                'n_r': 10000,
                'b_r': 500,
                'l_r': 200,
                'f_r': 10,
                'v_a_r': {
                    'student_id': 10000,  # primary key: semuanya unique
                    'name': 9500,         # hampir unique
                    'age': 50,            # range 18-67
                    'gpa': 41,            # range 0.0-4.0 (dengan step 0.1)
                    'major': 20           # 20 jurusan berbeda
                }
            },
            "courses": {
                'n_r': 500,
                'b_r': 50,
                'l_r': 100,
                'f_r': 10,
                'v_a_r': {
                    'course_id': 500,     # primary key
                    'course_name': 500,   # unique
                    'credits': 4,         # 1,2,3,4 credits
                    'department': 15      # 15 departments
                }
            },
            "enrollments": {
                'n_r': 50000,
                'b_r': 2500,
                'l_r': 150,
                'f_r': 20,
                'v_a_r': {
                    'enrollment_id': 50000,  # primary key
                    'student_id': 10000,     # foreign key ke students
                    'course_id': 500,        # foreign key ke courses
                    'grade': 13,             # A+, A, A-, B+, B, B-, C+, C, C-, D, F, W, I
                    'semester': 20           # semester values
                }
            },
            "employees": {
                'n_r': 10000,  # 10,000 tuples
                'b_r': 1000,   # 1,000 blocks
                'l_r': 40,     # 40 bytes per tuple
                'f_r': 10,     # 10 tuples per block
                'v_a_r': {
                    'id': 10000,
                    'name': 9500,
                    'dept_id': 50,
                    'salary': 500
                }
            },
            "departments": {
                'n_r': 1000,
                'b_r': 50,
                'l_r': 80,
                'f_r': 20,
                'v_a_r': {
                    'id': 1000,
                    'name': 950,
                    'manager_id': 800
                }
            },
            "orders": {
                'n_r': 75000,
                'b_r': 5000,
                'l_r': 60,
                'f_r': 15,
                'v_a_r': {
                    'id': 75000,
                    'customer_id': 2000,
                    'status': 5
                }
            },
            "customers": {
                'n_r': 24000,
                'b_r': 2000,
                'l_r': 50,
                'f_r': 12,
                'v_a_r': {
                    'id': 24000,
                    'name': 23000,
                    'city': 200
                }
            },
            "products": {
                'n_r': 20000,
                'b_r': 800,
                'l_r': 100,
                'f_r': 25,
                'v_a_r': {
                    'id': 20000,
                    'name': 19000,
                    'category': 50
                }
            }
        }
        
        # Default stats untuk tabel yang tidak dikenal
        default_stats = {
            'n_r': 10000,
            'b_r': 500,
            'l_r': 80,
            'f_r': 10,
            'v_a_r': {}
        }
        
        return dummy_stats.get(table_name.lower(), default_stats)
        # ==================== [AKHIR BAGIAN HAPUS] ====================
    
    def store_temp_stats(self, table_id: str, n_r: int, b_r: int, f_r: int, v_a_r: dict):
        """
        Menyimpan statistik untuk temporary table (hasil join, selection, dll)
        """
        self.temp_table_stats[table_id] = {
            'n_r': n_r,
            'b_r': b_r,
            'l_r': 0,  # ga perlu untuk temporary
            'f_r': f_r,
            'v_a_r': v_a_r
        }
    
    def estimate_selectivity(self, condition: str, v_a_r: dict = None) -> float:
        """
        Estimasi selectivity dari kondisi selection menggunakan V(A,r).
        Return: nilai antara 0.0 - 1.0
        
        Rumus dari slide "Size Estimation of Complex Selections":
        - Equality (A = value): σ_A=v(r) → selectivity = 1/V(A,r)
        - Inequality (A ≠ value): selectivity = 1 - (1/V(A,r))
        - Comparison (A > value): selectivity ≈ 0.5 (tanpa histogram)
        - LIKE, IN: heuristic estimates
        
        Args:
            condition: String kondisi (e.g., 'age > 18', 'name = "John"')
            v_a_r: Dictionary {attribute: distinct_count} dari tabel
        """
        if v_a_r is None:
            v_a_r = {}
        
        condition_upper = condition.upper()
        
        # Parse attribute name dari condition
        # Format: "attribute operator value" atau "table.attribute operator value"
        attribute = self._extract_attribute_from_condition(condition)
        
        # Equality condition: σ_A=v(r)
        # Formula: selectivity = 1 / V(A,r)
        if "=" in condition and "!=" not in condition and "<>" not in condition:
            if attribute and attribute in v_a_r:
                v_a = v_a_r[attribute]
                return 1.0 / v_a if v_a > 0 else 0.1
            else:
                # Fallback: asumsi equality sangat selective
                return 0.1
        
        # Inequality: σ_A≠v(r)
        # Formula: selectivity = 1 - (1 / V(A,r))
        elif "!=" in condition or "<>" in condition:
            if attribute and attribute in v_a_r:
                v_a = v_a_r[attribute]
                return 1.0 - (1.0 / v_a) if v_a > 0 else 0.9
            else:
                # Fallback: hampir semua tuple
                return 0.9
        
        # Comparison: σ_A>v(r) atau σ_A<v(r)
        # Formula ideal: (max - v) / (max - min)
        # Tanpa histogram: asumsi distribusi uniform → 0.5
        elif ">" in condition or "<" in condition or ">=" in condition or "<=" in condition:
            # TODO: Gunakan histogram jika tersedia
            return 0.5
        
        # Pattern matching: LIKE
        # Heuristic: tergantung pattern complexity / pure heuristic
        elif "LIKE" in condition_upper:
            return 0.2 
        
        # IN clause: σ_A IN (v1,v2,...,vn)(r)
        # Formula: selectivity = n / V(A,r)
        elif "IN" in condition_upper:
            if attribute and attribute in v_a_r:
                # Count jumlah values dalam IN clause
                # Simple heuristic: asumsi 5 values
                num_values = 5
                v_a = v_a_r[attribute]
                return min(1.0, num_values / v_a) if v_a > 0 else 0.15
            else:
                return 0.15
        
        # Default: konservatif
        else:
            return 0.5
    
    def _extract_attribute_from_condition(self, condition: str) -> str:
        """
        Extract attribute name dari condition string.
        Examples:
            'age > 18' → 'age'
            'students.age > 18' → 'age'
            'name = "John"' → 'name'
        """
        import re
        
        # Patterns untuk operator
        operators = ['>=', '<=', '<>', '!=', '=', '>', '<', 'LIKE', 'IN']
        
        for op in operators:
            if op in condition.upper():
                # Split by operator
                parts = re.split(f'\\s*{re.escape(op)}\\s*', condition, flags=re.IGNORECASE)
                if parts:
                    attr = parts[0].strip()
                    # Handle table.attribute format
                    if '.' in attr:
                        attr = attr.split('.')[-1].strip()
                    return attr
        
        return ""
    
    def estimate_conjunction_selectivity(self, conditions: list, v_a_r: dict = None) -> float:
        """
        Estimasi selectivity untuk CONJUNCTION (AND).
        
        Rumus dari slide: σ_θ1∧θ2∧...∧θn(r)
        Assuming independence: s_1 * s_2 * ... * s_n
        
        Formula: n_r * (s_1 * s_2 * ... * s_n) / n_r^n
        Simplified: s_1 * s_2 * ... * s_n
        
        Args:
            conditions: List of condition strings
            v_a_r: Dictionary {attribute: distinct_count}
        
        Returns:
            Combined selectivity (product of individual selectivities)
        """
        if not conditions:
            return 1.0
        
        # Asumsi independence: kalikan semua selectivity
        result = 1.0
        for cond in conditions:
            s_i = self.estimate_selectivity(cond, v_a_r)
            result *= s_i
        
        return result
    
    def estimate_disjunction_selectivity(self, conditions: list, v_a_r: dict = None) -> float:
        """
        Estimasi selectivity untuk DISJUNCTION (OR).
        
        Rumus dari slide: σ_θ1∨θ2∨...∨θn(r)
        Formula: n_r * (1 - (1 - s_1) * (1 - s_2) * ... * (1 - s_n))
        
        Explanation:
        - P(A OR B) = P(A) + P(B) - P(A AND B)
        - For multiple: 1 - P(not A AND not B AND not C)
        - = 1 - (1-s_1)*(1-s_2)*(1-s_3)
        
        Args:
            conditions: List of condition strings
            v_a_r: Dictionary {attribute: distinct_count}
        
        Returns:
            Combined selectivity for disjunction
        """
        if not conditions:
            return 1.0
        
        # Formula: 1 - (1-s_1)*(1-s_2)*...*(1-s_n)
        product = 1.0
        for cond in conditions:
            s_i = self.estimate_selectivity(cond, v_a_r)
            product *= (1.0 - s_i)
        
        return 1.0 - product
    
    def estimate_negation_selectivity(self, condition: str, v_a_r: dict = None) -> float:
        """
        Estimasi selectivity untuk NEGATION (NOT).
        
        Rumus dari slide: σ_¬θ(r)
        Formula: n_r - size(σ_θ(r))
        Selectivity: 1 - s_θ
        
        Args:
            condition: Condition string
            v_a_r: Dictionary {attribute: distinct_count}
        
        Returns:
            Negation selectivity
        """
        s = self.estimate_selectivity(condition, v_a_r)
        return 1.0 - s
    
    # ================================================ COST FUNCTIONS ================================================
    
    def cost_table_scan(self, node: QueryTree) -> dict:
        """
        Cost untuk full table scan.
        
        Rumus: Cost = b_r (jumlah blocks yang harus dibaca)
        
        Return: dict dengan cost, n_r (estimated tuples), dan b_r (blocks)
        """
        table_name = node.val
        stats = self.get_table_stats(table_name)
        
        return {
            "operation": "TABLE_SCAN",
            "table": table_name,
            "cost": stats['b_r'],
            "n_r": stats['n_r'],
            "b_r": stats['b_r'],
            "f_r": stats['f_r'],
            "v_a_r": stats['v_a_r'],
            "description": f"Full scan of table {table_name}"
        }
    
    def cost_selection(self, node: QueryTree, input_cost: dict) -> dict:
        """
        Cost untuk operasi SELECTION (σ - sigma).
        
        Rumus dari slide "Selection Size Estimation":
        - σ_A=v(r): output = n_r / V(A,r)
        - σ_A≤v(r): output = n_r * selectivity (dengan histogram)
        - Tanpa histogram: output = n_r / 2 (asumsi konservatif)
        
        Cost = cost(input) (tidak ada tambahan I/O, hanya processing)
        Output tuples: n_r * selectivity
        Output blocks: ceil(output_tuples / f_r)
        
        TODO: Implementasi index scan jika ada index
        - Dengan B+-tree index: cost = h_i + (selectivity * b_r)
        - h_i = tinggi tree
        """
        condition = node.val
        
        input_n_r = input_cost.get("n_r", 1000)
        input_b_r = input_cost.get("b_r", 100)
        input_f_r = input_cost.get("f_r", 10)
        input_v_a_r = input_cost.get("v_a_r", {})
        
        # Hitung selectivity DENGAN V(A,r)
        selectivity = self.estimate_selectivity(condition, input_v_a_r)
        
        # Estimasi output size
        # Formula: n_r(σ) = n_r(input) * selectivity
        output_n_r = max(1, int(input_n_r * selectivity))
        
        # Estimasi output blocks
        # Formula: b_r = ceil(n_r / f_r)
        output_b_r = max(1, math.ceil(output_n_r / input_f_r)) if input_f_r > 0 else input_b_r
        
        # V(A,r) untuk output
        # Formula dari slide: "If the selection condition θ is of the form A op r
        #                      estimated V(A, σ_θ(r)) = V(A,r) * s"
        # where s is the selectivity of the selection.
        # 
        # "In all the other cases: use approximate estimate of
        #  min(V(A,r), n·σ_θ(r))"
        output_v_a_r = {}
        for attr, v_val in input_v_a_r.items():
            # Formula: min(V(A,r), n_r(σ_θ(r)))
            # Karena kita tidak bisa detect "A op r" secara spesifik,
            # gunakan approximate: min(V(A,r), n_r(output))
            output_v_a_r[attr] = min(v_val, output_n_r)
        
        # Cost = cost input (selection tidak menambah I/O)
        total_cost = input_cost.get("cost", 0)
        
        # Generate unique ID untuk temporary result
        temp_id = f"sigma_{id(node)}"
        self.store_temp_stats(temp_id, output_n_r, output_b_r, input_f_r, output_v_a_r)
        
        return {
            "operation": "SELECTION",
            "condition": condition,
            "cost": total_cost,
            "n_r": output_n_r,
            "b_r": output_b_r,
            "f_r": input_f_r,
            "v_a_r": output_v_a_r,
            "selectivity": selectivity,
            "description": f"Filter: {condition} (selectivity={selectivity:.2f})"
        }
    
    def cost_projection(self, node: QueryTree, input_cost: dict) -> dict:
        """
        Cost untuk operasi PROJECTION (Π - pi).
        
        Rumus dari slide "Size Estimation for Other Operations":
        - Projection: estimated size of Π_A(r) = V(A,r)
        - Jika tanpa DISTINCT: size = n_r (sama dengan input)
        - Jika dengan DISTINCT: size = V(A,r)
        
        Cost = cost(input) (tidak ada tambahan I/O signifikan)
        
        V(A,r) estimation:
        - "They are the same in Π_A(r) as in r"
        - Untuk projected attributes, V values tetap sama
        """
        columns = node.val
        
        # Projection biasanya tidak mengubah jumlah tuples (kecuali DISTINCT)
        # Asumsi: tidak ada DISTINCT (karena tidak ada info di query tree)
        output_n_r = input_cost.get("n_r", 1000)
        output_b_r = input_cost.get("b_r", 100)
        output_f_r = input_cost.get("f_r", 10)
        
        # V(A,r) untuk projected attributes tetap sama
        input_v_a_r = input_cost.get("v_a_r", {})
        output_v_a_r = input_v_a_r.copy()  # preserve distinct values
        
        # Cost = cost input (projection overhead minimal)
        total_cost = input_cost.get("cost", 0)
        
        return {
            "operation": "PROJECTION",
            "columns": columns,
            "cost": total_cost,
            "n_r": output_n_r,
            "b_r": output_b_r,
            "f_r": output_f_r,
            "v_a_r": output_v_a_r,
            "description": f"Project columns: {columns}"
        }
    
    def cost_join(self, node: QueryTree, left_cost: dict, right_cost: dict) -> dict:
        """
        Cost untuk operasi JOIN (⋈ - bowtie).
        Menggunakan Nested-Loop Join sebagai default.
        
        === NESTED-LOOP JOIN ===
        Rumus: Cost = b_r(R) + n_r(R) * b_r(S)
        - Scan R sekali: b_r(R)
        - Untuk setiap tuple di R, scan S: n_r(R) * b_r(S)
        
        === ESTIMATION OF JOIN SIZE ===
        Dari slide "Estimation of the Size of Joins":
        
        Case 1: R ∩ S = ∅ (no common attributes)
        - Cartesian product: n_r(R ⋈ S) = n_r(R) * n_r(S)
        
        Case 2: R ∩ S is a key for R
        - A tuple of S joins with at most one tuple from R
        - n_r(R ⋈ S) ≤ n_r(S)
        
        Case 3: R ∩ S is a foreign key in S referencing R
        - n_r(R ⋈ S) = n_r(S) (exactly)
        
        Case 4: R ∩ S = {A} not a key for R or S
        - Formula: n_r(R ⋈ S) = (n_r(R) * n_r(S)) / max(V(A,R), V(A,S))
        - "Take the lower of these two estimates"
        
        === ESTIMATION OF V(A, R ⋈ S) ===
        - If all attributes in A are from R:
          V(A, R ⋈ S) = min(V(A,R), n_r(R⋈S))
        - If A contains attributes from both R and S:
          V(A, R⋈S) = min(V(A1,R)*V(A2-A1,S), V(A1-A2,R)*V(A2,S), n_r(R⋈S))
        
        TODO: Implementasi Hash Join dan Sort-Merge Join
        - Hash Join: 3 * (b_r(R) + b_r(S))
        - Sort-Merge Join: b_r(R) + b_r(S) + sort_cost
        """
        left_n_r = left_cost.get("n_r", 1000)
        left_b_r = left_cost.get("b_r", 100)
        left_v_a_r = left_cost.get("v_a_r", {})
        
        right_n_r = right_cost.get("n_r", 1000)
        right_b_r = right_cost.get("b_r", 100)
        right_v_a_r = right_cost.get("v_a_r", {})
        
        # === COST CALCULATION (Nested-Loop Join) ===
        # Formula: Cost = b_r(R) + n_r(R) * b_r(S)
        join_cost = left_b_r + (left_n_r * right_b_r)
        total_cost = left_cost.get("cost", 0) + right_cost.get("cost", 0) + join_cost
        
        # === SIZE ESTIMATION ===
        # Karena kita tidak tahu join attribute atau key info,
        # gunakan Case 4: R ∩ S = {A} not a key
        # Formula: n_r(R ⋈ S) = (n_r(R) * n_r(S)) / max(V(A,R), V(A,S))
        
        # Heuristic: asumsi ada common attribute dengan V(A,R) dan V(A,S)
        # Ambil rata-rata dari distinct values sebagai estimasi
        avg_v_left = sum(left_v_a_r.values()) / len(left_v_a_r) if left_v_a_r else 100
        avg_v_right = sum(right_v_a_r.values()) / len(right_v_a_r) if right_v_a_r else 100
        max_v = max(avg_v_left, avg_v_right)
        
        if max_v > 0:
            # Formula: n_r(R ⋈ S) = (n_r(R) * n_r(S)) / max(V(A,R), V(A,S))
            output_n_r = int((left_n_r * right_n_r) / max_v)
        else:
            # Fallback: cartesian product dengan selectivity 0.1
            output_n_r = int(left_n_r * right_n_r * 0.1)
        
        # Estimasi blocking factor untuk join result
        # Asumsi: f_r = average dari kedua input
        output_f_r = (left_cost.get("f_r", 10) + right_cost.get("f_r", 10)) // 2
        
        # Estimasi blocks untuk join result
        # Formula: b_r = ceil(n_r / f_r)
        output_b_r = max(1, math.ceil(output_n_r / output_f_r)) if output_f_r > 0 else (left_b_r + right_b_r)
        
        # === V(A, R ⋈ S) ESTIMATION ===
        # Formula dari slide:
        # 1. "If all attributes in A are from r:
        #     estimated V(A, r ⋈ s) = min(V(A,r), n_r⋈s)"
        # 2. "If A contains attributes A1 from r and A2 from s, then estimated:
        #     V(A,r⋈s) = min(V(A1,r)*V(A2-A1,s), V(A1-A2,r)*V(A2,s), n_r⋈s)"
        #
        # Implementation: Karena kita tidak tahu attribute overlap,
        # gunakan formula 1 untuk attributes dari masing-masing table
        output_v_a_r = {}
        
        # Attributes dari left table (R)
        for attr, v_val in left_v_a_r.items():
            # Formula: V(A, r⋈s) = min(V(A,r), n_r⋈s)
            output_v_a_r[attr] = min(v_val, output_n_r)
        
        # Attributes dari right table (S)
        for attr, v_val in right_v_a_r.items():
            if attr in output_v_a_r:
                # Join attribute (common attribute)
                # Formula: V(A, r⋈s) = min(V(A,r), V(A,s))
                # Karena join on common key
                output_v_a_r[attr] = min(output_v_a_r[attr], v_val, output_n_r)
            else:
                # Attribute only from S
                # Formula: V(A, r⋈s) = min(V(A,s), n_r⋈s)
                output_v_a_r[attr] = min(v_val, output_n_r)
        
        # Store temporary stats
        temp_id = f"join_{id(node)}"
        self.store_temp_stats(temp_id, output_n_r, output_b_r, output_f_r, output_v_a_r)
        
        return {
            "operation": "JOIN",
            "join_type": node.val if node.val else "INNER",
            "cost": total_cost,
            "n_r": output_n_r,
            "b_r": output_b_r,
            "f_r": output_f_r,
            "v_a_r": output_v_a_r,
            "join_cost": join_cost,
            "description": f"Nested-Loop Join (cost={join_cost})"
        }
    
    def cost_sort(self, node: QueryTree, input_cost: dict) -> dict:
        """
        Cost untuk operasi SORT (ORDER BY).
        Menggunakan External Merge Sort.
        
        Rumus: Cost ≈ 2 * b_r * (1 + ⌈log_{M-1}(b_r/M)⌉)
        dimana:
        - M = jumlah blocks yang fit di memory buffer
        - b_r = jumlah blocks input
        - Jika b_r ≤ M: sort in-memory, cost = b_r (satu pass)
        
        Penjelasan:
        - 2 * b_r: setiap block dibaca dan ditulis di setiap pass
        - ⌈log_{M-1}(b_r/M)⌉: jumlah merge passes
        """
        input_b_r = input_cost.get("b_r", 100)
        input_n_r = input_cost.get("n_r", 1000)
        
        # Asumsi: memory dapat hold 100 blocks
        M = 100
        
        if input_b_r <= M:
            # In-memory sort: hanya satu pass
            sort_cost = input_b_r
        else:
            # External merge sort
            # Formula: 2 * b_r * (1 + ⌈log_{M-1}(b_r/M)⌉)
            num_runs = math.ceil(input_b_r / M)
            num_passes = math.ceil(math.log(num_runs, M - 1)) if M > 1 else 1
            sort_cost = 2 * input_b_r * (1 + num_passes)
        
        total_cost = input_cost.get("cost", 0) + sort_cost
        
        # Sort tidak mengubah n_r, b_r, atau v_a_r
        return {
            "operation": "SORT",
            "sort_key": node.val,
            "cost": total_cost,
            "n_r": input_cost.get("n_r", 1000),
            "b_r": input_cost.get("b_r", 100),
            "f_r": input_cost.get("f_r", 10),
            "v_a_r": input_cost.get("v_a_r", {}),
            "sort_cost": sort_cost,
            "description": f"External Merge Sort (cost={sort_cost})"
        }
    
    def cost_limit(self, node: QueryTree, input_cost: dict) -> dict:
        """
        Cost untuk operasi LIMIT.
        
        Jika LIMIT sangat kecil, bisa early termination.
        Cost reduction: cost * (limit / n_r)
        """
        limit_val = int(node.val) if node.val.isdigit() else 100
        input_n_r = input_cost.get("n_r", 1000)
        
        # Output limited to min(limit, n_r)
        output_n_r = min(limit_val, input_n_r)
        
        # Cost reduction dengan early termination
        if input_n_r > 0:
            reduction_factor = min(1.0, output_n_r / input_n_r)
        else:
            reduction_factor = 1.0
        
        total_cost = input_cost.get("cost", 0) * reduction_factor
        
        # Blocks juga reduced
        output_b_r = max(1, int(input_cost.get("b_r", 100) * reduction_factor))
        
        return {
            "operation": "LIMIT",
            "limit": limit_val,
            "cost": total_cost,
            "n_r": output_n_r,
            "b_r": output_b_r,
            "f_r": input_cost.get("f_r", 10),
            "v_a_r": input_cost.get("v_a_r", {}),
            "description": f"Limit to {limit_val} records"
        }
    
    def cost_or(self, node: QueryTree) -> dict:
        """
        Cost untuk operasi OR (DISJUNCTION).
        
        Struktur tree untuk OR:
        OR (root)
        ├── SIGMA: condition1
        │   └── TABLE
        ├── SIGMA: condition2
        │   └── TABLE
        └── SIGMA: condition3
            └── TABLE
        
        Strategy:
        1. Collect semua conditions dari SIGMA children
        2. Gunakan formula disjunction untuk size estimation
        3. Cost = scan table sekali + apply combined OR predicate
        
        Formula dari slide:
        - Disjunction: n_r * (1 - ∏(1 - s_i))
        - Cost: b_r (optimized: hanya 1x table scan)
        
        V(A,r) untuk output: V(A,r) * combined_selectivity
        """
        if not node.childs:
            return {"cost": 0, "n_r": 0, "b_r": 0, "f_r": 10, "v_a_r": {}}
        
        # Collect all conditions from SIGMA children
        conditions = []
        base_table_cost = None
        base_v_a_r = {}
        base_n_r = 0
        
        for child in node.childs:
            if child.type == "SIGMA" and child.childs:
                conditions.append(child.val)
                # Get base table stats (semua branch pake table yang sama)
                if base_table_cost is None:
                    grandchild = child.childs[0]
                    if grandchild.type == "TABLE":
                        base_table_cost = self.cost_table_scan(grandchild)
                        base_v_a_r = base_table_cost.get("v_a_r", {})
                        base_n_r = base_table_cost.get("n_r", 1000)
        
        if not conditions or base_table_cost is None:
            # Fallback: hitung children separately
            child_costs = [self.calculate_cost(c) for c in node.childs]
            total_cost = sum(c.get("cost", 0) for c in child_costs)
            total_n_r = max(c.get("n_r", 0) for c in child_costs)
            return {
                "operation": "OR",
                "cost": total_cost,
                "n_r": total_n_r,
                "b_r": base_table_cost.get("b_r", 100) if base_table_cost else 100,
                "f_r": base_table_cost.get("f_r", 10) if base_table_cost else 10,
                "v_a_r": base_v_a_r,
                "description": f"OR with {len(node.childs)} branches"
            }
        
        # Gunakan formula disjunction untuk size estimation
        # Formula: n_r * (1 - (1-s_1)*(1-s_2)*...*(1-s_n))
        combined_selectivity = self.estimate_disjunction_selectivity(conditions, base_v_a_r)
        output_n_r = max(1, int(base_n_r * combined_selectivity))
        
        # Cost: scan table + apply combined predicate
        # Optimized: hanya perlu 1x scan dengan combined OR predicate
        total_cost = base_table_cost.get("cost", 0)
        
        # Output blocks
        output_f_r = base_table_cost.get("f_r", 10)
        output_b_r = max(1, math.ceil(output_n_r / output_f_r)) if output_f_r > 0 else base_table_cost.get("b_r", 100)
        
        # V(A,r) untuk output
        # Formula: V(A, σ_θ1∨θ2(r)) ≈ V(A,r) * combined_selectivity
        output_v_a_r = {}
        for attr, v_val in base_v_a_r.items():
            output_v_a_r[attr] = max(1, int(v_val * combined_selectivity))
        
        # Store temp stats
        temp_id = f"or_{id(node)}"
        self.store_temp_stats(temp_id, output_n_r, output_b_r, output_f_r, output_v_a_r)
        
        return {
            "operation": "OR",
            "conditions": conditions,
            "cost": total_cost,
            "n_r": output_n_r,
            "b_r": output_b_r,
            "f_r": output_f_r,
            "v_a_r": output_v_a_r,
            "selectivity": combined_selectivity,
            "description": f"OR with {len(conditions)} conditions (selectivity={combined_selectivity:.2f})"
        }
    
    def cost_aggregation(self, node: QueryTree, input_cost: dict) -> dict:
        """
        Cost untuk operasi AGGREGATION (GROUP BY, COUNT, SUM, AVG, etc).
        
        Rumus dari slide "Size Estimation for Other Operations":
        - Aggregation: estimated size of _A G_F(r) = V(A,r)
        - Untuk GROUP BY A: output tuples = V(A,r)
        
        Cost estimation:
        - Hash-based: cost ≈ cost(input) + b_r (build hash table)
        - Sort-based: cost ≈ cost(input) + sort_cost
        
        Asumsi: menggunakan hash-based aggregation
        """
        input_n_r = input_cost.get("n_r", 1000)
        input_b_r = input_cost.get("b_r", 100)
        input_v_a_r = input_cost.get("v_a_r", {})
        
        # Estimasi output size
        # Formula: output = V(A,r) untuk GROUP BY A
        # Heuristic: asumsi 10% dari input tuples (jika tidak tahu attribute)
        if input_v_a_r:
            # Ambil average distinct values sebagai estimasi output groups
            avg_v = sum(input_v_a_r.values()) / len(input_v_a_r)
            output_n_r = int(min(avg_v, input_n_r * 0.1))
        else:
            output_n_r = max(1, int(input_n_r * 0.1))
        
        # Hash table build cost
        agg_cost = input_b_r
        total_cost = input_cost.get("cost", 0) + agg_cost
        
        # Output blocks
        output_f_r = input_cost.get("f_r", 10)
        output_b_r = max(1, math.ceil(output_n_r / output_f_r)) if output_f_r > 0 else input_b_r
        
        # V(A,r) untuk aggregated values
        # "For min(A) and max(A), the number of distinct values can be estimated as 
        #  min(V(A,r), V(G,r)) where G denotes grouping attributes"
        output_v_a_r = {}
        for attr, v_val in input_v_a_r.items():
            output_v_a_r[attr] = min(v_val, output_n_r)
        
        return {
            "operation": "AGGREGATION",
            "aggregate": node.val,
            "cost": total_cost,
            "n_r": output_n_r,
            "b_r": output_b_r,
            "f_r": output_f_r,
            "v_a_r": output_v_a_r,
            "agg_cost": agg_cost,
            "description": f"Aggregation: {node.val} (cost={agg_cost})"
        }
    
    # =================================================================== MAIN COST PLANNING ======================================================================
    
    def calculate_cost(self, node: QueryTree) -> dict:
        """
        Recursively calculate cost untuk query tree.
        Bottom-up approach: calculate children first, then parent.
        """
        if node.type == "TABLE":
            return self.cost_table_scan(node)
        
        elif node.type == "SIGMA" or node.type == "SELECT":
            # Selection operation
            if not node.childs:
                return {"cost": 0, "n_r": 0, "b_r": 0, "f_r": 1, "v_a_r": {}}
            child_cost = self.calculate_cost(node.childs[0])
            return self.cost_selection(node, child_cost)
        
        elif node.type == "OR":
            # OR operation (disjunction)
            return self.cost_or(node)
        
        elif node.type == "PROJECT":
            # Projection operation
            if not node.childs:
                return {"cost": 0, "n_r": 0, "b_r": 0, "f_r": 1, "v_a_r": {}}
            child_cost = self.calculate_cost(node.childs[0])
            return self.cost_projection(node, child_cost)
        
        elif node.type == "JOIN":
            # Join operation
            if len(node.childs) < 2:
                return {"cost": 0, "n_r": 0, "b_r": 0, "f_r": 1, "v_a_r": {}}
            left_cost = self.calculate_cost(node.childs[0])
            right_cost = self.calculate_cost(node.childs[1])
            return self.cost_join(node, left_cost, right_cost)
        
        elif node.type == "SORT" or node.type == "ORDER":
            # Sort operation
            if not node.childs:
                return {"cost": 0, "n_r": 0, "b_r": 0, "f_r": 1, "v_a_r": {}}
            child_cost = self.calculate_cost(node.childs[0])
            return self.cost_sort(node, child_cost)
        
        elif node.type == "LIMIT":
            # Limit operation
            if not node.childs:
                return {"cost": 0, "n_r": 0, "b_r": 0, "f_r": 1, "v_a_r": {}}
            child_cost = self.calculate_cost(node.childs[0])
            return self.cost_limit(node, child_cost)
        
        elif node.type in ["GROUP", "AGGREGATE", "COUNT", "SUM", "AVG"]:
            # Aggregation operations
            if not node.childs:
                return {"cost": 0, "n_r": 0, "b_r": 0, "f_r": 1, "v_a_r": {}}
            child_cost = self.calculate_cost(node.childs[0])
            return self.cost_aggregation(node, child_cost)
        
        else:
            # Unknown operation, just pass through child cost
            if node.childs:
                return self.calculate_cost(node.childs[0])
            return {"cost": 0, "n_r": 0, "b_r": 0, "f_r": 1, "v_a_r": {}}
    

    #================================= MAIN FUNCTION, PANGGIL INI ===============================================
    def get_cost(self, query: ParsedQuery) -> int:
        """
        Fungsi utama untuk mendapatkan cost dari query.
        
        Args:
            query: ParsedQuery object dengan query_tree
        
        Returns:
            int: total cost (dalam unit block I/O)
        
        Usage:
            planner = CostPlanner()
            cost = planner.get_cost(parsed_query)
            print(f"Query cost: {cost}")
        """
        if not query.query_tree:
            raise ValueError("No query tree available in ParsedQuery")
        
        cost_info = self.calculate_cost(query.query_tree)
        return int(cost_info.get("cost", 0))
    
    
    def plan_query(self, parsed_query: ParsedQuery) -> dict:
        """
        Mendapatkan detailed cost breakdown dari query.
        Untuk mendapatkan hanya cost integer, gunakan get_cost().
        
        Args:
            parsed_query: ParsedQuery object
        
        Returns:
            dict: cost plan dengan breakdown lengkap
        """
        if not parsed_query.query_tree:
            return {
                "query": parsed_query.query,
                "error": "No query tree available",
                "total_cost": 0
            }
        
        cost_info = self.calculate_cost(parsed_query.query_tree)
        
        return {
            "query": parsed_query.query,
            "total_cost": cost_info.get("cost", 0),
            "estimated_records": cost_info.get("n_r", 0),
            "blocks_read": cost_info.get("b_r", 0),
            "details": cost_info
        }
    
    def print_cost_breakdown(self, cost_plan: dict):
        """
        Pretty print cost breakdown.
        """
        print("=" * 60)
        print("QUERY COST PLAN")
        print("=" * 60)
        print(f"Query: {cost_plan.get('query', 'N/A')}")
        print(f"Total Cost: {cost_plan.get('total_cost', 0):.2f}")
        print(f"Estimated Records: {cost_plan.get('estimated_records', 0)}")
        print(f"Blocks Read: {cost_plan.get('blocks_read', 0)}")
        print("=" * 60)
        
        if 'details' in cost_plan:
            self._print_details(cost_plan['details'], indent=0)
    
    def _print_details(self, details: dict, indent: int):
        """
        Helper untuk print nested details.
        """
        prefix = "  " * indent
        print(f"{prefix}Operation: {details.get('operation', 'Unknown')}")
        print(f"{prefix}Cost: {details.get('cost', 0):.2f}")
        print(f"{prefix}Description: {details.get('description', 'N/A')}")
        print()
    
    # ======================= HELPER FUNCTIONS (for backward compatibility) =======================
    
    def get_blocking_factor(self, table_name: str) -> int:
        """
        Get blocking factor f_r untuk tabel.
        f_r = number of records per block
        
        Args:
            table_name: nama tabel
            
        Returns:
            int: blocking factor (records per block)
        """
        stats = self.get_table_stats(table_name)
        return stats.get('f_r', 10)
    
    def get_total_blocks(self, table_name: str) -> int:
        """
        Get total blocks b_r untuk tabel.
        b_r = number of blocks
        
        Args:
            table_name: nama tabel
            
        Returns:
            int: total blocks
        """
        stats = self.get_table_stats(table_name)
        return stats.get('b_r', 100)
    
    def get_total_records(self, table_name: str) -> int:
        """
        Get total records n_r untuk tabel.
        n_r = number of tuples/records
        
        Args:
            table_name: nama tabel
            
        Returns:
            int: total records
        """
        stats = self.get_table_stats(table_name)
        return stats.get('n_r', 1000)


