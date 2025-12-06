from model.parsed_query import ParsedQuery
from model.query_tree import (
    QueryTree,
    SetClause,
    TableReference,
    InsertData,
    CreateTableData,
    DropTableData
)
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
    _get_columns_from_select,
    validate_query,
    _get_condition_from_where,
    _get_limit,
    _get_column_from_group_by,
    _parse_from_clause,
    _extract_set_conditions,
    _extract_table_update,
    _extract_table_delete,
    _extract_table_insert,
    _extract_columns_insert,
    _extract_values_insert,
    decompose_conjunctive_selection,
    swap_selection_order,
    eliminate_redundant_projections,
    push_selection_through_join_single,
    push_selection_through_join_split,
    push_projection_through_join_simple,
    push_projection_through_join_with_join_attrs,
    _get_order_by_info,
    _parse_drop_table,
    _parse_create_table,
    parse_where_condition,
    parse_columns_from_string,
    parse_order_by_string,
    parse_group_by_string,
    parse_insert_columns_string,
    parse_insert_values_string,
    _theta_pred,
    _extract_upper_operators,
    _reattach_upper_operators,
    _is_natural,
    _is_theta,
)

from helper.stats import get_stats
import random

class OptimizationEngine:

    def __init__(self, storage_manager=None):
        self.storage_manager = storage_manager
        
        # GA Configuration
        self.use_ga = True
        self.ga_population_size = 20
        self.ga_generations = 50
        self.ga_mutation_rate = 0.2
        self.ga_crossover_rate = 0.7
        self.ga_tournament_size = 3
        self.ga_elite_size = 2
        self.ga_threshold_tables = 4  # Pakai GA saat tables >= threshold
        
        # Optimization tracking
        self.last_optimization_info = {}
    
    # parse sql query string dan return ParsedQuery object
    def parse_query(self, query: str) -> ParsedQuery:
        if not query:
            raise Exception("Query is empty")
        
        is_valid, message = validate_query(query)
        if not is_valid:
            raise Exception(f"Query validation failed: {message}")
        
        q = query.strip().rstrip(';').strip()
        
        parse_result = ParsedQuery(query=query)
        
        try:
            if q.upper().startswith("SELECT"):
                current_root = None
                last_node = None
                
                # 1. parse PROJECT (SELECT columns)
                columns_str = _get_columns_from_select(q)
                if columns_str != "*":
                    columns_list = parse_columns_from_string(columns_str)
                    proj = QueryTree(type="PROJECT", val=columns_list)
                    current_root = proj
                    last_node = proj
                
                # 2. parse LIMIT
                if "LIMIT" in q.upper():
                    limit_val = _get_limit(q)
                    lim = QueryTree(type="LIMIT", val=limit_val)
                    
                    if last_node:
                        last_node.add_child(lim)
                    else:
                        current_root = lim
                    last_node = lim
                
                # 3. parse ORDER BY
                if "ORDER BY" in q.upper():
                    order_info_str = _get_order_by_info(q)
                    order_by_list = parse_order_by_string(order_info_str)
                    
                    sort = QueryTree(type="SORT", val=order_by_list)
                    
                    if last_node:
                        last_node.add_child(sort)
                    else:
                        current_root = sort
                    last_node = sort
                
                # 4. parse GROUP BY
                if "GROUP BY" in q.upper():
                    group_col_str = _get_column_from_group_by(q)
                    group_by_list = parse_group_by_string(group_col_str)
                    
                    group = QueryTree(type="GROUP", val=group_by_list)
                    
                    if last_node:
                        last_node.add_child(group)
                    else:
                        current_root = group
                    last_node = group
                
                # 5. parse WHERE (SIGMA)
                if "WHERE" in q.upper():
                    where_cond_str = _get_condition_from_where(q)
                    condition = parse_where_condition(where_cond_str)
                    
                    sigma = QueryTree(type="SIGMA", val=condition)
                    
                    if last_node:
                        last_node.add_child(sigma)
                    else:
                        current_root = sigma
                    last_node = sigma
                
                # 6. parse FROM
                if last_node:
                    from_node = _parse_from_clause(q)
                    last_node.add_child(from_node)
                else:
                    from_node = _parse_from_clause(q)
                    current_root = from_node
                
                parse_result.query_tree = current_root if current_root else from_node
          
            elif q.upper().startswith("UPDATE"):
                current_root = None
                last_node = None
                
                # 1. parse SET 
                set_conditions_list = _extract_set_conditions(q)
                
                set_clauses = []
                for set_cond in set_conditions_list:
                    if '=' in set_cond:
                        eq_pos = set_cond.find('=')
                        column = set_cond[:eq_pos].strip()
                        value = set_cond[eq_pos + 1:].strip()
                        set_clauses.append(SetClause(column, value))
                
                update_node = QueryTree(type="UPDATE", val=set_clauses)
                
                current_root = update_node
                last_node = update_node
                
                # 2. parse WHERE (optional)
                if "WHERE" in q.upper():
                    where_cond_str = _get_condition_from_where(q)
                    condition = parse_where_condition(where_cond_str)
                    
                    sigma = QueryTree(type="SIGMA", val=condition)
                    
                    last_node.add_child(sigma)
                    last_node = sigma
                
                # 3. parse table name
                table_str = _extract_table_update(q)
                table_ref = TableReference(table_str)
                
                table_node = QueryTree(type="TABLE", val=table_ref)
                
                last_node.add_child(table_node)
                
                parse_result.query_tree = current_root

            elif q.upper().startswith("DELETE"):
                current_root = None
                last_node = None
                
                # 1. DELETE node
                delete_node = QueryTree(type="DELETE", val=None)
                current_root = delete_node
                last_node = delete_node
                
                # 2. parse WHERE
                if "WHERE" in q.upper():
                    where_cond_str = _get_condition_from_where(q)
                    condition = parse_where_condition(where_cond_str)
                    
                    sigma = QueryTree(type="SIGMA", val=condition)
                    
                    last_node.add_child(sigma)
                    last_node = sigma
                
                # 3. parse table
                table_str = _extract_table_delete(q)
                table_ref = TableReference(table_str)
                
                table_node = QueryTree(type="TABLE", val=table_ref)
                
                last_node.add_child(table_node)
                parse_result.query_tree = current_root
            
            elif q.upper().startswith("INSERT"):
                table_name = _extract_table_insert(q)
                columns_str = _extract_columns_insert(q)
                values_str = _extract_values_insert(q)
                
                columns_list = parse_insert_columns_string(columns_str)
                values_list = parse_insert_values_string(values_str)
                
                insert_data = InsertData(table_name, columns_list, values_list)
                insert_node = QueryTree(type="INSERT", val=insert_data)
                
                parse_result.query_tree = insert_node
            
            elif q.upper().startswith("CREATE"):
                table_name, columns, primary_key, foreign_keys = _parse_create_table(q)
                
                create_data = CreateTableData(table_name, columns, primary_key, foreign_keys)
                create_node = QueryTree(type="CREATE_TABLE", val=create_data)
                
                parse_result.query_tree = create_node

            elif q.upper().startswith("DROP"):
                table_name, is_cascade = _parse_drop_table(q)
                
                drop_data = DropTableData(table_name, is_cascade)
                drop_node = QueryTree(type="DROP_TABLE", val=drop_data)
                
                parse_result.query_tree = drop_node

            elif q.upper().startswith("BEGIN"):
                begin_node = QueryTree(type="BEGIN_TRANSACTION", val=None)
                parse_result.query_tree = begin_node
            
            elif q.upper().startswith("COMMIT"):
                commit_node = QueryTree(type="COMMIT", val=None)
                parse_result.query_tree = commit_node
            
            elif q.upper().startswith("ROLLBACK"):
                rollback_node = QueryTree(type="ROLLBACK", val=None)
                parse_result.query_tree = rollback_node
            
            else:
                raise Exception(f"Unsupported query type: {q[:20]}")
                
        except Exception as e:
            raise Exception(f"Error parsing query: {str(e)}")
        
        return parse_result

    def optimize_query(self, parsed_query: ParsedQuery) -> ParsedQuery:
        if not parsed_query or not parsed_query.query_tree:
            return parsed_query

        # 1) START WITH ORIGINAL ROOT
        root = parsed_query.query_tree

        # 2) APPLY NON-JOIN RULES (push-down & simplify)
        changed = True
        max_iter = 5
        while changed and max_iter > 0:
            prev = repr(root)
            root = self._apply_non_join_rules(root)
            changed = (repr(root) != prev)
            max_iter -= 1

        # 3) APPLY JOIN RULES (fold selection, assoc, commutative)
        root = fold_selection_with_cartesian(root)
        root = merge_selection_into_join(root)
        root = make_join_commutative(root)
        root = associate_natural_join(root)
        root = associate_theta_join(root)

        # 4) TABLE EXTRACTION
        tables = list(_tables_under(root)) if root else []
        if len(tables) <= 1:
            self.last_optimization_info = {
                'num_tables': len(tables),
                'method': 'none',
                'reason': 'Single or no tables'
            }
            return ParsedQuery(parsed_query.query, root)

        # 5) EXTRACT NON-JOIN OPERATORS
        upper_operators = _extract_upper_operators(root)

        # 6) BUILD JOIN CONDITIONS FROM CURRENT TREE
        join_conditions, natural_joins = self._extract_join_info(root)

        # 7) GET STATS
        stats = get_stats()

        # 8) CHOOSE OPTIMIZATION METHOD BASED ON TABLE COUNT
        best_join_tree = None
        optimization_info = {
            'num_tables': len(tables),
            'tables': tables
        }
        
        if self.use_ga and len(tables) >= self.ga_threshold_tables:
            # Use both GA and Heuristic, compare results
            ga_tree, ga_cost, ga_details = self._genetic_algorithm_optimize(
                tables, join_conditions, natural_joins, stats
            )
            heuristic_tree, heuristic_cost = self._heuristic_optimize(
                tables, join_conditions, natural_joins, stats
            )
            
            # Choose the better one
            if ga_cost < heuristic_cost:
                best_join_tree = ga_tree
                optimization_info.update({
                    'method': 'genetic_algorithm',
                    'ga_cost': ga_cost,
                    'heuristic_cost': heuristic_cost,
                    'improvement': heuristic_cost - ga_cost,
                    'improvement_pct': ((heuristic_cost - ga_cost) / heuristic_cost * 100) if heuristic_cost > 0 else 0,
                    'ga_generations': ga_details['generations_run'],
                    'ga_converged': ga_details['converged']
                })
            else:
                best_join_tree = heuristic_tree
                optimization_info.update({
                    'method': 'heuristic',
                    'ga_cost': ga_cost,
                    'heuristic_cost': heuristic_cost,
                    'reason': 'heuristic_better'
                })
        else:
            # Use heuristic only for simple queries
            best_join_tree, cost = self._heuristic_optimize(
                tables, join_conditions, natural_joins, stats
            )
            optimization_info.update({
                'method': 'heuristic',
                'cost': cost,
                'reason': f'table_count_below_threshold ({len(tables)} < {self.ga_threshold_tables})'
            })

        # Store optimization info for later inspection
        self.last_optimization_info = optimization_info

        # 9) REATTACH UPPER OPERATORS TO BEST JOIN TREE
        final_tree = _reattach_upper_operators(best_join_tree, upper_operators)

        # 10) RETURN BEST PLAN AS FINAL OPTIMIZED QUERY TREE
        return ParsedQuery(parsed_query.query, final_tree)

    def _heuristic_optimize(self, tables, join_conditions, natural_joins, stats):
        orders = _some_permutations(tables, max_count=10)
        plans = []
        for order in orders:
            plan = build_join_tree(order, join_conditions, natural_joins)
            if plan:
                plans.append(plan)
        
        if not plans:
            default_plan = build_join_tree(tables, join_conditions, natural_joins)
            return default_plan, float('inf')
        
        best = choose_best(plans, stats)
        cost = plan_cost(best, stats)
        return best, cost

    def _genetic_algorithm_optimize(self, tables, join_conditions, natural_joins, stats):
        # Initialize population
        population = self._ga_initialize_population(tables)
        
        best_individual = None
        best_cost = float('inf')
        cost_history = []
        
        for generation in range(self.ga_generations):
            # Evaluate fitness for all individuals
            fitness_scores = []
            for individual in population:
                plan = build_join_tree(individual, join_conditions, natural_joins)
                cost = plan_cost(plan, stats) if plan else float('inf')
                fitness_scores.append((individual, cost))
            
            # Sort by cost (lower is better)
            fitness_scores.sort(key=lambda x: x[1])
            
            # Track best individual
            if fitness_scores[0][1] < best_cost:
                best_individual = fitness_scores[0][0]
                best_cost = fitness_scores[0][1]
            
            cost_history.append(best_cost)
            
            # Early stopping if converged
            if generation > 10:
                recent_best = cost_history[-10:]
                if len(set(recent_best)) == 1:  # No improvement in last 10 generations
                    break
            
            # Elitism: preserve best individuals
            new_population = [ind for ind, _ in fitness_scores[:self.ga_elite_size]]
            
            # Generate rest of population through selection, crossover, mutation
            while len(new_population) < self.ga_population_size:
                # Selection: tournament selection
                parent1 = self._ga_tournament_selection(fitness_scores)
                parent2 = self._ga_tournament_selection(fitness_scores)
                
                # Crossover
                if random.random() < self.ga_crossover_rate:
                    child1, child2 = self._ga_crossover(parent1, parent2)
                else:
                    child1, child2 = parent1[:], parent2[:]
                
                # Mutation
                if random.random() < self.ga_mutation_rate:
                    child1 = self._ga_mutate(child1)
                if random.random() < self.ga_mutation_rate:
                    child2 = self._ga_mutate(child2)
                
                new_population.append(child1)
                if len(new_population) < self.ga_population_size:
                    new_population.append(child2)
            
            population = new_population
        
        # Build final best plan
        best_tree = build_join_tree(best_individual, join_conditions, natural_joins)
        
        details = {
            'generations_run': generation + 1,
            'best_order': best_individual,
            'cost_history': cost_history,
            'converged': len(set(cost_history[-10:])) == 1 if len(cost_history) >= 10 else False
        }
        
        return best_tree, best_cost, details

    def _ga_initialize_population(self, tables):
        population = []
        
        # Add original order
        population.append(tables[:])
        
        # Add reversed order
        population.append(tables[::-1])
        
        # Add random permutations
        attempts = 0
        max_attempts = self.ga_population_size * 10
        while len(population) < self.ga_population_size and attempts < max_attempts:
            individual = tables[:]
            random.shuffle(individual)
            # Avoid duplicates
            if individual not in population:
                population.append(individual)
            attempts += 1
        
        # Fill remaining slots if needed
        while len(population) < self.ga_population_size:
            population.append(tables[:])
        
        return population

    def _ga_tournament_selection(self, fitness_scores):
        tournament_size = min(self.ga_tournament_size, len(fitness_scores))
        tournament = random.sample(fitness_scores, tournament_size)
        winner = min(tournament, key=lambda x: x[1])
        return winner[0][:]

    def _ga_crossover(self, parent1, parent2):
        size = len(parent1)
        
        if size <= 1:
            return parent1[:], parent2[:]
        
        # Select two crossover points
        point1 = random.randint(0, size - 1)
        point2 = random.randint(point1 + 1, size)
        
        # Create children
        child1 = [None] * size
        child2 = [None] * size
        
        # Copy segment from parents
        child1[point1:point2] = parent1[point1:point2]
        child2[point1:point2] = parent2[point1:point2]
        
        # Fill remaining positions
        self._ga_fill_child(child1, parent2, point2)
        self._ga_fill_child(child2, parent1, point2)
        
        return child1, child2

    def _ga_fill_child(self, child, parent, start_pos):
        size = len(child)
        current_pos = start_pos % size
        parent_pos = start_pos % size
        
        filled = 0
        max_iterations = size * 2  # Safety limit
        
        while None in child and filled < max_iterations:
            if parent[parent_pos] not in child:
                child[current_pos] = parent[parent_pos]
                current_pos = (current_pos + 1) % size
                filled += 1
            parent_pos = (parent_pos + 1) % size

    def _ga_mutate(self, individual):
        individual = individual[:]
        if len(individual) > 1:
            idx1, idx2 = random.sample(range(len(individual)), 2)
            individual[idx1], individual[idx2] = individual[idx2], individual[idx1]
        return individual

    def get_cost(self, parsed_query: ParsedQuery) -> int:
        if not parsed_query or not parsed_query.query_tree:
            return 0
        
        # Use Storage Manager stats if available, otherwise fallback to dummy stats
        if self.storage_manager:
            from helper.cost import CostPlanner
            try:
                cost_planner = CostPlanner(storage_manager=self.storage_manager)
                return cost_planner.get_cost(parsed_query)
            except Exception as e:
                print(f"[Optimizer] Failed to get cost from SM, using fallback: {e}")
        
        # Fallback to dummy stats
        root = parsed_query.query_tree
        stats = get_stats()
        return plan_cost(root, stats)
    
    def get_optimization_info(self):
        return self.last_optimization_info.copy()
    
    def optimize_query_non_join(self, pq: ParsedQuery) -> ParsedQuery:
        if not pq or not pq.query_tree:
            return pq
        
        root = pq.query_tree
        max_iterations = 5
        for _ in range(max_iterations):
            old_root = root
            root = self._apply_non_join_rules(root)
            if root == old_root:
                break
        
        return ParsedQuery(pq.query, root)

    def _apply_non_join_rules(self, node: QueryTree) -> QueryTree:
        """Apply non-join optimization rules recursively."""
        if not node:
            return node
        
        # Recursively apply to children first
        for i, child in enumerate(node.childs):
            node.childs[i] = self._apply_non_join_rules(child)
        
        # Apply transformation rules
        node = decompose_conjunctive_selection(node)
        node = eliminate_redundant_projections(node)
        node = swap_selection_order(node)
        node = push_selection_through_join_single(node)
        node = push_selection_through_join_split(node)
        node = push_projection_through_join_simple(node)
        node = push_projection_through_join_with_join_attrs(node)
        
        return node
    
    def _extract_join_info(self, tree: QueryTree):
        join_conditions = {}
        natural_joins = set()
        
        def traverse(node):
            if node.type == "JOIN":
                if len(node.childs) >= 2:
                    left_tables = _tables_under(node.childs[0])
                    right_tables = _tables_under(node.childs[1])
                    
                    for lt in left_tables:
                        for rt in right_tables:
                            key = frozenset({lt, rt})
                            
                            if _is_natural(node):
                                natural_joins.add(key)
                            elif _is_theta(node):
                                cond = _theta_pred(node)
                                if cond:
                                    join_conditions[key] = cond
            
            for child in node.childs:
                traverse(child)
        
        traverse(tree)
        return join_conditions, natural_joins