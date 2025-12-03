class DeadlockDetector:
    def __init__(self):
        # Wait graph. Ti -> set of Tx (Ti menunggu set of Tx melepas lock)
        self.graph = {}

        print("Deadlock Detector")
    
    def add_wait_edge(self, from_trx, to_trx):
        # menambahkan node pada graph
        if from_trx == to_trx:
            return 
        
        self.graph.setdefault(from_trx, set()).add(to_trx)
        self.graph.setdefault(to_trx, set())

    def remove_add_edge(self, from_tx, to_tx):
        # menghapus node pada graph
        if from_tx in self.graph:
            self.graph[from_tx].discard(to_tx)

    def _find_cycles(self):
        # mencari cycle yang ada pada graph
        visited = set()
        stack = []
        onstack = set()
        cycles = []

        def dfs(u):
            visited.add(u)
            stack.append(u)
            onstack.add(u)
            for v in self.graph.get(u, ()):
                if v not in visited:
                    dfs(v)
                elif v in onstack:
                    try:
                        idx = stack.index(v)
                        cycle = stack[idx:].copy()
                        cycles.append(cycle)
                    except ValueError:
                        pass
            stack.pop()
            onstack.remove(u)

        for node in list(self.graph.keys()):
            if node not in visited:
                dfs(node)
        return cycles


    def check_deadlock(self):
        # memeriksa apakah ada deadlock 
        cycle = self._find_cycles()
        has_deadlock = len(cycle) > 0

        if (has_deadlock):
            print(f"Deadlock Detected: {cycle}")

        return has_deadlock, cycle