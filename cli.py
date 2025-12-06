from __future__ import annotations

import sys
from typing import Iterable, Sequence

from bootstrap import Dependencies, load_dependencies
from MiniDBMS import MiniDBMS


def _print_tree_structure(node, indent="", is_last=True) -> None:
    if not node:
        return
    
    # Print current node
    prefix = indent + ("└── " if is_last else "├── ")
    node_type = node.type if hasattr(node, 'type') else "UNKNOWN"
    node_val = ""
    
    if hasattr(node, 'val') and node.val is not None:
        val = node.val
        if isinstance(val, list):
            if len(val) <= 3:
                node_val = f" {val}"
            else:
                node_val = f" [{len(val)} items]"
        elif isinstance(val, str) and len(val) > 50:
            node_val = f" {val[:47]}..."
        else:
            node_val = f" {val}"
    
    print(f"{prefix}{node_type}{node_val}")
    
    children = None
    if hasattr(node, 'childs') and node.childs:
        children = node.childs
    elif hasattr(node, 'children') and node.children:
        children = node.children
    
    if children:
        for i, child in enumerate(children):
            child_indent = indent + ("    " if is_last else "│   ")
            _print_tree_structure(child, child_indent, i == len(children) - 1)


def _handle_special_command(command: str, dbms: MiniDBMS, deps: Dependencies) -> None:
    parts = command.split()
    cmd = parts[0].lower()

    #  list semua table
    if cmd == "\\dt":
        storage_manager = getattr(dbms.query_processor, 'storage_manager', None)
        if storage_manager and hasattr(storage_manager, 'schema_manager'):
            tables = storage_manager.schema_manager.list_tables()
            if tables:
                print("\n  Tables:")
                print("  +-" + "-" * 30 + "-+")
                print("  | " + "Table Name".ljust(30) + " |")
                print("  +-" + "-" * 30 + "-+")
                for table in sorted(tables):
                    print("  | " + table.ljust(30) + " |")
                print("  +-" + "-" * 30 + "-+")
            else:
                print("  No tables found.")
        else:
            print("  [Error] Storage manager not available")
    
    elif cmd == "\\d":
        # Describe table
        if len(parts) < 2:
            print("  Usage: \\d <table_name>")
            return
        
        table_name = parts[1]
        storage_manager = getattr(dbms.query_processor, 'storage_manager', None)
        if storage_manager and hasattr(storage_manager, 'schema_manager'):
            schema = storage_manager.schema_manager.get_table_schema(table_name)
            if schema:
                print(f"\n  Table: {table_name}")
                print("  +-" + "-" * 20 + "-+-" + "-" * 15 + "-+-" + "-" * 10 + "-+")
                print("  | " + "Column".ljust(20) + " | " + "Type".ljust(15) + " | " + "Key".ljust(10) + " |")
                print("  +-" + "-" * 20 + "-+-" + "-" * 15 + "-+-" + "-" * 10 + "-+")
                
                # Schema uses attributes list, not columns dict
                if hasattr(schema, 'attributes'):
                    for attr in schema.attributes:
                        col_name = attr.get('name', 'unknown')
                        col_type = attr.get('type', 'unknown')
                        print("  | " + col_name.ljust(20) + " | " + col_type.ljust(15) + " | " + "".ljust(10) + " |")
                
                print("  +-" + "-" * 20 + "-+-" + "-" * 15 + "-+-" + "-" * 10 + "-+")
                
                # Show statistics
                stats = storage_manager.get_stats(table_name)
                if stats:
                    print(f"\n  Statistics:")
                    print(f"    Rows: {getattr(stats, 'n_r', 'N/A')}")
                    print(f"    Blocks: {getattr(stats, 'b_r', 'N/A')}")
                    print(f"    Block factor: {getattr(stats, 'f_r', 'N/A')}")
            else:
                print(f"  Table '{table_name}' not found.")
        else:
            print("  [Error] Storage manager not available")
    
    elif cmd == "\\checkpoint":
        print("  Executing checkpoint...")
        if dbms.checkpoint():
            print("  Checkpoint completed successfully.")
        else:
            print("  Checkpoint failed.")
    
    elif cmd == "\\tx" or cmd == "\\transactions":
        if hasattr(dbms, '_active_transactions') and dbms._active_transactions:
            print("  +-" + "-" * 20 + "-+-" + "-" * 15 + "-+-" + "-" * 25 + "-+")
            print("  | " + "Transaction ID".ljust(20) + " | " + "Status".ljust(15) + " | " + "Start Time".ljust(25) + " |")
            print("  +-" + "-" * 20 + "-+-" + "-" * 15 + "-+-" + "-" * 25 + "-+")
            
            if hasattr(dbms, 'concurrency_manager') and hasattr(dbms.concurrency_manager, 'transaction_manager'):
                tx_manager = dbms.concurrency_manager.transaction_manager
                for tx_id in sorted(dbms._active_transactions):
                    if hasattr(tx_manager, 'get_transaction'):
                        tx = tx_manager.get_transaction(tx_id)
                        if tx:
                            status = str(tx.status.name if hasattr(tx.status, 'name') else tx.status)
                            start_time = tx.start_time.strftime("%Y-%m-%d %H:%M:%S") if hasattr(tx, 'start_time') else "N/A"
                            print("  | " + str(tx_id).ljust(20) + " | " + status.ljust(15) + " | " + start_time.ljust(25) + " |")
                    else:
                        print("  | " + str(tx_id).ljust(20) + " | " + "ACTIVE".ljust(15) + " | " + "N/A".ljust(25) + " |")
            else:
                for tx_id in sorted(dbms._active_transactions):
                    print("  | " + str(tx_id).ljust(20) + " | " + "ACTIVE".ljust(15) + " | " + "N/A".ljust(25) + " |")
            
            print("  +-" + "-" * 20 + "-+-" + "-" * 15 + "-+-" + "-" * 25 + "-+")
            print(f"  Total: {len(dbms._active_transactions)} active transaction(s)")
        else:
            print("  No active transactions.")
    
    elif cmd == "explain":
        if len(parts) < 2:
            print("  Usage: explain <query>")
            print("  Example: explain SELECT * FROM Student WHERE GPA > 3.0")
            print("  Note: Query can end with or without semicolon")
            return
        
        query = " ".join(parts[1:]).strip()
        display_query = query.rstrip(";").strip()
        if not query.endswith(";"):
            query = query + ";"
        
        if not query.upper().startswith("SELECT"):
            print("  EXPLAIN only supports SELECT queries.")
            return
        
        try:
            optimizer = getattr(dbms.query_processor, 'optimization_engine', None)
            if not optimizer:
                print("  [Error] Query optimizer not available")
                return
            
            print(f"\n  Query: {display_query}")
            print("\n  " + "=" * 70)
            
            parsed = optimizer.parse_query(query)
            print("\n Original Query Tree:")
            if parsed and parsed.query_tree:
                _print_tree_structure(parsed.query_tree, indent="      ")
            else:
                print("      (empty)")
            
            # cost sblm optimize
            cost_before = optimizer.get_cost(parsed) if parsed else 0
            print(f"\n  Estimated Cost (before optimization): {cost_before}")
            
            # optimize
            optimized = optimizer.optimize_query(parsed)
            
            print("\n  Optimized Query Tree:")
            if optimized and optimized.query_tree:
                _print_tree_structure(optimized.query_tree, indent="      ")
            else:
                print("      (empty)")
            
            # cost sesudah optimize
            cost_after = optimizer.get_cost(optimized) if optimized else 0
            print(f"\n  Estimated Cost (after optimization): {cost_after}")
            
            if cost_before > 0 and cost_after > 0:
                improvement = ((cost_before - cost_after) / cost_before) * 100
                print(f"      Cost reduction: {improvement:.1f}%")
            
            if hasattr(optimizer, 'last_optimization_info') and optimizer.last_optimization_info:
                info = optimizer.last_optimization_info
                print("\n  Optimization Details:")
                if 'method' in info:
                    print(f"      Method: {info['method']}")
                if 'join_order' in info:
                    print(f"      Join order: {info['join_order']}")
                if 'heuristics_applied' in info:
                    print(f"      Heuristics: {', '.join(info['heuristics_applied'])}")
            
            print("\n  " + "=" * 70)
            
        except Exception as e:
            print(f"  [Error] Failed to explain query: {e}")
            import traceback
            traceback.print_exc()
    
    elif cmd == "\\help":
        print("\n  Special Commands:")
        print("    \\dt                  - List all tables")
        print("    \\d <table>           - Describe table schema")
        print("    \\tx, \\transactions   - View all active transactions")
        print("    explain <query>     - Show query execution plan and cost")
        print("    \\checkpoint          - Force checkpoint")
        print("    \\help                - Show this help message")
        print("    exit or quit         - Exit the shell\n")
    
    else:
        print(f"  Unknown command: {cmd}")
        print("  Type \\help for available commands.")


def _print_execution_result(result, deps: Dependencies) -> None:
    is_error = False
    if hasattr(result, 'data') and result.data == -1:
        is_error = True
    if hasattr(result, 'message') and result.message.startswith('Error'):
        is_error = True
    
    if is_error:
        print(f"[Transaction {result.transaction_id}] {result.message}")
        return
    
    print(f"[Transaction {result.transaction_id}] {result.message}")

    if isinstance(result.data, deps.rows_cls):
        if result.data.rows_count == 0:
            print("  Rows: []")
        else:
            # format jadi tabel
            rows = list(result.data.data)
            if not rows:
                print("  Rows: []")
            else:
                if hasattr(result.data, "columns") and result.data.columns:
                    headers = list(result.data.columns)
                    table_rows = [list(r) if not isinstance(r, dict) else [r.get(h) for h in headers] for r in rows]
                else:
                    first = rows[0]
                    if isinstance(first, dict):
                        headers = list(first.keys())
                        table_rows = [[r.get(h) for h in headers] for r in rows]
                    elif isinstance(first, (list, tuple)):
                        headers = [f"col{i}" for i in range(len(first))]
                        table_rows = [list(r) for r in rows]
                    else:
                        headers = ["value"]
                        table_rows = [[r] for r in rows]

                # lebar kolom
                widths = []
                for i, h in enumerate(headers):
                    col_vals = [str(row[i]) for row in table_rows]
                    widths.append(max(len(str(h)), *(len(v) for v in col_vals)))

                # fromat
                header_fmt = "  | " + " | ".join(f"{{:{w}}}" for w in widths) + " |"
                sep = "  +-" + "-+-".join("-" * w for w in widths) + "-+"

                # print
                print(sep)
                print(header_fmt.format(*headers))
                print(sep)
                for row in table_rows:
                    print(header_fmt.format(*[str(v) for v in row]))
                print(sep)
    elif result.data == -1:
        print("  Component returned an error indicator (-1)")
    else:
        print(f"  Data: {result.data}")


def _demo_queries() -> Iterable[str]:
    return [
        "SELECT * FROM Student;",
        "BEGIN TRANSACTION;",
        "UPDATE Student SET GPA = 3.9 WHERE StudentID = 1;",
        "SELECT StudentID, FullName, GPA FROM Student WHERE GPA > 3.0;",
        "COMMIT;",
        "ABORT;",
    ]


def run(argv: Sequence[str] | None = None) -> None:
    args = list(argv if argv is not None else sys.argv[1:])

    deps = load_dependencies()
    dbms = MiniDBMS(deps)

    if args and args[0] == "--interactive":
        _run_interactive(dbms, deps)
    else:
        _run_demo(dbms, deps)


def _run_demo(dbms: MiniDBMS, deps: Dependencies) -> None:
    print("MariaDanB-MiniDBMS (demo)")
    for query in _demo_queries():
        print(f"\n> {query}")
        result = dbms.execute(query)
        _print_execution_result(result, deps)


def _run_interactive(dbms: MiniDBMS, deps: Dependencies) -> None:
    print("MariaDanB-MiniDBMS Interactive Shell")
    print("Commands: \\dt, \\d <table>, \\tx, explain <query>, \\checkpoint, \\help, exit")
    buffer: list[str] = []

    while True:
        prompt = "SQL> " if not buffer else "... "
        try:
            raw_line = input(prompt)
        except EOFError:
            print()
            break
        except KeyboardInterrupt:
            print()
            buffer.clear()
            continue

        stripped = raw_line.strip()

        if not buffer and not stripped:
            continue

        lower = stripped.lower()
        if not buffer and lower in {"exit", "quit"}:
            break
        if buffer and lower in {"exit", "quit"}:
            buffer.clear()
            print("[Info] Cleared pending statement. Type 'exit' again to quit.")
            continue

        if not buffer and (stripped.startswith("\\") or stripped.lower().startswith("explain ")):
            _handle_special_command(stripped, dbms, deps)
            continue

        buffer.append(raw_line)
        statement = "\n".join(buffer).strip()

        if not statement:
            buffer.clear()
            continue

        if statement.endswith(";"):
            buffer.clear()
            try:
                result = dbms.execute(statement)
            except Exception as exc:
                print(f"[Error] {exc}")
                continue
            _print_execution_result(result, deps)
