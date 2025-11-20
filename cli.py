from __future__ import annotations

import sys
from typing import Iterable, Sequence

from bootstrap import Dependencies, load_dependencies
from MiniDBMS import MiniDBMS


def _print_execution_result(result, deps: Dependencies) -> None:
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
    print("MariaDanB-MiniDBMS Interactive Shell (type 'exit' to quit)")
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
