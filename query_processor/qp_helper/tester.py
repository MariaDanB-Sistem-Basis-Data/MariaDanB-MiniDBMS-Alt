import json
import math
import traceback
from collections import Counter
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple
from dataclasses import dataclass, field

@dataclass
class TestCase:
    name: str
    sql: str  # the query to run
    expected_rows: Optional[List[Dict[str, Any]]] = None
    expected_count: Optional[int] = None
    expected_affected_rows: Optional[int] = None  # for non-select statements returning int
    validator: Optional[Callable[[List[Dict[str, Any]]], Tuple[bool, str]]] = None
    setup_sql: Optional[Iterable[str]] = None
    teardown_sql: Optional[Iterable[str]] = None
    ordered: bool = False
    float_epsilon: float = 1e-6

def _is_rows_like(obj: Any) -> bool:
    return hasattr(obj, "data") and isinstance(getattr(obj, "data"), list) and hasattr(obj, "rows_count")

def _item_to_dict(item: Any) -> Dict[str, Any]:
    if isinstance(item, dict):
        return dict(item)
    if hasattr(item, "__dict__"):
        return dict(item.__dict__)
    return {"value": item}

def _normalize_value(v: Any, eps: float):
    if isinstance(v, float):
        # round to epsilon-based digits
        if eps <= 0:
            return v
        digits = max(0, int(-math.log10(eps)) if eps < 1 else 6)
        return round(v, digits)
    return v

def normalize_row(row: Dict[str, Any], eps: float) -> Dict[str, Any]:
    out = {}
    for k, v in row.items():
        if isinstance(k, str):
            key = k.strip().lower()
        else:
            key = k
        out[key] = _normalize_value(v, eps)
    return out

def canonicalize_rows(rows: Iterable[Dict[str, Any]], eps: float) -> List[Dict[str, Any]]:
    return [normalize_row(r, eps) for r in rows]

def _row_key(row: Dict[str, Any]) -> Tuple[Tuple[str, Any], ...]:
    return tuple(sorted(row.items()))

def multiset_diff(actual: List[Dict[str, Any]], expected: List[Dict[str, Any]]):
    a = Counter(_row_key(r) for r in actual)
    e = Counter(_row_key(r) for r in expected)
    missing_keys = list((e - a).elements())
    extra_keys = list((a - e).elements())
    def key_to_row(k): return dict(k)
    return [key_to_row(k) for k in missing_keys], [key_to_row(k) for k in extra_keys]

CSI = "\x1b["
RESET = CSI + "0m"
BOLD = CSI + "1m"
GREEN = CSI + "32m"
RED = CSI + "31m"
YELLOW = CSI + "33m"
CYAN = CSI + "36m"
GRAY = CSI + "90m"

def color(text: str, code: str, enable: bool = True) -> str:
    return (code + text + RESET) if enable else text

def _is_rows_like(obj: Any) -> bool:
    return hasattr(obj, "data") and isinstance(getattr(obj, "data"), list) and hasattr(obj, "rows_count")

def _item_to_dict(item: Any) -> Dict[str, Any]:
    if isinstance(item, dict):
        return dict(item)
    if hasattr(item, "__dict__"):
        return dict(item.__dict__)
    return {"value": item}

def _normalize_value(v: Any, eps: float):
    if isinstance(v, float):
        if eps <= 0:
            return v
        digits = max(0, int(-math.log10(eps)) if eps < 1 else 6)
        return round(v, digits)
    return v

def normalize_row(row: Dict[str, Any], eps: float) -> Dict[str, Any]:
    out = {}
    for k, v in row.items():
        key = (k.strip().lower() if isinstance(k, str) else k)
        out[key] = _normalize_value(v, eps)
    return out

def canonicalize_rows(rows: Iterable[Dict[str, Any]], eps: float) -> List[Dict[str, Any]]:
    return [normalize_row(r, eps) for r in rows]

def _row_key(row: Dict[str, Any]) -> Tuple[Tuple[str, Any], ...]:
    return tuple(sorted(row.items()))

def multiset_diff(actual: List[Dict[str, Any]], expected: List[Dict[str, Any]]):
    a = Counter(_row_key(r) for r in actual)
    e = Counter(_row_key(r) for r in expected)
    missing_keys = list((e - a).elements())
    extra_keys = list((a - e).elements())
    def key_to_row(k): return dict(k)
    return [key_to_row(k) for k in missing_keys], [key_to_row(k) for k in extra_keys]

class QueryTester:
    def __init__(self, qp, verbose: bool = False):
        self.qp = qp
        self.verbose = verbose
        self.results = []

    def _execute(self, sql: str):
        return self.qp.execute_query(sql)

    def _extract_rows_from_result(self, res) -> Tuple[List[Dict[str, Any]], Optional[int]]:
        if res is None:
            return [], None
        data = getattr(res, "data", None)
        if _is_rows_like(data):
            items = data.data
            rows = [_item_to_dict(it) for it in items]
            return rows, None
        if isinstance(data, int):
            return [], data
        if isinstance(data, list):
            rows = [_item_to_dict(it) for it in data]
            return rows, None
        return [], None

    def run_test(self, tc) -> dict:
        ok = False
        msg = ""
        detailed = {
            "name": getattr(tc, "name", "<unnamed>"),
            "sql": getattr(tc, "sql", None),
            "setup_sql": getattr(tc, "setup_sql", None),
            "teardown_sql": getattr(tc, "teardown_sql", None),
            "ok": False,
            "message": None,
            "rows": None,
            "affected": None,
            "expected_rows": getattr(tc, "expected_rows", None),
            "expected_count": getattr(tc, "expected_count", None),
            "expected_affected_rows": getattr(tc, "expected_affected_rows", None),
            "missing": None,
            "extra": None,
            "error": None,
        }

        try:
            # setup
            if getattr(tc, "setup_sql", None):
                for s in tc.setup_sql:
                    if s and s.strip():
                        if self.verbose:
                            print(f"[{tc.name}] setup: {s}")
                        self._execute(s)

            if self.verbose:
                print(f"[{tc.name}] executing: {tc.sql}")
            res = self._execute(tc.sql)

            rows_raw, affected = self._extract_rows_from_result(res)
            eps = getattr(tc, "float_epsilon", 1e-6)
            norm_rows = canonicalize_rows(rows_raw, eps)

            detailed["rows"] = norm_rows
            detailed["affected"] = affected

            # check expected_affected_rows
            if getattr(tc, "expected_affected_rows", None) is not None:
                expected_aff = tc.expected_affected_rows
                if affected is None:
                    ok = False
                    msg = f"expected affected_rows={expected_aff} but got rows/no affected info"
                else:
                    ok = (affected == expected_aff)
                    msg = f"affected_rows: got {affected}, expected {expected_aff}"
                detailed["ok"] = ok
                detailed["message"] = msg
                return self._finalize_test(tc, detailed)

            # expected_count
            if getattr(tc, "expected_count", None) is not None:
                ok = (len(norm_rows) == tc.expected_count)
                msg = f"expected_count: got {len(norm_rows)}, expected {tc.expected_count}"
                detailed["ok"] = ok
                detailed["message"] = msg

            # expected_rows
            if getattr(tc, "expected_rows", None) is not None:
                expected_norm = canonicalize_rows(tc.expected_rows, eps)
                detailed["expected_rows"] = expected_norm
                if getattr(tc, "ordered", False):
                    if len(expected_norm) == len(norm_rows) and all(e == a for e, a in zip(expected_norm, norm_rows)):
                        ok = True
                        msg = "ordered rows matched exactly"
                    else:
                        missing, extra = multiset_diff(norm_rows, expected_norm)
                        detailed["missing"] = missing
                        detailed["extra"] = extra
                        ok = False
                        msg = f"ordered mismatch: missing {len(missing)} extra {len(extra)}"
                else:
                    missing, extra = multiset_diff(norm_rows, expected_norm)
                    detailed["missing"] = missing
                    detailed["extra"] = extra
                    ok = (not missing and not extra)
                    msg = f"unordered mismatch: missing {len(missing)} extra {len(extra)}" if (missing or extra) else "unordered rows matched exactly"

            # validator
            if getattr(tc, "validator", None) is not None:
                try:
                    v_ok, v_msg = tc.validator(norm_rows)
                    ok = bool(v_ok)
                    msg = v_msg
                except Exception as vex:
                    ok = False
                    msg = f"validator raised: {vex}\n{traceback.format_exc()}"
                    detailed["error"] = msg

            # if no assertions, pass but note returned rows
            if tc.expected_rows is None and tc.expected_count is None and getattr(tc, "expected_affected_rows", None) is None and getattr(tc, "validator", None) is None:
                ok = True
                msg = f"No assertions; returned {len(norm_rows)} rows (affected={affected})"

            detailed["ok"] = ok
            detailed["message"] = msg

        except Exception as e:
            detailed["ok"] = False
            tb = traceback.format_exc()
            detailed["message"] = f"ERROR: {e}"
            detailed["error"] = tb

        finally:
            # teardown (try but don't mask main result)
            try:
                if getattr(tc, "teardown_sql", None):
                    for s in tc.teardown_sql:
                        if s and s.strip():
                            if self.verbose:
                                print(f"[{tc.name}] teardown: {s}")
                            self._execute(s)
            except Exception as e2:
                # attach teardown info
                prev = detailed.get("message", "")
                detailed["message"] = prev + f"\nTeardown error: {e2}"
                if "error" not in detailed or detailed["error"] is None:
                    detailed["error"] = f"Teardown error: {e2}"

        self.results.append(detailed)
        # optional immediate print (if verbose True, keep old behavior), else user calls printer
        if self.verbose:
            print(f"[{detailed['name']}] => {'PASS' if detailed['ok'] else 'FAIL'}: {detailed['message']}")
        return detailed

    def _finalize_test(self, tc, detailed):
        # append & print minimal
        self.results.append(detailed)
        if self.verbose:
            print(f"[{detailed['name']}] => {'PASS' if detailed['ok'] else 'FAIL'}: {detailed['message']}")
        return detailed

    def run_suite(self, cases: Iterable[Any]) -> List[dict]:
        self.results = []
        for tc in cases:
            self.run_test(tc)
        return self.results

def _short_json(obj):
    try:
        return json.dumps(obj, indent=2, sort_keys=True)
    except Exception:
        return str(obj)

def print_test_report(results: List[Dict[str, Any]], colorize: bool = True, show_rows: bool = True, max_display_rows: int = 5):
    total = len(results)
    passed = sum(1 for r in results if r.get("ok"))
    failed = total - passed

    # per-test detailed blocks
    for r in results:
        header = f" TEST: {r['name']} "
        status = "PASS" if r.get("ok") else "FAIL"
        status_color = GREEN if r.get("ok") else RED
        print(color(header, BOLD + CYAN, colorize) + color(f"[{status}]", status_color, colorize))

        if r.get("sql"):
            print(color("  SQL:", BOLD, colorize), r["sql"])
        if r.get("setup_sql"):
            print(color("  Setup:", BOLD, colorize))
            for s in r["setup_sql"]:
                print("    ", s)

        print("  " + color("Result:", BOLD, colorize), r.get("message"))
        # Error detail (if any)
        if r.get("error"):
            print("  " + color("Error (trace):", YELLOW, colorize))
            tb_lines = (r["error"].splitlines())
            for ln in tb_lines[:10]:
                print("    " + color(ln, GRAY, colorize))
            if len(tb_lines) > 10:
                print("    " + color("... (truncated)", GRAY, colorize))

        # show sample rows
        if show_rows and r.get("rows") is not None:
            rows = r["rows"]
            print("  " + color(f"Returned rows: {len(rows)}", BOLD, colorize))
            if rows:
                for i, row in enumerate(rows[:max_display_rows]):
                    print("    " + color(f"- {i+1}: ", CYAN, colorize) + _short_json(row))
                if len(rows) > max_display_rows:
                    print("    " + color(f"... ({len(rows)-max_display_rows} more rows)", GRAY, colorize))

        # missing / extra rows (if present)
        if r.get("missing") or r.get("extra"):
            if r.get("missing"):
                print("  " + color("Missing rows (expected but not found):", RED, colorize))
                for mr in r["missing"][:max_display_rows]:
                    print("    " + _short_json(mr))
            if r.get("extra"):
                print("  " + color("Extra rows (found but not expected):", YELLOW, colorize))
                for er in r["extra"][:max_display_rows]:
                    print("    " + _short_json(er))
        print("")  # blank line between tests

    # summary table
    print(color("SUMMARY", BOLD + CYAN, colorize))
    print(f"  Total: {total}  Passed: {color(str(passed), GREEN, colorize)}  Failed: {color(str(failed), RED, colorize)}")
    if failed:
        print("  Failed tests:")
        for r in results:
            if not r.get("ok"):
                print("   -", color(r["name"], RED, colorize), ":", r.get("message"))
    print("")  # final newline