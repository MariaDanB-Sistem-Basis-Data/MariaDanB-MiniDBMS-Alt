import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

try:
    from QueryOptimizer import OptimizationEngine 
    from model.query_tree import QueryTree 

    from helper.helper import (
        validate_query,
        _get_columns_from_select,  
        _get_from_table,          
        _get_condition_from_where, 
        _get_limit,
        _get_column_from_order_by,
        _get_column_from_group_by,
        _extract_set_conditions,
        _extract_table_update,
        _extract_table_delete,
        _extract_table_insert,
        _extract_columns_insert,
        _extract_values_insert,
        _parse_table_with_alias
    )
except ImportError as e:
    print(f"ERROR: Gagal mengimpor modul. Pastikan semua file ada.")
    print(f"Detail Error: {e}")
    print("\nPastikan Anda sudah menjalankan:")
    print("touch helper/__init__.py")
    print("touch model/__init__.py")
    sys.exit(1)
except Exception as e:
    print(f"ERROR saat mengimpor helper: {e}")
    sys.exit(1)

def test_unsupported_query_type():
        query = "TRUNCATE TABLE students;"
        is_valid, message = validate_query(query)
        assert is_valid == False
        assert "Unsupported query type" in message


class TestSelectQueryParsing:
    """Test SELECT query parsing to QueryTree"""

    def test_select_all_simple(self):
        optimizer = OptimizationEngine()
        query = "SELECT * FROM students;"
        parsed = optimizer.parse_query(query)

        # Should only have TABLE node (no PROJECT for *)
        assert parsed.query_tree.type == "TABLE"
        assert parsed.query_tree.val == "students"

    def test_select_specific_columns(self):
        optimizer = OptimizationEngine()
        query = "SELECT name, age FROM students;"
        parsed = optimizer.parse_query(query)

        # Root should be PROJECT
        assert parsed.query_tree.type == "PROJECT"
        assert "name" in parsed.query_tree.val
        assert "age" in parsed.query_tree.val

        # Child should be TABLE
        assert len(parsed.query_tree.childs) == 1
        assert parsed.query_tree.childs[0].type == "TABLE"
        assert parsed.query_tree.childs[0].val == "students"

    def test_select_with_single_where(self):
        optimizer = OptimizationEngine()
        query = "SELECT * FROM students WHERE age > 18;"
        parsed = optimizer.parse_query(query)

        # Root should be SIGMA
        assert parsed.query_tree.type == "SIGMA"
        assert parsed.query_tree.val == "age > 18"

        # Child should be TABLE
        assert parsed.query_tree.childs[0].type == "TABLE"

    def test_select_with_where_and(self):
        optimizer = OptimizationEngine()
        query = "SELECT * FROM students WHERE age > 18 AND gpa > 3.0;"
        parsed = optimizer.parse_query(query)

        # Root should be first SIGMA
        assert parsed.query_tree.type == "SIGMA"
        assert parsed.query_tree.val == "age > 18"

        # Second SIGMA should be child
        second_sigma = parsed.query_tree.childs[0]
        assert second_sigma.type == "SIGMA"
        assert second_sigma.val == "gpa > 3.0"

        # TABLE should be at the bottom
        table = second_sigma.childs[0]
        assert table.type == "TABLE"

    def test_select_with_where_multiple_and(self):
        optimizer = OptimizationEngine()
        query = "SELECT * FROM students WHERE age > 18 AND gpa > 3.0 AND department = 'CS';"
        parsed = optimizer.parse_query(query)

        # Should have 3 chained SIGMA nodes
        assert parsed.query_tree.type == "SIGMA"

        current = parsed.query_tree
        sigma_count = 0
        while current.type == "SIGMA":
            sigma_count += 1
            current = current.childs[0]

        assert sigma_count == 3
        assert current.type == "TABLE"

    def test_select_with_where_or(self):
        optimizer = OptimizationEngine()
        query = "SELECT * FROM students WHERE age < 18 OR age > 65;"
        parsed = optimizer.parse_query(query)

        current = parsed.query_tree
        while current.type != "OR" and current.childs:
            current = current.childs[0]

        assert current.type == "OR"
        assert len(current.childs) == 2
        assert current.childs[0].type == "SIGMA"
        assert current.childs[0].val == "age < 18"
        assert current.childs[1].type == "SIGMA"
        assert current.childs[1].val == "age > 65"

    def test_select_with_where_multiple_or(self):
        optimizer = OptimizationEngine()
        query = "SELECT * FROM students WHERE gpa > 3.5 OR gpa < 2.0 OR age > 30;"
        parsed = optimizer.parse_query(query)
        current = parsed.query_tree
        while current.type != "OR" and current.childs:
            current = current.childs[0]

        assert current.type == "OR"
        
        assert len(current.childs) == 3
        assert current.childs[0].type == "SIGMA"
        assert current.childs[1].type == "SIGMA"
        assert current.childs[2].type == "SIGMA"

    def test_select_with_order_by_asc(self):
        optimizer = OptimizationEngine()
        query = "SELECT * FROM students ORDER BY age;"
        parsed = optimizer.parse_query(query)

        # Root should be SORT
        assert parsed.query_tree.type == "SORT"
        assert parsed.query_tree.val == "age"

    def test_select_with_order_by_desc(self):
        optimizer = OptimizationEngine()
        query = "SELECT * FROM students ORDER BY age DESC;"
        parsed = optimizer.parse_query(query)

        assert parsed.query_tree.type == "SORT"
        assert "DESC" in parsed.query_tree.val

    def test_select_with_limit(self):
        optimizer = OptimizationEngine()
        query = "SELECT * FROM students LIMIT 10;"
        parsed = optimizer.parse_query(query)

        # Root should be LIMIT
        assert parsed.query_tree.type == "LIMIT"
        assert parsed.query_tree.val == "10"

    def test_select_with_group_by(self):
        optimizer = OptimizationEngine()
        query = "SELECT department FROM students GROUP BY department;"
        parsed = optimizer.parse_query(query)

        # Root should be PROJECT
        assert parsed.query_tree.type == "PROJECT"

        # Next should be GROUP
        group = parsed.query_tree.childs[0]
        assert group.type == "GROUP"
        assert group.val == "department"

    def test_select_with_all_clauses(self):
        optimizer = OptimizationEngine()
        query = "SELECT name, age FROM students WHERE gpa > 3.0 GROUP BY name ORDER BY age DESC LIMIT 5;"
        parsed = optimizer.parse_query(query)

        # PROJECT -> LIMIT -> SORT -> GROUP -> SIGMA -> TABLE

        assert parsed.query_tree.type == "PROJECT"

        limit = parsed.query_tree.childs[0]
        assert limit.type == "LIMIT"
        assert limit.val == "5"

        sort = limit.childs[0]
        assert sort.type == "SORT"
        assert "age" in sort.val

        group = sort.childs[0]
        assert group.type == "GROUP"

        sigma = group.childs[0]
        assert sigma.type == "SIGMA"

        table = sigma.childs[0]
        assert table.type == "TABLE"

    def test_select_with_comparison_operators(self):
        optimizer = OptimizationEngine()
        # Testing =, <>, >, >=, <, <=
        queries = [
            "SELECT * FROM students WHERE age = 18;",
            "SELECT * FROM students WHERE age <> 18;",
            "SELECT * FROM students WHERE age > 18;",
            "SELECT * FROM students WHERE age >= 18;",
            "SELECT * FROM students WHERE age < 18;",
            "SELECT * FROM students WHERE age <= 18;",
        ]

        for query in queries:
            parsed = optimizer.parse_query(query)
            assert parsed.query_tree.type == "SIGMA"

    def test_select_cartesian_product(self):
        optimizer = OptimizationEngine()
        query = "SELECT * FROM students, courses;"
        parsed = optimizer.parse_query(query)

        # Should have JOIN node at bottom
        current = parsed.query_tree
        while current.type != "JOIN":
            current = current.childs[0]

        assert current.val == "CARTESIAN"
        assert len(current.childs) == 2
        assert current.childs[0].type == "TABLE"
        assert current.childs[1].type == "TABLE"

    def test_select_multiple_tables_cartesian(self):
        optimizer = OptimizationEngine()
        query = "SELECT * FROM students, courses, departments;"
        parsed = optimizer.parse_query(query)

        # Should have nested JOIN nodes
        current = parsed.query_tree
        while current.type != "JOIN":
            current = current.childs[0]

        # Outer JOIN
        assert current.val == "CARTESIAN"

        # Inner JOIN
        inner_join = current.childs[0]
        assert inner_join.type == "JOIN"
        assert inner_join.val == "CARTESIAN"

    def test_select_with_join_on(self):
        optimizer = OptimizationEngine()
        query = "SELECT * FROM students JOIN courses ON students.course_id = courses.id;"
        parsed = optimizer.parse_query(query)

        # Find JOIN node
        current = parsed.query_tree
        while current.type != "JOIN":
            current = current.childs[0]

        assert "THETA:" in current.val
        assert "students.course_id = courses.id" in current.val
        assert len(current.childs) == 2

    def test_select_with_multiple_joins(self):
        optimizer = OptimizationEngine()
        query = "SELECT * FROM students JOIN courses ON students.course_id = courses.id JOIN departments ON courses.dept_id = departments.id;"
        parsed = optimizer.parse_query(query)

        # Find outermost JOIN
        current = parsed.query_tree
        while current.type != "JOIN":
            current = current.childs[0]

        # Outer JOIN
        assert "THETA:" in current.val

        # Inner JOIN (left child)
        inner_join = current.childs[0]
        assert inner_join.type == "JOIN"

    def test_select_with_natural_join(self):
        optimizer = OptimizationEngine()
        query = "SELECT * FROM students NATURAL JOIN courses;"
        parsed = optimizer.parse_query(query)

        # Find JOIN node
        current = parsed.query_tree
        while current.type != "JOIN":
            current = current.childs[0]

        assert current.val == "NATURAL"
        assert len(current.childs) == 2

    def test_select_with_table_alias(self):
        optimizer = OptimizationEngine()
        query = "SELECT * FROM students AS s WHERE s.age > 18;"
        parsed = optimizer.parse_query(query)

        # Find TABLE node
        current = parsed.query_tree
        while current.type != "TABLE":
            current = current.childs[0]

        assert "AS s" in current.val

    def test_select_with_multiple_table_aliases(self):
        optimizer = OptimizationEngine()
        query = "SELECT * FROM student AS s, lecturer AS l WHERE s.lecturer_id = l.id;"
        parsed = optimizer.parse_query(query)

        # Should have JOIN with both tables having aliases
        current = parsed.query_tree
        while current.type != "JOIN":
            current = current.childs[0]

        # Check left table
        left_table = current.childs[0]
        assert "AS s" in left_table.val

        # Check right table
        right_table = current.childs[1]
        assert "AS l" in right_table.val

    def test_select_join_with_aliases(self):
        optimizer = OptimizationEngine()
        query = "SELECT * FROM students AS s JOIN courses AS c ON s.course_id = c.id;"
        parsed = optimizer.parse_query(query)

        # Find JOIN node
        current = parsed.query_tree
        while current.type != "JOIN":
            current = current.childs[0]

        # Both tables should have aliases
        assert "AS s" in current.childs[0].val or "AS s" in current.childs[1].val
        assert "AS c" in current.childs[0].val or "AS c" in current.childs[1].val


class TestUpdateQueryParsing:
    def test_update_basic_no_where(self):
        optimizer = OptimizationEngine()
        query = "UPDATE students SET gpa = 4.0;"
        parsed = optimizer.parse_query(query)

        # Root should be UPDATE
        assert parsed.query_tree.type == "UPDATE"
        assert "gpa = 4.0" in parsed.query_tree.val

        # Child should be TABLE
        assert parsed.query_tree.childs[0].type == "TABLE"
        assert parsed.query_tree.childs[0].val == "students"

    def test_update_with_where(self):
        optimizer = OptimizationEngine()
        query = "UPDATE students SET gpa = 4.0 WHERE id = 123;"
        parsed = optimizer.parse_query(query)

        # UPDATE -> SIGMA -> TABLE
        assert parsed.query_tree.type == "UPDATE"

        sigma = parsed.query_tree.childs[0]
        assert sigma.type == "SIGMA"
        assert "id = 123" in sigma.val

        table = sigma.childs[0]
        assert table.type == "TABLE"
        assert table.val == "students"

    def test_update_multiple_sets(self):
        optimizer = OptimizationEngine()
        query = "UPDATE students SET gpa = 4.0, age = 21;"
        parsed = optimizer.parse_query(query)

        # Should have chained UPDATE nodes
        assert parsed.query_tree.type == "UPDATE"
        assert "gpa = 4.0" in parsed.query_tree.val

        second_update = parsed.query_tree.childs[0]
        assert second_update.type == "UPDATE"
        assert "age = 21" in second_update.val

    def test_update_multiple_sets_with_where(self):
        optimizer = OptimizationEngine()
        query = "UPDATE students SET gpa = 4.0, age = 21 WHERE id = 123;"
        parsed = optimizer.parse_query(query)

        # UPDATE -> UPDATE -> SIGMA -> TABLE
        assert parsed.query_tree.type == "UPDATE"

        second_update = parsed.query_tree.childs[0]
        assert second_update.type == "UPDATE"

        sigma = second_update.childs[0]
        assert sigma.type == "SIGMA"

    def test_update_with_expression(self):
        optimizer = OptimizationEngine()
        query = "UPDATE employee SET salary = 1.05 * salary WHERE salary > 1000;"
        parsed = optimizer.parse_query(query)

        assert parsed.query_tree.type == "UPDATE"
        assert "1.05 * salary" in parsed.query_tree.val

    def test_update_with_multiple_where_and(self):
        optimizer = OptimizationEngine()
        query = "UPDATE students SET gpa = 4.0 WHERE age > 18 AND department = 'CS';"
        parsed = optimizer.parse_query(query)

        # UPDATE -> SIGMA -> SIGMA -> TABLE
        assert parsed.query_tree.type == "UPDATE"

        first_sigma = parsed.query_tree.childs[0]
        assert first_sigma.type == "SIGMA"

        second_sigma = first_sigma.childs[0]
        assert second_sigma.type == "SIGMA"


class TestDeleteQueryParsing:
    def test_delete_no_where(self):
        optimizer = OptimizationEngine()
        query = "DELETE FROM students;"
        parsed = optimizer.parse_query(query)

        # Root should be DELETE
        assert parsed.query_tree.type == "DELETE"

        # Child should be TABLE
        assert parsed.query_tree.childs[0].type == "TABLE"
        assert parsed.query_tree.childs[0].val == "students"

    def test_delete_with_where(self):
        optimizer = OptimizationEngine()
        query = "DELETE FROM students WHERE age < 18;"
        parsed = optimizer.parse_query(query)

        # DELETE -> SIGMA -> TABLE
        assert parsed.query_tree.type == "DELETE"

        sigma = parsed.query_tree.childs[0]
        assert sigma.type == "SIGMA"
        assert "age < 18" in sigma.val

        table = sigma.childs[0]
        assert table.type == "TABLE"
        assert table.val == "students"

    def test_delete_with_string_comparison(self):
        optimizer = OptimizationEngine()
        query = "DELETE FROM employee WHERE department = 'RnD';"
        parsed = optimizer.parse_query(query)

        assert parsed.query_tree.type == "DELETE"

        sigma = parsed.query_tree.childs[0]
        assert "department = 'RnD'" in sigma.val or 'department = "RnD"' in sigma.val

    def test_delete_with_multiple_where_and(self):
        optimizer = OptimizationEngine()
        query = "DELETE FROM employee WHERE department = 'RnD' AND salary < 5000;"
        parsed = optimizer.parse_query(query)

        # DELETE -> SIGMA -> SIGMA -> TABLE
        assert parsed.query_tree.type == "DELETE"

        first_sigma = parsed.query_tree.childs[0]
        assert first_sigma.type == "SIGMA"

        second_sigma = first_sigma.childs[0]
        assert second_sigma.type == "SIGMA"

        table = second_sigma.childs[0]
        assert table.type == "TABLE"


class TestInsertQueryParsing:
    def test_insert_basic(self):
        optimizer = OptimizationEngine()
        query = "INSERT INTO students (id, name, age) VALUES (123, 'John', 20);"
        parsed = optimizer.parse_query(query)

        # Root should be INSERT
        assert parsed.query_tree.type == "INSERT"

        # table|columns|values
        val = parsed.query_tree.val
        assert "students" in val
        assert "(id, name, age)" in val
        assert "(123, 'John', 20)" in val or '(123, "John", 20)' in val

    def test_insert_with_different_types(self):
        optimizer = OptimizationEngine()
        query = "INSERT INTO employees (id, name, salary, active) VALUES (456, 'Alice', 75000.50, 1);"
        parsed = optimizer.parse_query(query)

        assert parsed.query_tree.type == "INSERT"
        assert "employees" in parsed.query_tree.val
        assert "75000.50" in parsed.query_tree.val

    def test_insert_string_with_spaces(self):
        optimizer = OptimizationEngine()
        query = "INSERT INTO employees (name, department) VALUES ('Alice Johnson', 'Human Resources');"
        parsed = optimizer.parse_query(query)

        assert parsed.query_tree.type == "INSERT"
        assert "Alice Johnson" in parsed.query_tree.val


class TestTransactionQueryParsing:
    def test_begin_transaction(self):
        optimizer = OptimizationEngine()
        query = "BEGIN TRANSACTION;"
        parsed = optimizer.parse_query(query)

        assert parsed.query_tree.type == "BEGIN_TRANSACTION"
        assert len(parsed.query_tree.childs) == 0

    def test_commit(self):
        optimizer = OptimizationEngine()
        query = "COMMIT;"
        parsed = optimizer.parse_query(query)

        assert parsed.query_tree.type == "COMMIT"
        assert len(parsed.query_tree.childs) == 0

    def test_rollback(self):
        optimizer = OptimizationEngine()
        query = "ROLLBACK;"
        parsed = optimizer.parse_query(query)

        assert parsed.query_tree.type == "ROLLBACK"
        assert len(parsed.query_tree.childs) == 0


class TestHelperFunctions:
    def test_get_columns_from_select_all(self):
        query = "SELECT * FROM students"
        columns = _get_columns_from_select(query)
        assert columns == "*"

    def test_get_columns_from_select_specific(self):
        query = "SELECT name, age, gpa FROM students"
        columns = _get_columns_from_select(query)
        assert "name" in columns
        assert "age" in columns
        assert "gpa" in columns

    def test_get_columns_with_expressions(self):
        query = "SELECT name, age * 2 AS double_age FROM students"
        columns = _get_columns_from_select(query)
        assert "name" in columns
        assert "age * 2" in columns

    def test_get_from_table_simple(self):
        query = "SELECT * FROM students"
        table = _get_from_table(query)
        assert table == "students"

    def test_get_from_table_with_where(self):
        query = "SELECT * FROM students WHERE age > 18"
        table = _get_from_table(query)
        assert table == "students"
        assert "WHERE" not in table

    def test_get_from_table_with_join(self):
        query = "SELECT * FROM students JOIN courses ON students.id = courses.student_id WHERE age > 18"
        table = _get_from_table(query)
        assert "students" in table
        assert "JOIN" in table
        assert "WHERE" not in table

    def test_get_from_table_with_alias(self):
        query = "SELECT * FROM students AS s WHERE s.age > 18"
        table = _get_from_table(query)
        assert "students AS s" in table or "students as s" in table

    def test_get_condition_from_where_simple(self):
        query = "SELECT * FROM students WHERE age > 18"
        condition = _get_condition_from_where(query)
        assert condition == "age > 18"

    def test_get_condition_from_where_and(self):
        query = "SELECT * FROM students WHERE age > 18 AND gpa > 3.0"
        condition = _get_condition_from_where(query)
        assert "age > 18" in condition
        assert "AND" in condition
        assert "gpa > 3.0" in condition

    def test_get_condition_from_where_or(self):
        query = "SELECT * FROM students WHERE age < 18 OR age > 65"
        condition = _get_condition_from_where(query)
        assert "age < 18" in condition
        assert "OR" in condition
        assert "age > 65" in condition

    def test_get_condition_from_where_no_where(self):
        query = "SELECT * FROM students"
        condition = _get_condition_from_where(query)
        assert condition == ""

    def test_get_limit(self):
        query = "SELECT * FROM students LIMIT 10"
        limit = _get_limit(query)
        assert limit == 10

    def test_get_limit_large_number(self):
        query = "SELECT * FROM students LIMIT 1000"
        limit = _get_limit(query)
        assert limit == 1000

    def test_get_column_from_order_by(self):
        query = "SELECT * FROM students ORDER BY age"
        col = _get_column_from_order_by(query)
        assert col == "age"

    def test_get_column_from_order_by_asc(self):
        query = "SELECT * FROM students ORDER BY age ASC"
        col = _get_column_from_order_by(query)
        assert "age" in col
        assert "ASC" in col

    def test_get_column_from_order_by_desc(self):
        query = "SELECT * FROM students ORDER BY age DESC"
        col = _get_column_from_order_by(query)
        assert "age" in col
        assert "DESC" in col

    def test_get_column_from_group_by(self):
        query = "SELECT department FROM students GROUP BY department"
        col = _get_column_from_group_by(query)
        assert col == "department"

    def test_extract_set_conditions_single(self):
        query = "UPDATE students SET gpa = 4.0 WHERE id = 123"
        conditions = _extract_set_conditions(query)
        assert len(conditions) == 1
        assert "gpa = 4.0" in conditions[0]

    def test_extract_set_conditions_multiple(self):
        query = "UPDATE students SET gpa = 4.0, age = 21 WHERE id = 123"
        conditions = _extract_set_conditions(query)
        assert len(conditions) == 2
        assert any("gpa = 4.0" in cond for cond in conditions)
        assert any("age = 21" in cond for cond in conditions)

    def test_extract_set_conditions_with_expression(self):
        query = "UPDATE employee SET salary = salary * 1.05 WHERE id = 1"
        conditions = _extract_set_conditions(query)
        assert "salary * 1.05" in conditions[0]

    def test_extract_table_update(self):
        query = "UPDATE students SET gpa = 4.0"
        table = _extract_table_update(query)
        assert table == "students"

    def test_extract_table_delete(self):
        query = "DELETE FROM students WHERE age < 18"
        table = _extract_table_delete(query)
        assert table == "students"

    def test_extract_table_delete_no_where(self):
        query = "DELETE FROM employees"
        table = _extract_table_delete(query)
        assert table == "employees"

    def test_extract_table_insert(self):
        query = "INSERT INTO students (id, name) VALUES (123, 'John')"
        table = _extract_table_insert(query)
        assert table == "students"

    def test_extract_columns_insert(self):
        query = "INSERT INTO students (id, name, age) VALUES (123, 'John', 20)"
        columns = _extract_columns_insert(query)
        assert columns == "(id, name, age)"

    def test_extract_values_insert(self):
        query = "INSERT INTO students (id, name, age) VALUES (123, 'John', 20)"
        values = _extract_values_insert(query)
        assert values == "(123, 'John', 20)" or values == '(123, "John", 20)'

    def test_parse_table_with_alias(self):
        table_str = "students AS s"
        node = _parse_table_with_alias(table_str)
        assert node.type == "TABLE"
        assert "students" in node.val
        assert "AS s" in node.val

    def test_parse_table_without_alias(self):
        table_str = "students"
        node = _parse_table_with_alias(table_str)
        assert node.type == "TABLE"
        assert node.val == "students"


class TestErrorHandling:
    def test_empty_query(self):
        optimizer = OptimizationEngine()
        try:
            optimizer.parse_query("")
            assert False, "Harusnya memunculkan error"
        except Exception as exc_info:
            assert "empty" in str(exc_info).lower()

    def test_none_query(self):
        optimizer = OptimizationEngine()
        try:
            optimizer.parse_query(None)
            assert False, "Harusnya memunculkan error"
        except Exception as exc_info:
            assert exc_info is not None

    def test_invalid_query_no_semicolon(self):
        optimizer = OptimizationEngine()
        try:
            optimizer.parse_query("SELECT * FROM students")
            assert False, "Harusnya memunculkan error"
        except Exception as exc_info:
            assert "semicolon" in str(exc_info).lower()

    def test_invalid_query_typo(self):
        optimizer = OptimizationEngine()
        try:
            optimizer.parse_query("SELCT * FROM students;")
            assert False, "Harusnya memunculkan error"
        except Exception as exc_info:
            assert "validation failed" in str(exc_info).lower()

    def test_invalid_clause_order(self):
        optimizer = OptimizationEngine()
        try:
            optimizer.parse_query("SELECT * FROM students LIMIT 10 WHERE age > 18;")
            assert False, "Harusnya memunculkan error"
        except Exception as exc_info:
            assert "order" in str(exc_info).lower() or "validation failed" in str(exc_info).lower()

    def test_unsupported_query_create(self):
        optimizer = OptimizationEngine()
        try:
            optimizer.parse_query("CREATE TABLE students (id INTEGER);")
            assert False, "Harusnya memunculkan error"
        except Exception as exc_info:
            assert "not implemented" in str(exc_info).lower() or "storage manager" in str(exc_info).lower()

    def post_unsupported_query_drop(self):
        optimizer = OptimizationEngine()
        try:
            optimizer.parse_query("DROP TABLE students;")
            assert False, "Harusnya memunculkan error"
        except Exception as exc_info:
            assert "not implemented" in str(exc_info).lower() or "storage manager" in str(exc_info).lower()

    def test_unsupported_query_truncate(self):
        optimizer = OptimizationEngine()
        try:
            optimizer.parse_query("TRUNCATE TABLE students;")
            assert False, "Harusnya memunculkan error"
        except Exception as exc_info:
            assert "unsupported" in str(exc_info).lower() or "validation failed" in str(exc_info).lower()

    def test_mixed_and_or_error(self):
        optimizer = OptimizationEngine()
        try:
            optimizer.parse_query("SELECT * FROM students WHERE age > 18 AND gpa > 3.0 OR department = 'CS';")
            assert False, "Harusnya memunculkan error"
        except Exception as exc_info:
            assert "mixed" in str(exc_info).lower()

    def test_insert_without_values(self):
        optimizer = OptimizationEngine()
        try:
            optimizer.parse_query("INSERT INTO students (id, name);")
            assert False, "Harusnya memunculkan error"
        except Exception as exc_info:
            # Will fail validation
            assert "validation failed" in str(exc_info).lower()


class TestComplexQueries:
    def test_complex_select_query(self):
        optimizer = OptimizationEngine()
        query = """
        SELECT s.name, s.age, c.course_name
        FROM students AS s
        JOIN courses AS c ON s.course_id = c.id
        WHERE s.gpa > 3.5 AND s.age > 18
        ORDER BY s.name ASC
        LIMIT 20;
        """
        parsed = optimizer.parse_query(query)
        assert parsed.query_tree is not None
        assert parsed.query_tree.type == "PROJECT"

    def test_complex_multi_join(self):
        optimizer = OptimizationEngine()
        query = """
        SELECT *
        FROM students AS s
        JOIN courses AS c ON s.course_id = c.id
        JOIN departments AS d ON c.dept_id = d.id
        WHERE s.gpa > 3.0;
        """
        parsed = optimizer.parse_query(query)
        assert parsed.query_tree is not None

    def test_complex_natural_join_with_conditions(self):
        optimizer = OptimizationEngine()
        query = """
        SELECT *
        FROM students NATURAL JOIN enrollments
        WHERE grade > 3.0 AND year = 2024
        ORDER BY grade DESC
        LIMIT 10;
        """
        parsed = optimizer.parse_query(query)
        assert parsed.query_tree is not None

    def test_complex_update_multiple_sets(self):
        optimizer = OptimizationEngine()
        query = """
        UPDATE employees
        SET salary = salary * 1.1, bonus = bonus + 1000, last_updated = '2024-01-01'
        WHERE department = 'Engineering' AND years_of_service > 5;
        """
        parsed = optimizer.parse_query(query)
        assert parsed.query_tree.type == "UPDATE"

    def test_complex_cartesian_with_conditions(self):
        optimizer = OptimizationEngine()
        query = """
        SELECT *
        FROM student AS s, lecturer AS l, course AS c
        WHERE s.lecturer_id = l.id AND s.course_id = c.id AND s.gpa > 3.0;
        """
        parsed = optimizer.parse_query(query)
        assert parsed.query_tree is not None


class TestEdgeCases:
    def test_whitespace_handling(self):
        optimizer = OptimizationEngine()
        query = "  SELECT   * FROM   students  ;  "
        parsed = optimizer.parse_query(query)
        assert parsed.query_tree is not None

    def test_multiline_query(self):
        optimizer = OptimizationEngine()
        query = """
        SELECT *
        FROM students
        WHERE age > 18;
        """
        parsed = optimizer.parse_query(query)
        assert parsed.query_tree is not None

    def test_case_insensitive_keywords(self):
        optimizer = OptimizationEngine()
        query = "select * from students where age > 18;"
        parsed = optimizer.parse_query(query)
        assert parsed.query_tree is not None

    def test_mixed_case_keywords(self):
        optimizer = OptimizationEngine()
        query = "SeLeCt * FrOm students WhErE age > 18;"
        parsed = optimizer.parse_query(query)
        assert parsed.query_tree is not None

    def test_limit_zero(self):
        optimizer = OptimizationEngine()
        query = "SELECT * FROM students LIMIT 0;"
        parsed = optimizer.parse_query(query)
        assert parsed.query_tree.type == "LIMIT"
        assert parsed.query_tree.val == "0"

    def test_single_character_table_name(self):
        optimizer = OptimizationEngine()
        query = "SELECT * FROM a;"
        parsed = optimizer.parse_query(query)
        assert parsed.query_tree.val == "a"

    def test_table_with_underscore(self):
        optimizer = OptimizationEngine()
        query = "SELECT * FROM student_records;"
        parsed = optimizer.parse_query(query)
        assert parsed.query_tree.val == "student_records"

    def test_column_with_dot_notation(self):
        optimizer = OptimizationEngine()
        query = "SELECT students.name FROM students;"
        parsed = optimizer.parse_query(query)
        assert "students.name" in parsed.query_tree.val


if __name__ == "__main__":
    tests_to_run = []
    tests_to_run.append(test_unsupported_query_type)
    
    test_classes = [
        TestSelectQueryParsing,
        TestUpdateQueryParsing,
        TestDeleteQueryParsing,
        TestInsertQueryParsing,
        TestTransactionQueryParsing,
        TestHelperFunctions,
        TestErrorHandling,
        TestComplexQueries,
        TestEdgeCases
    ]

    for test_class in test_classes:
        instance = test_class()
        methods = [getattr(instance, func) for func in dir(test_class) if callable(getattr(instance, func)) and func.startswith("test_")]
        tests_to_run.extend(methods)

    passed_count = 0
    failed_count = 0

    for test_func in tests_to_run:
        test_name = test_func.__name__
        try:
            test_func()
            print(f"PASS: {test_name}")
            passed_count += 1
        except AssertionError as e:
            print(f"FAIL: {test_name}\n  Error: {e}")
            failed_count += 1
        except Exception as e:
            print(f"ERROR: {test_name}\n  Unexpected error: {type(e).__name__}: {e}")
            failed_count += 1

    print("\n" + "="*40)
    print("Ringkasan Tes")
    print("="*40)
    print(f"Total Tes : {passed_count + failed_count}")
    print(f"Lulus     : {passed_count}")
    print(f"Gagal     : {failed_count}")
    print("="*40)

    if failed_count > 0:
        sys.exit(1)
    else:
        sys.exit(0)