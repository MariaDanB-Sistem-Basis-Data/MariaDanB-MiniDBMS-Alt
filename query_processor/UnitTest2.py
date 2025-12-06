from qp_helper.tester import TestCase, QueryTester, print_test_report
from qp_helper.demo_dependencies import build_query_processor
from typing import List


def main():
    TEST_CASES: List[TestCase] = [
        TestCase(
            name="setup_create_and_seed",
            setup_sql=[
                "DROP TABLE Attends;",
                "DROP TABLE Student;",
                "DROP TABLE Course;",
                # CREATEs
                "CREATE TABLE Student (StudentID INTEGER, FullName VARCHAR(100), GPA FLOAT);",
                "CREATE TABLE Course (CourseID INTEGER, Year INTEGER, CourseName VARCHAR(100), CourseDescription VARCHAR(255));",
                "CREATE TABLE Attends (StudentID INTEGER, CourseID INTEGER);",
                # INSERT Student (1..8)s
                "INSERT INTO Student (StudentID, FullName, GPA) VALUES (1, 'Alice',   3.9);",
                "INSERT INTO Student (StudentID, FullName, GPA) VALUES (2, 'Bob',     2.8);",
                "INSERT INTO Student (StudentID, FullName, GPA) VALUES (3, 'Charlie', 3.2);",
                "INSERT INTO Student (StudentID, FullName, GPA) VALUES (4, 'Diana',   3.9);",
                "INSERT INTO Student (StudentID, FullName, GPA) VALUES (5, 'Eve',     1.9);",
                "INSERT INTO Student (StudentID, FullName, GPA) VALUES (6, 'Frank',   3.5);",
                "INSERT INTO Student (StudentID, FullName, GPA) VALUES (7, 'Grace',   3.5);",
                "INSERT INTO Student (StudentID, FullName, GPA) VALUES (8, 'Hank',    2.0);",
                # INSERT Course (101..106)s
                "INSERT INTO Course (CourseID, Year, CourseName, CourseDescription) VALUES (101, 2024, 'Algorithms', 'Algo course');",
                "INSERT INTO Course (CourseID, Year, CourseName, CourseDescription) VALUES (102, 2025, 'Databases',  'DB course');",
                "INSERT INTO Course (CourseID, Year, CourseName, CourseDescription) VALUES (103, 2024, 'Networks',   'Networks course');",
                "INSERT INTO Course (CourseID, Year, CourseName, CourseDescription) VALUES (104, 2023, 'AI',         'AI course');",
                "INSERT INTO Course (CourseID, Year, CourseName, CourseDescription) VALUES (105, 2025, '.Zspecial',  'special');",
                "INSERT INTO Course (CourseID, Year, CourseName, CourseDescription) VALUES (106, 2025, 'databases',  'lowercase name test');",
                # INSERT Attends (10 rows)
                "INSERT INTO Attends (StudentID, CourseID) VALUES (1, 101);",
                "INSERT INTO Attends (StudentID, CourseID) VALUES (1, 102);",
                "INSERT INTO Attends (StudentID, CourseID) VALUES (2, 102);",
                "INSERT INTO Attends (StudentID, CourseID) VALUES (2, 103);",
                "INSERT INTO Attends (StudentID, CourseID) VALUES (3, 103);",
                "INSERT INTO Attends (StudentID, CourseID) VALUES (4, 101);",
                "INSERT INTO Attends (StudentID, CourseID) VALUES (4, 102);",
                "INSERT INTO Attends (StudentID, CourseID) VALUES (5, 104);",
                "INSERT INTO Attends (StudentID, CourseID) VALUES (6, 102);",
                "INSERT INTO Attends (StudentID, CourseID) VALUES (7, 105);",
            ],
            sql="SELECT * FROM Student;",
            expected_count=8,
        ),
        # SELECT projection + WHERE
        TestCase(
            name="select_projection_where_gpa_gt_3",
            sql="SELECT StudentID, FullName FROM Student WHERE GPA > 3.0 ORDER BY StudentID;",
            expected_rows=[
                {"studentid": 1, "fullname": "Alice"},
                {"studentid": 3, "fullname": "Charlie"},
                {"studentid": 4, "fullname": "Diana"},
                {"studentid": 6, "fullname": "Frank"},
                {"studentid": 7, "fullname": "Grace"},
            ],
            ordered=True,
        ),
        # FROM multiple tables, Cartesian product
        TestCase(
            name="cartesian_product_student_course",
            sql="SELECT * FROM Student, Course;",
            expected_count=8 * 6,  # 8 students * 6 courses = 48
        ),
        # JOIN chain Student, Attends -> Course
        TestCase(
            name="join_student_attends_course",
            sql=(
                "SELECT S.StudentID, S.FullName, C.CourseID, C.CourseName "
                "FROM Student S JOIN Attends A ON S.StudentID = A.StudentID "
                "JOIN Course C ON A.CourseID = C.CourseID;"
            ),
            expected_count=10,  # 10 attends rows seeded
        ),
        # NATURAL JOIN (on StudentID)
        TestCase(
            name="natural_join_student_attends",
            sql="SELECT * FROM Student NATURAL JOIN Attends;",
            expected_count=10,
        ),
        # WHERE operator tests: equality (=)
        TestCase(
            name="where_eq_gpa_3_9",
            sql="SELECT * FROM Student WHERE GPA = 3.9;",
            expected_count=2,  # Alice & Diana
        ),
        # WHERE operator tests: not equal (<>)
        TestCase(
            name="where_neq_gpa_not_3_9",
            sql="SELECT * FROM Student WHERE GPA <> 3.9;",
            expected_count=6,
        ),
        # ORDER BY test (string + numeric ordering)
        TestCase(
            name="order_by_gpa_desc_fullname_asc",
            sql="SELECT StudentID, FullName, GPA FROM Student ORDER BY GPA DESC, FullName ASC;",
            validator=lambda rows: (
                (True, "ok")
                if len(rows) > 0
                and float(rows[0].get("gpa", 0)) == 3.9
                and rows[0].get("fullname", "").lower() == "alice"
                else (
                    False,
                    f"expected top row to be Alice with GPA 3.9 but got: {rows[0] if rows else 'NO_ROWS'}",
                )
            ),
        ),
        # LIMIT basic
        TestCase(
            name="limit_order_by_studentid_limit_3",
            sql="SELECT * FROM Student ORDER BY StudentID LIMIT 3;",
            expected_count=3,
        ),
        # UPDATE single-row (SET must be constant value; single WHERE condition)
        TestCase(
            name="update_set_constant_single_condition",
            setup_sql=[
                # perform update in setup so main sql can SELECT and verify
                "UPDATE Student SET GPA = 3.0 WHERE StudentID = 2;"
            ],
            sql="SELECT StudentID, GPA FROM Student WHERE StudentID = 2;",
            expected_rows=[{"studentid": 2, "gpa": 3.0}],
            ordered=True,
        ),
        # INSERT one record (columns can be unordered)
        TestCase(
            name="insert_single_record_unordered_columns",
            setup_sql=[
                "INSERT INTO Student (StudentID, FullName, GPA) VALUES (9, 'Ivan', 2.7);"
            ],
            sql="SELECT StudentID, FullName, GPA FROM Student WHERE StudentID = 9;",
            expected_rows=[{"studentid": 9, "fullname": "Ivan", "gpa": 2.7}],
            ordered=True,
        ),
        # DELETE single-condition (delete the inserted row)
        TestCase(
            name="delete_single_condition",
            setup_sql=["DELETE FROM Student WHERE StudentID = 9;"],
            sql="SELECT * FROM Student WHERE StudentID = 9;",
            expected_count=0,
        ),
        # INSERT with columns in arbitrary order (GPA, FullName, StudentID)
        TestCase(
            name="insert_columns_unordered_variant",
            setup_sql=[
                "INSERT INTO Student (GPA, FullName, StudentID) VALUES (2.6, 'Jill', 10);"
            ],
            sql="SELECT StudentID, FullName, GPA FROM Student WHERE StudentID = 10;",
            expected_rows=[{"studentid": 10, "fullname": "Jill", "gpa": 2.6}],
            ordered=True,
        ),
        # CREATE TABLE and verify empty
        TestCase(
            name="create_table_minimal",
            setup_sql=[
                "DROP TABLE Dept;",
                "CREATE TABLE Dept (DeptID INTEGER, DeptName VARCHAR(50));",
            ],
            sql="SELECT * FROM Dept;",
            expected_count=0,
        ),
        # DROP TABLE minimal
        TestCase(
            name="drop_table_minimal",
            setup_sql=["CREATE TABLE Dept2 (DeptID INTEGER, DeptName VARCHAR(50));"],
            sql="DROP TABLE Dept2;",
            # no explicit assertions here (DROP behavior can vary); test will pass if no error thrown
        ),
    ]

    qp = build_query_processor()
    tester = QueryTester(qp, verbose=True)
    results = tester.run_suite(TEST_CASES)
    print_test_report(results, colorize=True, show_rows=True)

if __name__ == "__main__":
    main()
