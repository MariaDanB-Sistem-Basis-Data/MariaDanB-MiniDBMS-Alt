from StorageManager import StorageManager
from model.data_retrieval import DataRetrieval
from model.data_write import DataWrite
from model.condition import Condition

def print_section(title):
    print("\n" + "=" * 60)
    print(title)
    print("=" * 60)

def test_list_tables(sm: StorageManager):
    print_section("TEST 1: LIST TABLES (schema.dat)")
    tables = sm.schema_manager.list_tables()
    print("Tables found:", tables)

def test_select_all(sm: StorageManager, table):
    print_section(f"TEST 2: SELECT * FROM {table}")
    req = DataRetrieval(
        table=table,
        column="*"
    )
    rows = sm.read_block(req)
    print(f"Found {len(rows)} rows")
    for r in rows:
        print(r)

def test_select_projection(sm: StorageManager, table, cols):
    print_section(f"TEST 3: SELECT {cols} FROM {table}")
    req = DataRetrieval(
        table=table,
        column=cols
    )
    rows = sm.read_block(req)
    print(f"Found {len(rows)} rows")
    for r in rows:
        print(r)

def test_select_where(sm: StorageManager, table, col, op, val):
    print_section(f"TEST 4: SELECT * FROM {table} WHERE {col} {op} {val}")
    cond = Condition(col, op, val)
    req = DataRetrieval(
        table=table,
        column="*",
        conditions=[cond]
    )
    rows = sm.read_block(req)
    print(f"Found {len(rows)} rows")
    for r in rows:
        print(r)

def test_insert_record(sm: StorageManager):
    print_section("TEST 9: INSERT RECORD INTO Student")
    new_student = {
        "StudentID": 999,
        "FullName": "Test Student",
        "GPA": 3.75
    }
    write_req = DataWrite(
        table = "Student",
        column = None,
        conditions = [],
        new_value = new_student
    )
    sm.write_block(write_req)
    print("Inserted new student record:", new_student)

    print_section("VERIFY INSERTION: SELECT * FROM Student WHERE StudentID = 999")
    cond = Condition("StudentID", "=", 999)
    read_req = DataRetrieval(
        table="Student",
        column="*",
        conditions=[cond]
    )
    rows = sm.read_block(read_req)
    print(f"Found {len(rows)} rows")
    for r in rows:
        print(r)

def test_update_record(sm: StorageManager):
    print_section("TEST 10: UPDATE RECORD IN Student")
    write_req = DataWrite(
        table = "Student",
        column = "GPA",
        conditions = [Condition("StudentID", "=", 3)],
        new_value = 3.95
    )
    row_affected = sm.write_block(write_req)
    print(f"Rows affected: {row_affected}")
    print("Updated GPA of StudentID 3 to 3.95")

    print_section("VERIFY UPDATE: SELECT * FROM Student WHERE StudentID = 3")
    cond = Condition("StudentID", "=", 3)
    read_req = DataRetrieval(
        table="Student",
        column="*",
        conditions=[cond]
    )
    rows = sm.read_block(read_req)
    print(f"Found {len(rows)} rows")
    for r in rows:
        print(r)

def main():
    sm = StorageManager()

    # 1. cek apakah schema.dat berhasil diload
    test_list_tables(sm)

    tables = sm.schema_manager.list_tables()

    # 2. test SELECT * untuk semua tabel
    for t in tables:
        test_select_all(sm, t)

    # 3. test SELECT kolom tertentu
    for t in tables:
        if t == "Student":
            test_select_projection(sm, t, ["StudentID"])
        elif t == "Course":
            test_select_projection(sm, t, ["CourseName", "Year"])

    # 4. test SELECT WHERE
    for t in tables:
        if t == "Student":
            try:
                test_select_where(sm, t, "StudentID", ">", 25)
            except:
                pass

        if t == "Course":
            try:
                test_select_where(sm, t, "CourseName", "=", "Database Systems")
            except:
                pass

    # 5. test error handling: pilih kolom yang tidak ada
    print_section("TEST 5: ERROR HANDLING - INVALID COLUMN")
    try:
        req = DataRetrieval(
            table="Student",
            column=["InvalidColumnExample"]
        )
        sm.read_block(req)
    except ValueError as ve:
        print("Caught expected error:", ve)

    # 6. test error handling: pilih tabel yang tidak ada
    print_section("TEST 6: ERROR HANDLING - INVALID TABLE")
    try:
        req = DataRetrieval(
            table="InvalidTableName",
            column=["StudentID"]
        )
        sm.read_block(req)
    except ValueError as ve:
        print("Caught expected error:", ve)


    # 7. test error handling: WHERE dengan kolom yang tidak ada
    print_section("TEST 7: ERROR HANDLING - INVALID COLUMN IN WHERE")
    try:
        cond = Condition("NonExistentColumn", "=", 10)
        req = DataRetrieval(
            table="Student",
            column="*",
            conditions=[cond]
        )
        sm.read_block(req)
    except ValueError as ve:
        print("Caught expected error:", ve)

    # # 8. test error handling: file data tabel tidak ada
    # print_section("TEST 8: ERROR HANDLING - MISSING DATA FILE")
    # try:
    #     req = DataRetrieval(
    #         table="MissingDataFileTable",
    #         column="*"
    #     )
    #     sm.read_block(req)
    # except FileNotFoundError as fe:
    #     print("Caught expected error:", fe)

    # 9. test insert record
    test_insert_record(sm)

    # 10. test update record
    test_update_record(sm)

if __name__ == "__main__":
    main()
