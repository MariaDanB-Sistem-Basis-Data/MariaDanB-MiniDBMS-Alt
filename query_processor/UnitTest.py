#run pake python -m unittest query_processor.UnitTest -v 
import unittest
import os
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "query_processor"))
sys.path.insert(0, str(ROOT / "query_optimizer"))
sys.path.insert(0, str(ROOT / "storage_manager"))

from datetime import datetime
from QueryProcessor import QueryProcessor
from qp_model.ExecutionResult import ExecutionResult
from qp_model.Rows import Rows

from StorageManager import StorageManager
from storagemanager_model.data_retrieval import DataRetrieval
from storagemanager_model.data_write import DataWrite
from storagemanager_model.condition import Condition
from storagemanager_helper.schema import Schema
from QueryOptimizer import OptimizationEngine


class TestQueryProcessorWithRealData(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        cls.test_db_path = "./test_db_data"
        if os.path.exists(cls.test_db_path):
            shutil.rmtree(cls.test_db_path)
        os.makedirs(cls.test_db_path, exist_ok=True)
    
    @classmethod
    def tearDownClass(cls):
        if os.path.exists(cls.test_db_path):
            shutil.rmtree(cls.test_db_path)
    
    def setUp(self):
        for file in os.listdir(self.test_db_path):
            file_path = os.path.join(self.test_db_path, file)
            if file.endswith('.dat') or file.endswith('.json'):
                try:
                    os.remove(file_path)
                except:
                    pass
        
        self.storage_manager = StorageManager(self.test_db_path)
        self.optimizer = OptimizationEngine()
        
        self.query_processor = QueryProcessor(
            optimization_engine=self.optimizer,
            storage_manager=self.storage_manager,
            data_retrieval_factory=lambda table, column, conditions: DataRetrieval(table, column, conditions),
            data_write_factory=lambda table, column, conditions, new_value: DataWrite(table, column, conditions, new_value),
            condition_factory=lambda column, operation, operand: Condition(column, operation, operand),
            schema_factory=lambda: Schema()
        )
    
    def tearDown(self):
        for file in os.listdir(self.test_db_path):
            file_path = os.path.join(self.test_db_path, file)
            if file.endswith('.dat') or file.endswith('.json'):
                try:
                    os.remove(file_path)
                except:
                    pass
    
    def test_execute_select(self):
        print("\nTEST 1: SELECT Query with Real Data")
        
        create_query = "CREATE TABLE Student (StudentID int, Name varchar(50), GPA float);"
        result = self.query_processor.execute_query(create_query)
        self.assertEqual(result.message, "Success")
        print("Table 'Student' created successfully")
        
        students = [
            ("INSERT INTO Student (StudentID, Name, GPA) VALUES (1, 'Alice', 3.8);", "Alice", 3.8),
            ("INSERT INTO Student (StudentID, Name, GPA) VALUES (2, 'Bob', 3.2);", "Bob", 3.2),
            ("INSERT INTO Student (StudentID, Name, GPA) VALUES (3, 'Charlie', 3.9);", "Charlie", 3.9),
            ("INSERT INTO Student (StudentID, Name, GPA) VALUES (4, 'Diana', 3.0);", "Diana", 3.0)
        ]
        
        inserted_count = 0
        for insert_query, name, gpa in students:
            result = self.query_processor.execute_query(insert_query)
            if result.message == "Success":
                inserted_count += 1
                print(f"Inserted: {name} (GPA: {gpa})")
        
        self.assertEqual(inserted_count, 4, "Should insert 4 students")
        
        select_query = "SELECT StudentID, Name FROM Student WHERE GPA > 3.5;"
        result = self.query_processor.execute_query(select_query)
        
        self.assertEqual(result.message, "Success", "Query should succeed")
        self.assertIsInstance(result.data, Rows, "Result should be Rows object")
        
        print(f"\nSELECT Results (GPA > 3.5)")
        print(f"Total rows returned: {result.data.rows_count}")
        
        self.assertGreaterEqual(result.data.rows_count, 2, "Should return at least 2 students with GPA > 3.5")
        
        returned_names = []
        for row in result.data.data:
            if isinstance(row, dict):
                name = row.get('Name') or row.get('Student.Name')
                returned_names.append(name)
                student_id = row.get('StudentID') or row.get('Student.StudentID')
                print(f"  StudentID: {student_id}, Name: {name}")
        
        self.assertIn('Alice', returned_names, "Alice (GPA 3.8) should be in results")
        self.assertIn('Charlie', returned_names, "Charlie (GPA 3.9) should be in results")
        
        print("\nTEST 1 PASSED: SELECT with WHERE clause works correctly")
        print(f"Filtered 2 students with GPA > 3.5 from 4 total students")
        
        drop_result = self.query_processor.execute_query("DROP TABLE Student;")
        print("Cleanup: Table dropped")
    
    def test_execute_update(self):
        print("\nTEST 2: UPDATE Query with Real Data")
        
        create_query = "CREATE TABLE Employee (EmployeeID int, Name varchar(50), Salary int);"
        result = self.query_processor.execute_query(create_query)
        self.assertEqual(result.message, "Success")
        print("Table 'Employee' created successfully")
        
        employees = [
            ("INSERT INTO Employee (EmployeeID, Name, Salary) VALUES (1, 'Alice', 50000);", "Alice", 50000),
            ("INSERT INTO Employee (EmployeeID, Name, Salary) VALUES (2, 'Bob', 60000);", "Bob", 60000),
            ("INSERT INTO Employee (EmployeeID, Name, Salary) VALUES (3, 'Charlie', 70000);", "Charlie", 70000)
        ]
        
        inserted_count = 0
        for insert_query, name, salary in employees:
            result = self.query_processor.execute_query(insert_query)
            if result.message == "Success":
                inserted_count += 1
                print(f"Inserted: {name} (Salary: ${salary})")
        
        self.assertEqual(inserted_count, 3, "Should insert 3 employees")
        
        print(f"\nUpdating Bob's Salary")
        update_query = "UPDATE Employee SET Salary = 75000 WHERE EmployeeID = 2;"
        result = self.query_processor.execute_query(update_query)
        
        self.assertEqual(result.message, "Success", "UPDATE should succeed")
        self.assertIsInstance(result.data, Rows)
        
        update_message = result.data.data[0] if result.data.data else ""
        print(f"UPDATE result: {update_message}")
        self.assertIn("Updated", str(update_message), "Should return update count message")
        
        print(f"\nVerifying Update")
        verify_query = "SELECT EmployeeID, Name, Salary FROM Employee WHERE EmployeeID = 2;"
        result = self.query_processor.execute_query(verify_query)
        
        self.assertEqual(result.message, "Success", "SELECT should succeed")
        self.assertEqual(result.data.rows_count, 1, "Should return 1 employee")
        
        bob_data = result.data.data[0]
        if isinstance(bob_data, dict):
            salary = bob_data.get('Salary') or bob_data.get('Employee.Salary')
            name = bob_data.get('Name') or bob_data.get('Employee.Name')
            emp_id = bob_data.get('EmployeeID') or bob_data.get('Employee.EmployeeID')
            
            print(f"After UPDATE:")
            print(f"  EmployeeID: {emp_id}")
            print(f"  Name: {name}")
            print(f"  Salary: ${salary}")
            
            self.assertEqual(str(salary), "75000", "Bob's salary should be updated to 75000")
            self.assertEqual(name, "Bob", "Employee name should be Bob")
        
        print(f"\nVerifying Other Employees Unchanged")
        all_query = "SELECT EmployeeID, Name, Salary FROM Employee;"
        result = self.query_processor.execute_query(all_query)
        
        self.assertGreaterEqual(result.data.rows_count, 3, "Should have at least 3 employees")
        
        for row in result.data.data:
            if isinstance(row, dict):
                emp_id = row.get('EmployeeID') or row.get('Employee.EmployeeID')
                name = row.get('Name') or row.get('Employee.Name')
                salary = row.get('Salary') or row.get('Employee.Salary')
                
                if str(emp_id) == "1":
                    self.assertEqual(str(salary), "50000", "Alice's salary should remain 50000")
                    print(f"{name} (ID {emp_id}): Salary ${salary} - Unchanged")
                elif str(emp_id) == "2":
                    self.assertEqual(str(salary), "75000", "Bob's salary should be 75000")
                    print(f"{name} (ID {emp_id}): Salary ${salary} - Updated!")
                elif str(emp_id) == "3":
                    self.assertEqual(str(salary), "70000", "Charlie's salary should remain 70000")
                    print(f"{name} (ID {emp_id}): Salary ${salary} - Unchanged")
        
        print("\nTEST 2 PASSED: UPDATE query works correctly")
        print("Data persisted correctly in Storage Manager")
        print("Only targeted row was updated")
        
        drop_result = self.query_processor.execute_query("DROP TABLE Employee;")
        print("Cleanup: Table dropped")
