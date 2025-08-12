import unittest
from unittest.mock import patch, MagicMock
from pathlib import Path
import tempfile
import json

from scripts.etl.extract_splitwise import (
    normalize_expense_record,
    validate_expense_record,
    normalize_user_data,
    ETLConfig
)

class TestETLFunctions(unittest.TestCase):
    
    def setUp(self):
        """Set up test fixtures."""
        self.sample_user_data = [{
            "getFirstName": lambda: "John",
            "getLastName": lambda: "Doe", 
            "getOwedShare": lambda: "10.50",
            "getPaidShare": lambda: "21.00",
            "getNetBalance": lambda: "-10.50"
        }]
        
    def test_normalize_user_data(self):
        """Test user data normalization."""
        # Mock user objects
        mock_user = MagicMock()
        mock_user.getFirstName.return_value = "John"
        mock_user.getLastName.return_value = "Doe"
        mock_user.getOwedShare.return_value = "10.50"
        mock_user.getPaidShare.return_value = "21.00"
        mock_user.getNetBalance.return_value = "-10.50"
        
        result = normalize_user_data([mock_user])
        
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["user_id"], "John")
        self.assertEqual(result[0]["owed_share"], 10.50)
        
    def test_validate_expense_record_valid(self):
        """Test validation with valid record."""
        record = {
            "transaction_id": 12345,
            "group_id": 67890,
            "date": "2024-01-15",
            "cost": 25.50,
            "currency_code": "EUR",
            "description": "Grocery shopping",
            "users_json": json.dumps([{"user_id": "John", "owed_share": 12.75}])
        }
        
        self.assertTrue(validate_expense_record(record))
        
    def test_validate_expense_record_invalid(self):
        """Test validation with invalid record."""
        # Missing required fields
        record = {"transaction_id": 12345}
        self.assertFalse(validate_expense_record(record))
        
        # Invalid cost
        record = {
            "transaction_id": 12345,
            "group_id": 67890, 
            "date": "2024-01-15",
            "cost": "invalid"
        }
        self.assertFalse(validate_expense_record(record))

if __name__ == '__main__':
    unittest.main()

# tests/test_db_connection.py  
import unittest
import tempfile
from pathlib import Path

from scripts.utils.db_connection import DuckDBManager

class TestDatabaseConnection(unittest.TestCase):
    
    def setUp(self):
        """Create temporary database for testing."""
        self.temp_dir = tempfile.mkdtemp()
        self.test_db = Path(self.temp_dir) / "test.duckdb"
        self.db_manager = DuckDBManager(self.test_db)
        
    def test_connection_creation(self):
        """Test that connections can be created."""
        with self.db_manager.get_connection() as conn:
            result = conn.execute("SELECT 1 as test").fetchall()
            self.assertEqual(result[0][0], 1)

if __name__ == '__main__':
    unittest.main()