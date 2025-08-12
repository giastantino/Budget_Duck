# tests/test_etl.py
import pytest
from unittest.mock import MagicMock
import json

from scripts.etl.extract_splitwise import validate_expense_record, normalize_user_data

def test_normalize_user_data():
    """Test user data normalization."""
    mock_user = MagicMock()
    mock_user.getFirstName.return_value = "John"
    mock_user.getLastName.return_value = "Doe"
    mock_user.getOwedShare.return_value = "10.50"
    mock_user.getPaidShare.return_value = "21.00"
    mock_user.getNetBalance.return_value = "-10.50"
    
    result = normalize_user_data([mock_user])
    
    assert len(result) == 1
    assert result[0]["user_id"] == "John"
    assert result[0]["owed_share"] == 10.50

@pytest.mark.parametrize("cost,expected", [
    (25.50, True),
    ("invalid", False),
    (-100.0, True),  # Negative costs can be valid (refunds)
])
def test_validate_cost_values(cost, expected):
    """Test different cost validation scenarios."""
    record = {
        "transaction_id": 12345,
        "group_id": 67890,
        "date": "2024-01-15", 
        "cost": cost
    }
    assert validate_expense_record(record) == expected