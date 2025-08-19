# tests/test_etl.py
import pytest
from unittest.mock import MagicMock
import json

from scripts.etl.extract_splitwise import (
    validate_transaction_record, 
    validate_user_payment_record,
    normalize_user_data,
    normalize_expense_record
)

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
    assert result[0]["user_last_name"] == "Doe"
    assert result[0]["owed_share"] == 10.50
    assert result[0]["paid_share"] == 21.00
    assert result[0]["net_balance"] == -10.50


def test_normalize_expense_record():
    """Test expense record normalization into transaction and user payment records."""
    # Create mock expense
    mock_expense = MagicMock()
    mock_expense.getId.return_value = 12345
    mock_expense.getGroupId.return_value = 67890
    mock_expense.getDate.return_value = "2024-01-15"
    mock_expense.getCost.return_value = "25.50"
    mock_expense.getCurrencyCode.return_value = "USD"
    mock_expense.getDescription.return_value = "Test expense"
    mock_expense.getUpdatedAt.return_value = "2024-01-15T10:30:00Z"
    mock_expense.getCreatedAt.return_value = "2024-01-15T10:00:00Z"
    mock_expense.getPayment.return_value = False
    mock_expense.getCategory.return_value = None
    
    # Create mock users
    mock_user1 = MagicMock()
    mock_user1.getFirstName.return_value = "John"
    mock_user1.getLastName.return_value = "Doe"
    mock_user1.getOwedShare.return_value = "12.75"
    mock_user1.getPaidShare.return_value = "25.50"
    mock_user1.getNetBalance.return_value = "-12.75"
    
    mock_user2 = MagicMock()
    mock_user2.getFirstName.return_value = "Jane"
    mock_user2.getLastName.return_value = "Smith"
    mock_user2.getOwedShare.return_value = "12.75"
    mock_user2.getPaidShare.return_value = "0.00"
    mock_user2.getNetBalance.return_value = "12.75"
    
    mock_expense.getUsers.return_value = [mock_user1, mock_user2]
    
    # Test normalization
    transaction_record, user_payment_records = normalize_expense_record(mock_expense)
    
    # Verify transaction record
    assert transaction_record["transaction_id"] == 12345
    assert transaction_record["group_id"] == 67890
    assert transaction_record["cost"] == 25.50
    assert transaction_record["description"] == "Test expense"
    assert transaction_record["is_payment"] == False
    
    # Verify user payment records
    assert len(user_payment_records) == 2
    
    john_payment = next(p for p in user_payment_records if p["user_id"] == "John")
    assert john_payment["transaction_id"] == 12345
    assert john_payment["user_last_name"] == "Doe"
    assert john_payment["owed_share"] == 12.75
    assert john_payment["paid_share"] == 25.50
    assert john_payment["net_balance"] == -12.75
    
    jane_payment = next(p for p in user_payment_records if p["user_id"] == "Jane")
    assert jane_payment["transaction_id"] == 12345
    assert jane_payment["user_last_name"] == "Smith"
    assert jane_payment["owed_share"] == 12.75
    assert jane_payment["paid_share"] == 0.00
    assert jane_payment["net_balance"] == 12.75


@pytest.mark.parametrize("cost,expected", [
    (25.50, True),
    ("invalid", False),
    (-100.0, True),  # Negative costs can be valid (refunds)
])
def test_validate_transaction_cost_values(cost, expected):
    """Test different cost validation scenarios for transaction records."""
    record = {
        "transaction_id": 12345,
        "group_id": 67890,
        "date": "2024-01-15", 
        "cost": cost
    }
    assert validate_transaction_record(record) == expected


def test_validate_transaction_record_required_fields():
    """Test transaction record validation with missing required fields."""
    # Valid record
    valid_record = {
        "transaction_id": 12345,
        "group_id": 67890,
        "date": "2024-01-15",
        "cost": 25.50
    }
    assert validate_transaction_record(valid_record) == True
    
    # Missing transaction_id
    invalid_record = {
        "group_id": 67890,
        "date": "2024-01-15",
        "cost": 25.50
    }
    assert validate_transaction_record(invalid_record) == False
    
    # Empty transaction_id
    invalid_record = {
        "transaction_id": "",
        "group_id": 67890,
        "date": "2024-01-15",
        "cost": 25.50
    }
    assert validate_transaction_record(invalid_record) == False


def test_validate_user_payment_record():
    """Test user payment record validation."""
    # Valid record
    valid_record = {
        "transaction_id": 12345,
        "user_id": "John",
        "owed_share": 12.75,
        "paid_share": 25.50,
        "net_balance": -12.75
    }
    assert validate_user_payment_record(valid_record) == True
    
    # Missing user_id
    invalid_record = {
        "transaction_id": 12345,
        "owed_share": 12.75,
        "paid_share": 25.50,
        "net_balance": -12.75
    }
    assert validate_user_payment_record(invalid_record) == False
    
    # Empty user_id
    invalid_record = {
        "transaction_id": 12345,
        "user_id": "",
        "owed_share": 12.75,
        "paid_share": 25.50,
        "net_balance": -12.75
    }
    assert validate_user_payment_record(invalid_record) == False
    
    # Invalid monetary value
    invalid_record = {
        "transaction_id": 12345,
        "user_id": "John",
        "owed_share": "invalid",
        "paid_share": 25.50,
        "net_balance": -12.75
    }
    assert validate_user_payment_record(invalid_record) == False


def test_normalize_user_data_with_errors():
    """Test user data normalization handles errors gracefully."""
    # Mock user with missing methods
    mock_user_broken = MagicMock()
    mock_user_broken.getFirstName.side_effect = AttributeError("Missing method")
    
    # Mock user with valid data
    mock_user_valid = MagicMock()
    mock_user_valid.getFirstName.return_value = "Jane"
    mock_user_valid.getLastName.return_value = "Doe"
    mock_user_valid.getOwedShare.return_value = "15.00"
    mock_user_valid.getPaidShare.return_value = "0.00"
    mock_user_valid.getNetBalance.return_value = "15.00"
    
    # Test with both users - should only return the valid one
    result = normalize_user_data([mock_user_broken, mock_user_valid])
    
    assert len(result) == 1
    assert result[0]["user_id"] == "Jane"
    assert result[0]["user_last_name"] == "Doe"
    assert result[0]["owed_share"] == 15.00


@pytest.mark.parametrize("shares,expected_valid", [
    ({"owed": "10.50", "paid": "21.00", "net": "-10.50"}, True),
    ({"owed": "invalid", "paid": "21.00", "net": "-10.50"}, False),
    ({"owed": "10.50", "paid": None, "net": "-10.50"}, False),
    ({"owed": "", "paid": "21.00", "net": "-10.50"}, False),
])
def test_validate_user_payment_monetary_fields(shares, expected_valid):
    """Test validation of monetary fields in user payment records."""
    record = {
        "transaction_id": 12345,
        "user_id": "John",
        "owed_share": shares["owed"],
        "paid_share": shares["paid"],
        "net_balance": shares["net"]
    }
    assert validate_user_payment_record(record) == expected_valid


def test_normalize_expense_record_with_category():
    """Test expense normalization with category data."""
    mock_expense = MagicMock()
    mock_expense.getId.return_value = 12345
    mock_expense.getGroupId.return_value = 67890
    mock_expense.getDate.return_value = "2024-01-15"
    mock_expense.getCost.return_value = "25.50"
    mock_expense.getCurrencyCode.return_value = "USD"
    mock_expense.getDescription.return_value = "Groceries"
    mock_expense.getUpdatedAt.return_value = "2024-01-15T10:30:00Z"
    mock_expense.getCreatedAt.return_value = "2024-01-15T10:00:00Z"
    mock_expense.getPayment.return_value = False
    
    # Mock category
    mock_category = MagicMock()
    mock_category.getId.return_value = 15
    mock_category.getName.return_value = "Groceries"
    mock_expense.getCategory.return_value = mock_category
    
    # Mock single user
    mock_user = MagicMock()
    mock_user.getFirstName.return_value = "John"
    mock_user.getLastName.return_value = "Doe"
    mock_user.getOwedShare.return_value = "25.50"
    mock_user.getPaidShare.return_value = "25.50"
    mock_user.getNetBalance.return_value = "0.00"
    mock_expense.getUsers.return_value = [mock_user]
    
    transaction_record, user_payment_records = normalize_expense_record(mock_expense)
    
    # Verify category data is captured
    assert transaction_record["category_id"] == 15
    assert transaction_record["category_name"] == "Groceries"


def test_normalize_expense_record_payment_type():
    """Test expense normalization for payment transactions."""
    mock_expense = MagicMock()
    mock_expense.getId.return_value = 54321
    mock_expense.getGroupId.return_value = 67890
    mock_expense.getDate.return_value = "2024-01-15"
    mock_expense.getCost.return_value = "50.00"
    mock_expense.getCurrencyCode.return_value = "USD"
    mock_expense.getDescription.return_value = "Payment to John"
    mock_expense.getUpdatedAt.return_value = "2024-01-15T11:00:00Z"
    mock_expense.getCreatedAt.return_value = "2024-01-15T11:00:00Z"
    mock_expense.getPayment.return_value = True  # This is a payment
    mock_expense.getCategory.return_value = None
    
    # Mock users for payment
    mock_user1 = MagicMock()
    mock_user1.getFirstName.return_value = "John"
    mock_user1.getLastName.return_value = "Doe"
    mock_user1.getOwedShare.return_value = "0.00"
    mock_user1.getPaidShare.return_value = "-50.00"  # John receives payment
    mock_user1.getNetBalance.return_value = "50.00"
    
    mock_user2 = MagicMock()
    mock_user2.getFirstName.return_value = "Jane"
    mock_user2.getLastName.return_value = "Smith"
    mock_user2.getOwedShare.return_value = "0.00"
    mock_user2.getPaidShare.return_value = "50.00"   # Jane makes payment
    mock_user2.getNetBalance.return_value = "-50.00"
    
    mock_expense.getUsers.return_value = [mock_user1, mock_user2]
    
    transaction_record, user_payment_records = normalize_expense_record(mock_expense)
    
    # Verify it's marked as a payment
    assert transaction_record["is_payment"] == True
    assert transaction_record["description"] == "Payment to John"
    
    # Verify user payments are correct
    john_payment = next(p for p in user_payment_records if p["user_id"] == "John")
    jane_payment = next(p for p in user_payment_records if p["user_id"] == "Jane")
    
    assert john_payment["paid_share"] == -50.00  # Receives money
    assert jane_payment["paid_share"] == 50.00   # Pays money