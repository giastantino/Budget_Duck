#!/usr/bin/env python3
"""
extract_splitwise.py

Provides functions to fetch Splitwise expenses and load them into DuckDB
with incremental loading, data validation, and error handling.
"""

import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional

import duckdb
import pandas as pd
import requests
from splitwise import Splitwise
from tenacity import retry, stop_after_attempt, wait_exponential

from ..utils.splitwise_client import get_splitwise_client
from ..utils.db_connection import get_db_connection, get_db_manager

LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())


@dataclass
class ETLConfig:
    """Configuration for ETL process."""
    batch_size: int = 1000
    max_retries: int = 3
    incremental_mode: bool = True
    validate_data: bool = True
    api_timeout: int = 30


def load_config() -> ETLConfig:
    """Load configuration from environment variables."""
    return ETLConfig(
        batch_size=int(os.getenv('ETL_BATCH_SIZE', 1000)),
        max_retries=int(os.getenv('ETL_MAX_RETRIES', 3)),
        incremental_mode=os.getenv('ETL_INCREMENTAL', 'true').lower() == 'true',
        validate_data=os.getenv('ETL_VALIDATE', 'true').lower() == 'true',
        api_timeout=int(os.getenv('ETL_API_TIMEOUT', 30))
    )


def _resolve_paths() -> Dict[str, Path]:
    """
    Determine project-root-relative paths for the DuckDB file and the
    create_raw_tables.sql schema, based on this module's location.
    """
    scripts_dir     = Path(__file__).resolve().parent       # .../scripts/etl
    project_root    = scripts_dir.parent.parent             # BUDGET_DUCK
    return {
        "db_path":      project_root / "db_files" / "budget.duckdb",
        "schema_sql":   project_root / "sql" / "schema" / "create_raw_tables.sql",
    }


def get_last_update_timestamp(db_path: Path, group_id: int) -> Optional[str]:
    """Get the most recent updated_at timestamp for incremental loading."""
    try:
        db_manager = get_db_manager(db_path)
        
        if not db_manager.table_exists('raw', 'transactions'):
            LOGGER.info("Table raw.transactions does not exist yet")
            return None
            
        result = db_manager.execute_query(
            "SELECT MAX(updated_at) FROM raw.transactions WHERE group_id = ? AND is_current = true",
            [group_id],
            read_only=True
        )
        return result[0][0] if result and result[0] and result[0][0] else None
    except Exception as e:
        LOGGER.warning("Could not get last update timestamp: %s", e)
        return None


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10)
)
def fetch_expenses_with_retry(
    client: Splitwise,
    group_id: int,
    since: Optional[str] = None,
    timeout: int = 30
) -> List:
    """Fetch expenses with retry logic for API failures."""
    try:
        LOGGER.info("Fetching expenses for group_id=%s since=%s", group_id, since)
        
        # Build parameters for API call
        params = {'group_id': group_id}
        if since:
            params['dated_after'] = since
            
        # Set timeout if the client supports it
        raw_expenses = client.getExpenses(**params) or []
        LOGGER.info("Successfully fetched %d raw expenses", len(raw_expenses))
        return raw_expenses
        
    except requests.RequestException as e:
        LOGGER.error("API request failed: %s", e)
        raise
    except Exception as e:
        LOGGER.error("Unexpected error fetching expenses: %s", e)
        raise


def normalize_user_data(users) -> List[Dict]:
    """Extract and normalize user data with better error handling."""
    normalized_users = []
    
    for user in (users or []):
        try:
            user_data = {
                "user_id": user.getFirstName(),
                "user_last_name": getattr(user, 'getLastName', lambda: None)(),
                "owed_share": float(user.getOwedShare() or 0),
                "paid_share": float(user.getPaidShare() or 0),
                "net_balance": float(user.getNetBalance() or 0),
            }
            normalized_users.append(user_data)
        except (AttributeError, ValueError) as e:
            LOGGER.warning("Failed to normalize user data: %s", e)
            continue
    
    return normalized_users


def normalize_expense_record(exp) -> Dict:
    """Normalize a single expense record with improved error handling."""
    try:
        users = normalize_user_data(exp.getUsers())
        
        return {
            "transaction_id": exp.getId(),
            "group_id": exp.getGroupId(),
            "date": exp.getDate(),
            "cost": float(exp.getCost() or 0),
            "currency_code": exp.getCurrencyCode(),
            "description": exp.getDescription() or "",
            "updated_at": exp.getUpdatedAt(),
            "created_at": exp.getCreatedAt(),
            "is_payment": bool(exp.getPayment()),
            "users_json": json.dumps(users, ensure_ascii=False),
            "category_id": exp.getCategory().getId() if exp.getCategory() else None,
            "category_name": exp.getCategory().getName() if exp.getCategory() else None,
            "version_start": exp.getUpdatedAt(),
            "version_end": None,
            "is_current": True,
        }
    except Exception as e:
        LOGGER.error("Failed to normalize expense %s: %s", getattr(exp, 'getId', lambda: 'unknown')(), e)
        raise


def validate_expense_record(record: Dict) -> bool:
    """Validate required fields and data types."""
    required_fields = ['transaction_id', 'group_id', 'date', 'cost']
    
    # Check required fields
    missing_fields = [field for field in required_fields if field not in record or record[field] is None]
    if missing_fields:
        LOGGER.warning("Missing required fields %s in transaction %s", 
                      missing_fields, record.get('transaction_id', 'unknown'))
        return False
    
    # Validate transaction_id is not empty
    if not str(record['transaction_id']).strip():
        LOGGER.warning("Empty transaction_id")
        return False
    
    # Validate cost is reasonable
    try:
        cost = float(record['cost'])
        if cost < 0 and not record.get('is_payment', False):
            LOGGER.warning("Negative cost for non-payment transaction: %s", record['transaction_id'])
        
        # Check for extremely large amounts (might indicate data issues)
        if abs(cost) > 1000000:  # 1 million in any currency
            LOGGER.warning("Unusually large amount %s for transaction %s", 
                          cost, record['transaction_id'])
    except (ValueError, TypeError):
        LOGGER.warning("Invalid cost value for transaction %s", record['transaction_id'])
        return False
    
    # Validate JSON structure
    try:
        if record.get('users_json'):
            json.loads(record['users_json'])
    except json.JSONDecodeError:
        LOGGER.warning("Invalid JSON in users_json for transaction %s", record['transaction_id'])
        return False
    
    return True


def handle_updated_records(transaction_ids: List, db_path: Path) -> None:
    """Handle SCD Type 2 updates for changed records."""
    if not transaction_ids:
        return
    
    LOGGER.info("Closing out old versions for %d transactions", len(transaction_ids))
    
    with get_db_connection() as conn:
        placeholders = ','.join(['?' for _ in transaction_ids])
        update_query = f"""
            UPDATE raw.transactions 
            SET is_current = false, version_end = CURRENT_TIMESTAMP
            WHERE transaction_id IN ({placeholders})
            AND is_current = true
        """
        
        rows_updated = conn.execute(update_query, transaction_ids).rowcount
        LOGGER.info("Closed out %d old record versions", rows_updated)


def batch_insert_records(records: List[Dict], batch_size: int = 1000) -> None:
    """Insert records in batches to avoid memory issues."""
    if not records:
        LOGGER.info("No records to insert")
        return
        
    total_inserted = 0
    
    with get_db_connection() as conn:
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            df = pd.DataFrame(batch)
            
            # Register the batch as a temporary table
            batch_name = f"tmp_batch_{i}"
            conn.register(batch_name, df)
            
            # Insert the batch
            insert_query = f"""
                INSERT INTO raw.transactions (
                    transaction_id, group_id, date, cost, currency_code,
                    description, updated_at, created_at, is_payment,
                    category_id, category_name, users_json,
                    version_start, version_end, is_current
                )
                SELECT
                    transaction_id, group_id, date, cost, currency_code,
                    description, updated_at, created_at, is_payment,
                    category_id, category_name, users_json,
                    version_start, version_end, is_current
                FROM {batch_name}
            """
            
            conn.execute(insert_query)
            batch_inserted = len(batch)
            total_inserted += batch_inserted
            
            LOGGER.info("Inserted batch %d-%d (%d records)", 
                       i + 1, min(i + batch_size, len(records)), batch_inserted)
    
    LOGGER.info("Total inserted: %d records", total_inserted)


def apply_schema_and_insert_incremental(
    paths: Dict[str, Path],
    records: List[Dict],
    config: ETLConfig
) -> None:
    """Apply DDL and bulk-insert records with SCD Type 2 handling."""
    db_path = paths["db_path"]
    schema_sql_path = paths["schema_sql"]
    
    LOGGER.info("Connecting to DuckDB at %s", db_path)
    
    # Apply schema using connection manager
    db_manager = get_db_manager(db_path)
    schema_sql = schema_sql_path.read_text(encoding='utf-8')
    db_manager.execute_script(schema_sql)
    
    if not records:
        LOGGER.info("No records to process")
        return
    
    # Handle SCD Type 2 updates
    transaction_ids = [r['transaction_id'] for r in records]
    handle_updated_records(transaction_ids, db_path)
    
    # Insert new records in batches
    batch_insert_records(records, config.batch_size)


def fetch_and_normalize_expenses(
    client: Splitwise,
    group_id: int,
    since: Optional[str] = None,
    config: ETLConfig = None
) -> List[Dict]:
    """Retrieve expenses for group_id and normalize into list of dicts."""
    config = config or load_config()
    
    # Fetch raw expenses with retry logic
    raw_expenses = fetch_expenses_with_retry(client, group_id, since, config.api_timeout)
    
    records: List[Dict] = []
    validation_failures = 0
    
    for exp in raw_expenses:
        try:
            # Normalize the expense record
            record = normalize_expense_record(exp)
            
            # Validate if configured to do so
            if config.validate_data:
                if not validate_expense_record(record):
                    validation_failures += 1
                    continue
            
            records.append(record)
            
        except Exception as e:
            LOGGER.error("Failed to process expense %s: %s", 
                        getattr(exp, 'getId', lambda: 'unknown')(), e)
            continue
    
    LOGGER.info("Successfully processed %d expenses (%d validation failures)", 
               len(records), validation_failures)
    return records


def extract_splitwise(
    user: str,
    group_id: int,
    config: Optional[ETLConfig] = None,
    full_refresh: bool = False
) -> None:
    """
    Enhanced extraction with incremental loading, validation, and error handling.
    
    Args:
        user: Username for Splitwise authentication
        group_id: Splitwise group ID to extract expenses from
        config: ETL configuration (uses defaults if None)
        full_refresh: If True, ignores incremental loading and fetches all data
    """
    # Set up logging
    logging.getLogger().setLevel(logging.INFO)
    
    # Load configuration
    config = config or load_config()
    
    # Resolve paths
    paths = _resolve_paths()
    db_path = paths["db_path"]
    
    LOGGER.info("Starting extraction for user=%s, group_id=%s, full_refresh=%s", 
               user, group_id, full_refresh)
    
    try:
        # Get last update timestamp for incremental loading
        since = None
        if config.incremental_mode and not full_refresh:
            since = get_last_update_timestamp(db_path, group_id)
            if since:
                LOGGER.info("Incremental mode: fetching expenses since %s", since)
            else:
                LOGGER.info("No previous data found, performing full extraction")
        else:
            LOGGER.info("Full refresh mode: fetching all expenses")
        
        # Authenticate
        client = get_splitwise_client(user)
        current_user = client.getCurrentUser()
        LOGGER.info("Authenticated as %s", current_user.getFirstName())
        
        # Extract and transform
        records = fetch_and_normalize_expenses(client, group_id, since, config)
        
        if not records:
            LOGGER.info("No new records to process")
            return
        
        # Load with SCD Type 2 handling
        apply_schema_and_insert_incremental(paths, records, config)
        
        LOGGER.info("Extraction completed successfully. Processed %d records", len(records))
        
    except Exception as e:
        LOGGER.error("Extraction failed: %s", e)
        raise
