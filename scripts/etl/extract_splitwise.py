#!/usr/bin/env python3
"""
extract_splitwise.py

Provides functions to fetch Splitwise expenses and load them into DuckDB.
"""

import json
import logging
from pathlib import Path
from typing import List, Dict

import duckdb
import pandas as pd
from splitwise import Splitwise

from ..utils.splitwise_client import get_splitwise_client

LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())

def _resolve_paths() -> Dict[str, Path]:
    """
    Determine project-root-relative paths for the DuckDB file and the
    create_raw_tables.sql schema, based on this module's location.
    """
    scripts_dir   = Path(__file__).resolve().parent       # .../scripts/etl
    project_root  = scripts_dir.parent.parent             # BUDGET_DUCK
    return {
        "db_path":        project_root / "db_files" / "budget.duckdb",
        "schema_sql":     project_root / "sql" / "schema" / "create_raw_tables.sql",
    }

def fetch_and_normalize_expenses(
    client: Splitwise,
    group_id: int
) -> List[Dict]:
    """Retrieve expenses for `group_id` and normalize into list of dicts."""
    LOGGER.info("Fetching expenses for group_id=%s", group_id)
    raw = client.getExpenses(group_id=group_id) or []
    records: List[Dict] = []

    for exp in raw:
        users = [
            {
                "user_id": u.getFirstName(),
                "owed_share": float(u.getOwedShare()),
                "paid_share": float(u.getPaidShare()),
                "net_balance": float(u.getNetBalance()),
            }
            for u in (exp.getUsers() or [])
        ]

        records.append({
            "transaction_id": exp.getId(),
            "group_id":       exp.getGroupId(),
            "date":           exp.getDate(),
            "cost":           float(exp.getCost()),
            "currency_code":  exp.getCurrencyCode(),
            "description":    exp.getDescription(),
            "updated_at":     exp.getUpdatedAt(),
            "created_at":     exp.getCreatedAt(),
            "is_payment":     exp.getPayment(),
            "users_json":     json.dumps(users),
            "category_id":    exp.getCategory().getId()   if exp.getCategory() else None,
            "category_name":  exp.getCategory().getName() if exp.getCategory() else None,
            "version_start":  exp.getUpdatedAt(),
            "version_end":    None,
            "is_current":     True,
        })

    LOGGER.info("Fetched %d expenses", len(records))
    return records


def apply_schema_and_insert(
    db_path: Path,
    schema_sql_path: Path,
    records: List[Dict]
) -> None:
    """Apply DDL and bulk-insert the normalized records into raw.transactions."""
    LOGGER.info("Connecting to DuckDB at %s", db_path)
    conn = duckdb.connect(str(db_path))
    conn.execute(schema_sql_path.read_text())

    df = pd.DataFrame(records)
    conn.register("tmp_records", df)
    LOGGER.info("Registered tmp_records with %d rows", len(df))

    conn.execute(
        """
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
        FROM tmp_records
        """
    )
    LOGGER.info("Inserted %d records into raw.transactions", len(df))
    conn.close()


def exract_splitwise(
    user: str,
    group_id: int
) -> None:
    """
    Orchestrate the extract & load:
      1) Authenticate via utils.get_splitwise_client
      2) Fetch & normalize expenses
      3) Apply schema & insert into DuckDB

    Paths for DuckDB and schema are auto‐resolved.
    """
    logging.getLogger().setLevel(logging.INFO)

    # resolve our storage & schema paths
    paths = _resolve_paths()
    db_path        = paths["db_path"]
    schema_sql     = paths["schema_sql"]

    # authenticate
    client = get_splitwise_client(user)
    LOGGER.info("Logged in as %s", client.getCurrentUser().getFirstName())

    # extract + transform
    records = fetch_and_normalize_expenses(client, group_id)

    # load
    apply_schema_and_insert(db_path, schema_sql, records)