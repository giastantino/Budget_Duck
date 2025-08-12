#!/usr/bin/env python3
"""
Database connection manager for DuckDB with multi-process support.
"""

import logging
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Generator

import duckdb

LOGGER = logging.getLogger(__name__)


class DuckDBManager:
    """Manages DuckDB connections with multi-process support."""
    
    def __init__(self, db_path: Path, max_retries: int = 3, retry_delay: float = 0.1):
        self.db_path = str(db_path)
        self.max_retries = max_retries
        self.retry_delay = retry_delay
    
    @contextmanager
    def get_connection(self, read_only: bool = False) -> Generator[duckdb.DuckDBPyConnection, None, None]:
        """
        Get a database connection with retry logic and proper configuration.
        
        Args:
            read_only: If True, opens connection in read-only mode
        """
        conn = None
        attempt = 0
        
        while attempt < self.max_retries:
            try:
                # Connect to database
                if read_only:
                    conn = duckdb.connect(self.db_path, read_only=True)
                else:
                    conn = duckdb.connect(self.db_path)
                    # Enable WAL mode for better concurrent access
                    conn.execute("PRAGMA wal_mode=ON")
                    # Set busy timeout for locked database
                    conn.execute("PRAGMA busy_timeout=30000")  # 30 seconds
                
                LOGGER.debug("Connected to DuckDB (attempt %d, read_only=%s)", attempt + 1, read_only)
                yield conn
                break
                
            except duckdb.IOException as e:
                attempt += 1
                if "database is locked" in str(e).lower() and attempt < self.max_retries:
                    LOGGER.warning("Database locked, retrying in %s seconds (attempt %d/%d)", 
                                 self.retry_delay, attempt, self.max_retries)
                    time.sleep(self.retry_delay)
                    self.retry_delay *= 2  # Exponential backoff
                else:
                    LOGGER.error("Failed to connect to database: %s", e)
                    raise
            except Exception as e:
                LOGGER.error("Unexpected database error: %s", e)
                raise
            finally:
                if conn:
                    try:
                        conn.close()
                    except Exception:
                        pass  # Connection might already be closed
    
    def execute_query(self, query: str, params=None, read_only: bool = False):
        """Execute a single query with connection management."""
        with self.get_connection(read_only=read_only) as conn:
            if params:
                return conn.execute(query, params).fetchall()
            else:
                return conn.execute(query).fetchall()
    
    def execute_script(self, script: str):
        """Execute a SQL script (multiple statements)."""
        with self.get_connection() as conn:
            # Split script into individual statements
            statements = [stmt.strip() for stmt in script.split(';') if stmt.strip()]
            for statement in statements:
                conn.execute(statement)
    
    def table_exists(self, schema: str, table: str) -> bool:
        """Check if a table exists."""
        try:
            result = self.execute_query(
                """
                SELECT COUNT(*) FROM information_schema.tables 
                WHERE table_schema = ? AND table_name = ?
                """,
                [schema, table],
                read_only=True
            )
            return result[0][0] > 0
        except Exception:
            return False


# Global instance - initialize once per module
_db_manager = None

def get_db_manager(db_path: Path = None) -> DuckDBManager:
    """Get the global database manager instance."""
    global _db_manager
    
    if _db_manager is None:
        if db_path is None:
            # Default path resolution
            scripts_dir = Path(__file__).resolve().parent
            project_root = scripts_dir.parent.parent
            db_path = project_root / "db_files" / "budget.duckdb"
        
        _db_manager = DuckDBManager(db_path)
    
    return _db_manager


@contextmanager
def get_db_connection(read_only: bool = False) -> Generator[duckdb.DuckDBPyConnection, None, None]:
    """Convenience function to get a database connection."""
    db_manager = get_db_manager()
    with db_manager.get_connection(read_only=read_only) as conn:
        yield conn