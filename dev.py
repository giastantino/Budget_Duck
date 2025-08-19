#!/usr/bin/env python3
"""
Improved development script for running Splitwise ETL with various options.

This script extracts Splitwise expense data and loads it into two DuckDB tables:
- raw.transactions: Main transaction data (one row per expense)
- raw.user_payments: User-specific payment data (one row per user per transaction)

Both tables support SCD Type 2 versioning for historical tracking.
"""

import argparse
import logging
import sys
from scripts.etl.extract_splitwise import extract_splitwise, ETLConfig

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('etl.log')
    ]
)

def main():
    parser = argparse.ArgumentParser(
        description='Run Splitwise ETL extraction to populate transactions and user_payments tables'
    )
    parser.add_argument('--user', required=True, choices=['Jakub', 'Lucja'], 
                       help='User for Splitwise authentication')
    parser.add_argument('--group-id', type=int, required=True,
                       help='Splitwise group ID to extract from')
    parser.add_argument('--full-refresh', action='store_true',
                       help='Perform full refresh instead of incremental')
    parser.add_argument('--batch-size', type=int, default=100,
                       help='Batch size for database inserts')
    parser.add_argument('--no-validation', action='store_true',
                       help='Skip data validation')
    parser.add_argument('--no-incremental', action='store_true',
                       help='Disable incremental mode')
    
    args = parser.parse_args()
    
    # Create configuration
    config = ETLConfig(
        batch_size=args.batch_size,
        validate_data=not args.no_validation,
        incremental_mode=not args.no_incremental
    )
    
    try:
        extract_splitwise(
            user=args.user,
            group_id=args.group_id,
            config=config,
            full_refresh=args.full_refresh
        )
        print("✅ ETL extraction completed successfully!")
        print("📊 Data loaded into:")
        print("   - raw.transactions (main expense data)")
        print("   - raw.user_payments (user-specific payment data)")
        
    except Exception as e:
        print(f"❌ ETL extraction failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()

# Quick examples for testing:
# python dev.py --user Jakub --group-id 82641053
# python dev.py --user Jakub --group-id 82641053 --full-refresh
# python dev.py --user Lucja --group-id 82641053 --batch-size 500 --no-validation

# Sample queries to explore the extracted data:
"""
-- View recent transactions
SELECT transaction_id, date, description, cost, currency_code, is_payment
FROM raw.transactions 
WHERE is_current = true 
ORDER BY date DESC 
LIMIT 10;

-- View user payments for a specific transaction
SELECT up.user_id, up.user_last_name, up.owed_share, up.paid_share, up.net_balance
FROM raw.user_payments up
WHERE up.transaction_id = <transaction_id> AND up.is_current = true;

-- Summary by user (who owes/is owed money)
SELECT 
    user_id,
    user_last_name,
    SUM(owed_share) as total_owed,
    SUM(paid_share) as total_paid,
    SUM(net_balance) as net_balance
FROM raw.user_payments 
WHERE is_current = true
GROUP BY user_id, user_last_name
ORDER BY net_balance DESC;

-- Transaction history with user details
SELECT 
    t.date,
    t.description,
    t.cost,
    t.currency_code,
    up.user_id,
    up.owed_share,
    up.paid_share
FROM raw.transactions t
JOIN raw.user_payments up ON t.transaction_id = up.transaction_id
WHERE t.is_current = true AND up.is_current = true
ORDER BY t.date DESC, t.transaction_id, up.user_id;
"""