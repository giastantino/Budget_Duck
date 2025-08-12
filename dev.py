#!/usr/bin/env python3
"""
Improved development script for running Splitwise ETL with various options.
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
    parser = argparse.ArgumentParser(description='Run Splitwise ETL extraction')
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
        
    except Exception as e:
        print(f"❌ ETL extraction failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()

# Quick examples for testing:
# python dev.py --user Jakub --group-id 82641053
# python dev.py --user Jakub --group-id 82641053 --full-refresh
# python dev.py --user Lucja --group-id 82641053 --batch-size 500 --no-validation