# Budget Duck 🦆

A Python ETL pipeline for extracting expense data from Splitwise and loading it into DuckDB for analysis and learning dbt.

## Overview

This project extracts expense data from the Splitwise API, transforms it into a normalized format, and loads it into a DuckDB database.

## Project Structure

```
Budget_Duck/
├── scripts/
│   ├── etl/
│   │   └── extract_splitwise.py    # Main ETL logic
│   └── utils/
│       ├── splitwise_client.py     # Splitwise authentication
│       └── db_connection.py        # Database connection manager
├── sql/
│   └── schema/
│       └── create_raw_tables.sql   # Database schema
├── db_files/
│   └── budget.duckdb              # DuckDB database (created automatically)
├── dev.py                         # Development script
├── main.py                        # Production entry point
├── requirements.txt               # Python dependencies
├── .env.example                   # Environment variables template
└── README.md                      # This file
```

## Setup

### 1. Clone the repository

```bash
git clone <your-repo-url>
cd Budget_Duck
```

### 2. Create virtual environment

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Set up environment variables

```bash
cp .env.example .env
# Edit .env with your configuration
```

### 5. Configure Splitwise credentials

Store your Splitwise API credentials in the system keyring:

```python
import keyring

# For each user (Jakub, Lucja, etc.)
keyring.set_password("Splitwise_<USER>_Consumer_Key", "Consumer_Key", "your_consumer_key")
keyring.set_password("Splitwise_<USER>_Consumer_Secret", "Consumer_Secret", "your_consumer_secret")
keyring.set_password("Splitwise_<USER>_API_Key", "API_Key", "your_api_key")
```

## Usage

### Basic ETL Run

```bash
python dev.py --user Jakub --group-id 82641053
```

### Command Line Options

```bash
# Full refresh (re-extract all data)
python dev.py --user Lucja --group-id 82641053 --full-refresh

# Custom batch size
python dev.py --user Jakub --group-id 82641053 --batch-size 500

# Disable data validation (faster processing)
python dev.py --user Lucja --group-id 82641053 --no-validation

# Disable incremental mode
python dev.py --user Jakub --group-id 82641053 --no-incremental
```

### Programmatic Usage

```python
from scripts.etl.extract_splitwise import extract_splitwise, ETLConfig

# Custom configuration
config = ETLConfig(
    batch_size=500,
    validate_data=True,
    incremental_mode=True
)

extract_splitwise(
    user="Jakub",
    group_id=82641053,
    config=config,
    full_refresh=False
)
```

## Database Schema

The pipeline creates a `raw.transactions` table with SCD Type 2 versioning:

| Column | Type | Description |
|--------|------|-------------|
| transaction_id | BIGINT | Splitwise transaction ID (PK) |
| group_id | BIGINT | Splitwise group ID |
| date | DATE | Transaction date |
| cost | DECIMAL(10,2) | Transaction amount |
| currency_code | VARCHAR(3) | Currency (EUR, USD, etc.) |
| description | TEXT | Transaction description |
| updated_at | TIMESTAMP | Last updated timestamp |
| created_at | TIMESTAMP | Creation timestamp |
| is_payment | BOOLEAN | Whether this is a payment transaction |
| category_id | BIGINT | Splitwise category ID |
| category_name | TEXT | Category name |
| users_json | TEXT | JSON array of user details |
| version_start | TIMESTAMP | SCD Type 2 version start |
| version_end | TIMESTAMP | SCD Type 2 version end |
| is_current | BOOLEAN | Current version flag |

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| ETL_BATCH_SIZE | 1000 | Batch size for database operations |
| ETL_MAX_RETRIES | 3 | Maximum retry attempts for API calls |
| ETL_INCREMENTAL | true | Enable incremental loading |
| ETL_VALIDATE | true | Enable data validation |
| ETL_API_TIMEOUT | 30 | API timeout in seconds |
| LOG_LEVEL | INFO | Logging level |

### Features

#### Incremental Loading

- Automatically detects the last processed timestamp
- Only fetches new/updated records since last run
- Falls back to full refresh if no previous data exists

#### Data Validation

- Validates required fields and data types
- Checks for reasonable cost values
- Validates JSON structure
- Logs validation failures for debugging

#### Error Handling

- Retry logic for API failures with exponential backoff
- Graceful handling of individual record failures
- Comprehensive logging for troubleshooting

#### Multi-Process Support

- Connection pooling for concurrent access
- Retry mechanisms for database locks
- Read-only connections for query operations

## Development

### Running Tests

```bash
# Add tests when available
pytest tests/
```

### Code Style

```bash
# Format code
black scripts/
flake8 scripts/
```

### Database Management

```bash
# Connect to DuckDB CLI
duckdb db_files/budget.duckdb

# Basic queries
SELECT COUNT(*) FROM raw.transactions;
SELECT * FROM raw.transactions WHERE is_current = true LIMIT 10;
```

## Future Plans

- [ ] Add dbt transformations
- [ ] Create staging and mart layers
- [ ] Add data quality tests
- [ ] Implement change data capture
- [ ] Add web dashboard
- [ ] Support multiple Splitwise groups
- [ ] Add data export functionality

## Troubleshooting

### Common Issues

**Polish characters not displaying correctly:**

- Ensure your schema file is saved as UTF-8
- Check that TEXT fields are used instead of VARCHAR

**Database locked errors:**

- The connection manager handles this automatically with retries
- Ensure only one long-running process accesses the database

**API rate limiting:**

- The retry mechanism handles temporary failures
- Consider reducing batch sizes for large datasets

**Memory issues with large datasets:**

- Reduce batch size using `--batch-size` parameter
- Enable incremental mode to process only new data

### Logging

Logs are written to both console and `etl.log` file. Set `LOG_LEVEL=DEBUG` in your `.env` file for detailed debugging.
