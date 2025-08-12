CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.transactions (
    transaction_id BIGINT PRIMARY KEY,
    group_id BIGINT NOT NULL,
    date DATE NOT NULL,
    cost DECIMAL(10,2) NOT NULL,
    currency_code VARCHAR(3) NOT NULL,
    description TEXT NOT NULL,  -- TEXT type handles UTF-8 better than VARCHAR
    updated_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP NOT NULL,
    is_payment BOOLEAN NOT NULL DEFAULT FALSE,
    category_id BIGINT,
    category_name TEXT,  -- TEXT for UTF-8 category names
    users_json TEXT NOT NULL,  -- JSON data as TEXT with UTF-8 support
    version_start TIMESTAMP NOT NULL,
    version_end TIMESTAMP,
    is_current BOOLEAN NOT NULL DEFAULT TRUE
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_transactions_group_current 
ON raw.transactions(group_id, is_current);

CREATE INDEX IF NOT EXISTS idx_transactions_updated_at 
ON raw.transactions(updated_at);

CREATE INDEX IF NOT EXISTS idx_transactions_date 
ON raw.transactions(date);
