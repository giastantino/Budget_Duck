CREATE SCHEMA IF NOT EXISTS raw;

-- Main transactions table (without user-specific data)
CREATE TABLE IF NOT EXISTS raw.transactions (
    transaction_id BIGINT PRIMARY KEY,
    group_id BIGINT NOT NULL,
    date DATE NOT NULL,
    cost DECIMAL(10,2) NOT NULL,
    currency_code VARCHAR(3) NOT NULL,
    description TEXT NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP NOT NULL,
    is_payment BOOLEAN NOT NULL DEFAULT FALSE,
    category_id BIGINT,
    category_name TEXT,
    version_start TIMESTAMP NOT NULL,
    version_end TIMESTAMP,
    is_current BOOLEAN NOT NULL DEFAULT TRUE
);

-- User payments table (one row per user per transaction)
CREATE TABLE IF NOT EXISTS raw.user_payments (
    transaction_id BIGINT NOT NULL,
    user_id VARCHAR(255) NOT NULL,
    user_last_name VARCHAR(255),
    owed_share DECIMAL(10,2) NOT NULL DEFAULT 0.00,
    paid_share DECIMAL(10,2) NOT NULL DEFAULT 0.00,
    net_balance DECIMAL(10,2) NOT NULL DEFAULT 0.00,
    version_start TIMESTAMP NOT NULL,
    version_end TIMESTAMP,
    is_current BOOLEAN NOT NULL DEFAULT TRUE,
    PRIMARY KEY (transaction_id, user_id, version_start)
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_transactions_group_current 
ON raw.transactions(group_id, is_current);

CREATE INDEX IF NOT EXISTS idx_transactions_updated_at 
ON raw.transactions(updated_at);

CREATE INDEX IF NOT EXISTS idx_transactions_date 
ON raw.transactions(date);

CREATE INDEX IF NOT EXISTS idx_user_payments_transaction_current
ON raw.user_payments(transaction_id, is_current);

CREATE INDEX IF NOT EXISTS idx_user_payments_user_current
ON raw.user_payments(user_id, is_current);