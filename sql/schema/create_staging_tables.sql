CREATE SCHEMA IF NOT EXISTS staging;

-- Normalized transactions table (business entities)
CREATE TABLE IF NOT EXISTS staging.transactions (
    transaction_id BIGINT PRIMARY KEY,
    group_id BIGINT NOT NULL,
    date DATE NOT NULL,
    cost DECIMAL(10,2) NOT NULL,
    currency_code VARCHAR(3) NOT NULL,
    description TEXT NOT NULL,
    category_id BIGINT,
    category_name TEXT,
);

-- User-transaction relationship (fact table)
CREATE TABLE IF NOT EXISTS staging.transaction_participants (
    transaction_id BIGINT NOT NULL,
    user_id TEXT NOT NULL,
    owed_share DECIMAL(10,2) NOT NULL,
    paid_share DECIMAL(10,2) NOT NULL,
    net_balance DECIMAL(10,2) NOT NULL,
    
    -- Derived flags for easy querying
    is_payer BOOLEAN GENERATED ALWAYS AS (paid_share > 0) STORED,
    is_debtor BOOLEAN GENERATED ALWAYS AS (owed_share > 0) STORED,
    
    -- Audit fields
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    PRIMARY KEY (transaction_id, user_id),
    FOREIGN KEY (transaction_id) REFERENCES staging.transactions(transaction_id),
);

-- Group dimension table
CREATE TABLE IF NOT EXISTS staging.groups (
    group_id BIGINT PRIMARY KEY,
    group_name TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN NOT NULL DEFAULT TRUE
);

-- Group membership (many-to-many relationship)
CREATE TABLE IF NOT EXISTS staging.group_members (
    group_id BIGINT NOT NULL,
    user_id TEXT NOT NULL
    
    PRIMARY KEY (group_id, user_id),
    FOREIGN KEY (group_id) REFERENCES staging.groups(group_id)
);

-- Categories dimension table
CREATE TABLE IF NOT EXISTS staging.categories (
    category_id BIGINT PRIMARY KEY,
    category_name TEXT NOT NULL,
    category_mapped TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Performance indexes
CREATE INDEX IF NOT EXISTS idx_staging_transactions_group_date 
ON staging.transactions(group_id, date, is_current);

CREATE INDEX IF NOT EXISTS idx_staging_transactions_category 
ON staging.transactions(category_id, is_current);

CREATE INDEX IF NOT EXISTS idx_staging_transaction_participants_user 
ON staging.transaction_participants(user_id);

CREATE INDEX IF NOT EXISTS idx_staging_transaction_participants_transaction 
ON staging.transaction_participants(transaction_id);

CREATE INDEX IF NOT EXISTS idx_staging_group_members_active 
ON staging.group_members(group_id, is_active);