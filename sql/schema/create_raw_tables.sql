CREATE SCHEMA IF NOT EXISTS raw;
DROP TABLE raw.transactions;
CREATE TABLE IF NOT EXISTS raw.transactions (
    transaction_id BIGINT,
    group_id BIGINT,
    date DATE,
    cost DECIMAL(10, 2),
    currency_code TEXT,
    description TEXT,
    updated_at TIMESTAMP,
    created_at TIMESTAMP,
    is_payment BOOLEAN,
    users_json JSON,  -- you can also use JSON if supported natively
    category_id BIGINT,
    category_name TEXT,
    version_start TIMESTAMP,
    version_end TIMESTAMP,
    is_current BOOLEAN,
);


