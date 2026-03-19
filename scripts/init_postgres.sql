-- Esquema de la tabla destino en Postgres para consumo de Power BI

CREATE TABLE IF NOT EXISTS financial_transactions (
    transaction_id   BIGINT PRIMARY KEY,
    account_id       BIGINT NOT NULL,
    amount           NUMERIC(18, 2) NOT NULL,
    currency         CHAR(3) NOT NULL DEFAULT 'USD',
    transaction_date TIMESTAMP NOT NULL,
    description      TEXT,
    category         VARCHAR(100),
    status           VARCHAR(20) NOT NULL,
    created_at       TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ft_updated_at ON financial_transactions (updated_at);
CREATE INDEX IF NOT EXISTS idx_ft_account_id ON financial_transactions (account_id);
CREATE INDEX IF NOT EXISTS idx_ft_status ON financial_transactions (status);

CREATE TABLE IF NOT EXISTS accounts (
    account_id     BIGINT PRIMARY KEY,
    account_number VARCHAR(50) NOT NULL UNIQUE,
    account_type   VARCHAR(50) NOT NULL,
    owner_name     VARCHAR(200) NOT NULL,
    balance        NUMERIC(18, 2) NOT NULL DEFAULT 0,
    is_active      BOOLEAN NOT NULL DEFAULT TRUE,
    created_at     TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at     TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Tabla de control para mantener watermarks entre ejecuciones
CREATE TABLE IF NOT EXISTS etl_watermarks (
    table_name       VARCHAR(200) PRIMARY KEY,
    last_watermark   TIMESTAMP,
    last_run_at      TIMESTAMP DEFAULT NOW(),
    records_processed BIGINT DEFAULT 0
);
