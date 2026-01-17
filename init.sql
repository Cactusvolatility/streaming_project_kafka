CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS prices (
    event_time TIMESTAMPTZ NOT NULL,
    ingest_ts  TIMESTAMPTZ NOT NULL DEFAULT now(),
    symbol     TEXT NOT NULL,
    price      DOUBLE PRECISION,
    volume     DOUBLE PRECISION,
    sequence   BIGINT NOT NULL,
    PRIMARY KEY (event_time, symbol, sequence)  -- event_time MUST be first or included
);

SELECT create_hypertable('prices', 'event_time', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_prices_symbol_time
ON prices (symbol, event_time DESC);