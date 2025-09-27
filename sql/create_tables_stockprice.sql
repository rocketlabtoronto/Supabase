create table if not exists stock_prices (
  id serial primary key,
  symbol text not null,
  exchange text not null default 'US',  -- 'US', 'CA', etc.
  open numeric,
  high numeric,
  low numeric,
  price numeric,
  volume bigint,
  latest_day date,
  previous_close numeric,
  change numeric,
  change_percent text,
  inserted_at timestamp default now()
);

-- Index for efficient lookups by symbol and date
create index if not exists idx_stock_prices_symbol_date on stock_prices (symbol, latest_day);

-- Index for efficient lookups by symbol only
create index if not exists idx_stock_prices_symbol on stock_prices (symbol);

-- Index for efficient lookups by exchange
create index if not exists idx_stock_prices_exchange on stock_prices (exchange);

-- Index for efficient lookups by exchange and symbol
create index if not exists idx_stock_prices_exchange_symbol on stock_prices (exchange, symbol);

-- Unique constraint to prevent duplicate symbol/date combinations
create unique index if not exists idx_stock_prices_unique on stock_prices (symbol, latest_day, exchange);

-- Sample insert based on your example data:
-- INSERT INTO stock_prices (symbol, exchange, open, high, low, price, volume, latest_day, previous_close, change, change_percent)
-- VALUES ('IBM', 'US', 245.23, 245.4599, 241.72, 243.49, 2967558, '2025-08-29', 245.73, -2.24, '-0.91%');
