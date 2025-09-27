# LookThroughProfits Supabase Data Pipeline

Automated stock data pipeline powering LookThroughProfits. Loads US and Canadian daily prices from Yahoo Finance (yfinance) and imports SimFin financial statements from CSV into a Supabase (Postgres) database. Includes smart resume, rate limiting, exchange tagging, and .env-based configuration.

## Features

- US/CA daily prices via yfinance with robust error handling and configurable delays
- US prices via SimFin API (optional) with rate limiting and batching tuned for free tier
- Annual financial statements via SimFin CSV exports (semicolon-delimited)
- Smart resume: continues where it left off based on last processed ticker/date
- Exchange tagging (US/CA) and upsert-safe inserts
- Supabase Postgres compatible schema and SQL helpers

## Repository structure

- `Orchestrator.py` — Orchestrates CA Yahoo financials + SimFin CSV loading
- `ingestion/`
  - `ingest_yahoo_financials_postgres_ca.py` — Canadian annual financials via Yahoo Finance
  - `ingest_simfin_financials_csv_to_postgres_us.py` — Import IS/BS from SimFin CSV exports
  - `ingest_yahoo_prices_us.py` — US daily prices via Yahoo Finance (smart resume)
  - `ingest_yahoo_prices_ca.py` — Canadian daily prices via Yahoo Finance
- `tools/`
  - `scrape_tsx_stocksymbols_ca.py` — Scrape TSX stock symbols (no prices) to `data/ca_tickers.txt`
  - `explore_yahoo_finance_fields.py` — Discover available Yahoo Finance fields
- `sql/`
  - `create_tables.sql` — Financials table schema (statements)
  - `create_tables_stockprice.sql` — Stock prices table schema (daily OHLCV)
- `data/` — CSVs and ticker lists (ignored by git)
- `dependencies.txt` — Python dependencies

## Setup

1. Create a Python virtual environment

PowerShell:

```
python -m venv venv
.\venv\Scripts\Activate.ps1
```

2. Install dependencies

```
python -m pip install -r dependencies.txt
```

3. Configure environment variables in a `.env` file at the repo root

Required for database:

- `DB_HOST` — Postgres host (Supabase pooler host if applicable)
- `DB_PORT` — Postgres port (default 5432)
- `DB_NAME` — Database name
- `DB_USER` — Username
- `DB_PASSWORD` — Password

Optional for loaders:

- `SIMFIN_API_KEY` — SimFin API key (for future SimFin API loaders)
- `SIMFIN_DATA_DIR` — Folder containing SimFin CSV exports (default `data`)
- `CA_TICKERS` — Comma list (e.g. `BNS.TO,RY.TO`) or `FILE` to read `data/ca_tickers.txt`
- `YFINANCE_DELAY` — Seconds between requests (default `1.0` US, `0.5` CA)
- `SIMFIN_BATCH_SIZE` — Companies per request for SimFin (free tier: `1`)
- `CLEAR_STOCK_PRICES` — `true` to truncate prices before load (use with care)

Example `.env`:

```
DB_HOST=localhost
DB_PORT=5432
DB_NAME=financials
DB_USER=me
DB_PASSWORD=secret
SIMFIN_API_KEY=your_simfin_key
SIMFIN_DATA_DIR=data
CA_TICKERS=FILE
YFINANCE_DELAY=1.0
SIMFIN_BATCH_SIZE=1
CLEAR_STOCK_PRICES=false
```

## Database schema

Apply the SQL files in `sql/` to your Postgres database (Supabase):

- `create_tables.sql` — creates `financials` for statements (IS/BS)
- `create_tables_stockprice.sql` — creates `stock_prices` for OHLCV with `exchange` column and useful indexes

## Usage

Run orchestrator:

```
python Orchestrator.py
```

Or run individual ingesters, e.g.:

```
python ingestion/ingest_yahoo_prices_us.py
python ingestion/ingest_yahoo_prices_ca.py
python ingestion/ingest_simfin_financials_csv_to_postgres_us.py
python ingestion/ingest_yahoo_financials_postgres_ca.py
```

Notes:

- The yfinance loaders implement delays to respect rate limits.
- US loader resumes from the last processed ticker automatically.
- Canadian loader normalizes tickers and tags `exchange='CA'`.
- SimFin CSVs must be semicolon-delimited and placed in `SIMFIN_DATA_DIR`.
- Legacy scripts under `scripts/` remain as shims and will forward to the new modules under `ingestion/` and `tools/`. Prefer calling the new paths directly.

## Troubleshooting

- Connection issues: verify `.env` and that your Supabase IP is allowed.
- Empty or missing data: some tickers may be delisted or lack fields; the loaders skip and continue.
- Non-fast-forward pushes: `git pull --rebase origin main` then `git push`.

## Contributing / Next steps

- Add CI to run a lightweight smoke test against a local Postgres
- Expand field mappings and add unit tests for transforms
- Optional: Dockerfile and Compose for local DB and runners

## License

No license specified. Add a LICENSE file if you plan to share publicly.
