# LookThroughProfits Supabase Data Pipeline

Automated stock data pipeline powering LookThroughProfits. Loads US and Canadian issuer listings, daily prices, and annual financial statements into a Supabase (Postgres) database. Uses a mix of SimFin (bulk) and Yahoo Finance for coverage, with batch inserts, progress logs, and .env-based configuration.

## Features

- CA daily prices via Yahoo Finance (parallel download, batch insert)
- US daily prices via SimFin bulk shareprices dataset
- Annual financial statements via SimFin bulk (US) and Yahoo Finance (CA) into a normalized `financials` table
- Insert-if-newer policy for prices (no upserts); supports time-based gating via `as_of` if present
- Exchange tagging (US/CA) and batched inserts for performance
- Supabase Postgres compatible schema and SQL helpers

## Repository structure

- `Orchestrator.py` — Orchestrates: TMX listings → CA financials (yfinance) → US financials (SimFin) → US prices (SimFin) → CA prices (yfinance)
- `ingestion/`
  - `ingest_yfinance_financials_api_to_postgres_ca.py` — CA annual/quarterly yfinance ingest into normalized `financials`
  - `ingest_simfin_financials_api_to_postgres_us.py` — US annual financials via SimFin into normalized `financials`
  - `ingest_simfin_prices_us.py` — US daily prices via SimFin bulk
  - `ingest_yfinance_prices_ca.py` — CA daily prices via yfinance (parallel, batch insert)
- `tools/`
  - `scrape_tsx_stocksymbols_ca.py` — Scrape TSX stock symbols (no prices) to `data/ca_tickers.txt`
- `sql/`
  - `create_tables.sql` — Financials table schema (statements)
  - `create_tables_stockprice.sql` — Stock prices table schema (daily OHLCV)
- `data/` — CSVs and ticker lists (local; large/generated files should be gitignored)
- `dependencies.txt` — Python dependencies

## Workflow (at a glance)

The orchestrator runs six steps in order:

```
1) Download official TMX symbol list (TMX API → data/tsx_tsxv_all_symbols.csv)
  scripts/download_tsx_symbols_from_api.py

2) Derive instrument types (ETFs, trusts, etc.) → instrument_meta
  ingestion/derive_instrument_types_ca.py

3) CA financials (yfinance, mandatory)
  ingestion/ingest_yfinance_financials_api_to_postgres_ca.py → financials

4) US financials (SimFin bulk)
  ingestion/ingest_simfin_financials_api_to_postgres_us.py → financials

5) US daily prices (SimFin bulk; variant=latest or daily)
  ingestion/ingest_simfin_prices_us.py → stock_prices (exchange='US')

6) CA daily prices (yfinance; parallel)
  ingestion/ingest_yfinance_prices_ca.py → stock_prices (exchange='CA')
```

**The Orchestrator handles all dependencies automatically** - just run `python Orchestrator.py`!

### Source details

- TMX Official API: JSON API providing complete TSX/TSXV symbol lists with all class suffixes (A, B, PR, UN, etc.). Used by `download_tsx_symbols_from_api.py` to generate the canonical symbol list.
- SimFin: Bulk download API returning zip files containing semicolon-delimited CSVs. Datasets used:
  - Financials (US): income, income-banks, income-insurance; balance (+-banks/+ -insurance); cashflow (+-banks/+ -insurance)
  - Prices (US): shareprices (variant=latest or daily)
- Yahoo Finance:
  - Financials (CA): pulled by `yfinance` and normalized to row-wise tags
  - Prices (CA): concurrent `yfinance` download of latest daily bar

## Setup

1. Create a Python virtual environment

PowerShell:

```
python -m venv venv
.\venv\Scripts\Activate.ps1
```

2. Install dependencies

```
python -m pip install -r requirements.txt
```

3. Configure environment variables in a `.env` file at the repo root

Required for database:

- `DB_HOST` — Postgres host (Supabase pooler host if applicable)
- `DB_PORT` — Postgres port (default 5432)
- `DB_NAME` — Database name
- `DB_USER` — Username
- `DB_PASSWORD` — Password

Optional for loaders:

- `SIMFIN_API_KEY` — SimFin API key (required for all SimFin ingesters)
- `SIMFIN_MARKET` — SimFin market code; defaults: 'us' for US datasets (and 'ca' for CA prices if using SimFin)
- `SIMFIN_PRICES_VARIANT` — 'latest' (default) or 'daily' for shareprices datasets.
- `SIMFIN_INSERT_BATCH` — Batch size for financials inserts (default 5000)
- `PRICES_INSERT_BATCH` — Batch size for prices inserts (default 1000)
- `CLEAR_STOCK_PRICES` — `true` to truncate prices before load (use with care)
- `YF_MAX_WORKERS` — Concurrency for yfinance (default 24)
- `YFIN_MAX_TICKERS` — Cap CA tickers processed (for smoke tests)
- `YF_USE_HISTORY` — Use per-symbol history path (default true)
- `YF_USE_FAST_INFO` — Use fast_info path (default false)
- `YF_USE_QUOTES` — Use Yahoo quote API path (default false; may 401)

Example `.env`:

```
DB_HOST=localhost
DB_PORT=5432
DB_NAME=financials
DB_USER=me
DB_PASSWORD=secret
SIMFIN_API_KEY=your_simfin_key
CA_TICKERS=FILE
CLEAR_STOCK_PRICES=false
SIMFIN_PRICES_VARIANT=latest
PRICES_INSERT_BATCH=1000
SIMFIN_INSERT_BATCH=5000
```

## Database schema

Apply the SQL files in `sql/` to your Postgres database (Supabase):

- `create_tables.sql` — creates `financials` for statements (IS/BS)
- `create_tables_stockprice.sql` — creates `stock_prices` for OHLCV with `exchange` column and useful indexes

## Usage

**Simplest approach - Run the orchestrator for complete end-to-end execution:**

```bash
python Orchestrator.py
```

This automatically:
1. Downloads the latest TMX symbol list (4,246 symbols)
2. Classifies instruments (ETFs, trusts, REITs, etc.)
3. Ingests CA financials
4. Ingests US financials (if `SIMFIN_API_KEY` set)
5. Ingests US prices (if `SIMFIN_API_KEY` set)
6. Ingests CA prices (Mode C: 71% coverage, ~2 minutes)

Optional: start with a clean slate using --truncate (clears financials and stock_prices once at the start):

```powershell
python Orchestrator.py --truncate
```

**Advanced: Run individual ingesters**

```bash
python scripts/download_tsx_symbols_from_api.py
python ingestion/derive_instrument_types_ca.py
python ingestion/ingest_yfinance_financials_api_to_postgres_ca.py
python ingestion/ingest_simfin_financials_api_to_postgres_us.py
python ingestion/ingest_simfin_prices_us.py
python ingestion/ingest_yfinance_prices_ca.py
```

Notes:

- **Zero manual steps required**: Orchestrator handles all dependencies automatically
- Prices: insert-only when incoming timestamp/date is newer than existing (no upserts); if table has `as_of` and SimFin provides `DateTime/Timestamp`, time-based gating is used; otherwise date-based (`latest_day`).
- CA financials are mandatory (yfinance) and feed the normalized `financials` table.
- CA prices: Mode C (history API, 71% coverage, ~2 minutes) configured in `.env`

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
