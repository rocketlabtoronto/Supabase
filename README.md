# LookThroughProfits Supabase Data Pipeline

Automated stock data pipeline powering LookThroughProfits. Loads US and Canadian daily prices and annual financial statements from SimFin into a Supabase (Postgres) database via the SimFin Bulk API (CSV-in-zip, parsed in-memory). Includes batching, progress logs, and .env-based configuration.

## Features

- US/CA daily prices via SimFin bulk shareprices dataset (no Yahoo fallback)
- Annual financial statements via SimFin bulk datasets (income/balance/cashflow)
- Insert-if-newer policy for prices (no upserts); supports time-based gating via `as_of` if present
- Exchange tagging (US/CA) and batched inserts for performance
- Supabase Postgres compatible schema and SQL helpers

## Repository structure

- `Orchestrator.py` — Orchestrates SimFin financials (CA/US) and SimFin prices (US/CA)
- `ingestion/`
  - `ingest_simfin_financials_csv_to_postgres_ca.py` — Canadian annual financials via SimFin (banks/insurance/general; IS/BS/CF)
  - `ingest_simfin_financials_csv_to_postgres_us.py` — US annual financials via SimFin (banks/insurance/general; IS/BS/CF)
  - `ingest_simfin_prices_us.py` — US daily prices via SimFin
  - `ingest_simfin_prices_ca.py` — Canadian daily prices via SimFin
- `tools/`
  - `scrape_tsx_stocksymbols_ca.py` — Scrape TSX stock symbols (no prices) to `data/ca_tickers.txt`
- `sql/`
  - `create_tables.sql` — Financials table schema (statements)
  - `create_tables_stockprice.sql` — Stock prices table schema (daily OHLCV)
- `data/` — CSVs and ticker lists (local; large/generated files should be gitignored)
- `dependencies.txt` — Python dependencies

## Workflow (at a glance)

There are four primary ingestion flows. The Orchestrator runs all four in order.

```
1) TSX symbols (optional)
   EODData.com (TSX listings HTML)
     ---> tools/scrape_tsx_stocksymbols_ca.py
     ---> data/ca_tickers.txt (one .TO symbol per line)
     [Used when CA_TICKERS=FILE]

2) CA financials (Orchestrator step 1)
   SimFin Bulk API (income/balance/cashflow; banks/insurance/general)
     ---> ingestion/ingest_simfin_financials_csv_to_postgres_ca.py
     ---> financials table (IS/BS/CF)

3) US financials (Orchestrator step 2)
   SimFin Bulk API (income/balance/cashflow; banks/insurance/general)
     ---> ingestion/ingest_simfin_financials_csv_to_postgres_us.py
     ---> financials table (IS/BS/CF)

4) US daily prices (Orchestrator step 3)
  SimFin Bulk API (shareprices; variant=latest or daily)
    ---> ingestion/ingest_simfin_prices_us.py
     ---> stock_prices (exchange='US')

5) CA daily prices (Orchestrator step 4)
  SimFin Bulk API (shareprices; variant=latest or daily)
    ---> ingestion/ingest_simfin_prices_ca.py
     ---> stock_prices (exchange='CA')
```

### Source details

- EODData (https://www.eoddata.com/): Public website with TSX listings rendered as HTML. The scraper parses symbol links from A–Z pages using a regex; there is no REST API used. Output is a plain text file `data/ca_tickers.txt` with one `.TO`-suffixed symbol per line.
- SimFin: Bulk download API returning zip files containing semicolon-delimited CSVs. We read them directly into pandas without writing to disk. Datasets used:
  - Financials: income, income-banks, income-insurance; balance, balance-banks, balance-insurance; cashflow, cashflow-banks, cashflow-insurance.
  - Prices: shareprices (variant=latest or daily).

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
- `SIMFIN_MARKET` — SimFin market code; defaults: 'world' (CA financials/prices), 'us' (US prices). You can override per run via env.
- `SIMFIN_PRICES_VARIANT` — 'latest' (default) or 'daily' for shareprices datasets.
- `SIMFIN_INSERT_BATCH` — Batch size for financials inserts (default 5000)
- `PRICES_INSERT_BATCH` — Batch size for prices inserts (default 1000)
- `CA_TICKERS` — Comma list (e.g. `BNS.TO,RY.TO`) or `FILE` to read `data/ca_tickers.txt` (used to filter CA SimFin financials)
- `CLEAR_STOCK_PRICES` — `true` to truncate prices before load (use with care)

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

Run orchestrator (runs CA financials → US financials → US prices → CA prices):

```
python Orchestrator.py
```

Or run individual ingesters, e.g.:

```
python ingestion/ingest_simfin_prices_us.py
python ingestion/ingest_simfin_prices_ca.py
python ingestion/ingest_simfin_financials_csv_to_postgres_us.py
python ingestion/ingest_simfin_financials_csv_to_postgres_ca.py
```

Notes:

- Prices: insert-only when incoming timestamp/date is newer than existing (no upserts); if table has `as_of` and SimFin provides `DateTime/Timestamp`, time-based gating is used; otherwise date-based (`latest_day`).
- Canadian financials use CA_TICKERS filter (env or `data/ca_tickers.txt`).
- Legacy Yahoo-based loaders are deprecated and now delegate to SimFin equivalents where present.

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
