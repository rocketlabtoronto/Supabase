<<<<<<< HEAD
# Supabase
Automated stock data pipeline powering LookThroughProfits. Loads US/CA prices from Yahoo Finance and SimFin, imports SimFin financials from CSV, and writes to Supabase Postgres. Features smart resume, rate limiting, exchange tagging, .env config, and robust error handling and logging.
=======
# LookThroughProfits-Supabase

Small data ingestion toolkit that loads company financials into a Postgres database. The repository contains scripts to fetch Canadian tickers, import Yahoo (yfinance) data for TSX (.TO) tickers, and import SimFin CSV exports for US companies.

Contents

- download_ca_tickers.py - scrape TSX tickers from EODData and save to data/ca_tickers.txt
- update_financials.py - top-level runner that executes scripts in `scripts/`
- scripts/load_yfinance_ca.py - load annual income / balance / cashflow fields from yfinance for Canadian tickers
- scripts/load_simfin.py - load SimFin CSV exports (semicolon-separated) into DB
- scripts/yfinance_fields_explorer.py - helper to dump available yfinance field names to CSV
- data/ - expected data files (see below)
- sql/create_tables.sql - recommended table schema (financials table)
- requirements.txt - Python dependencies

Quick start

1. Create and activate a Python virtual environment
   PowerShell:
   python -m venv venv; .\venv\Scripts\Activate.ps1

2. Install dependencies
   python -m pip install -r requirements.txt

3. Copy or create a .env file in the repository root with database and config variables (example below).

Environment variables (used by scripts)

- DB_HOST - Postgres host
- DB_PORT - Postgres port (defaults to 5432)
- DB_NAME - Postgres database name
- DB_USER - Postgres username
- DB_PASSWORD - Postgres password
- CA_TICKERS - either a comma-separated list of tickers (e.g. "BNS.TO,RY.TO") or the literal "FILE" to read `data/ca_tickers.txt`
- SIMFIN_DATA_DIR - directory containing SimFin CSV files (defaults to `data`)

Example .env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=financials
DB_USER=me
DB_PASSWORD=secret
CA_TICKERS=FILE
SIMFIN_DATA_DIR=data

Database: expected table
All loader scripts insert into a table named `financials`. See `sql/create_tables.sql` for a recommended schema. The code expects at least these columns:

- ticker (text)
- exchange (text) — e.g. "CA" or "US"
- fy_end_date (date)
- stmt_type (text) — "IS" or "BS"
- tag (text) — semantic tag like Revenue, NetIncome, Equity, etc.
- value (numeric)
- unit (text) — currency, e.g. "CAD" or "USD"
- source (text) — e.g. "Yahoo" or "SimFin"

How each script works

- download_ca_tickers.py
  Scrapes EODData (A–Z pages) for TSX symbols and writes `data/ca_tickers.txt`. Symbols are written with the ".TO" suffix for Yahoo/yfinance compatibility.

- scripts/load_yfinance_ca.py
  Reads tickers from the `CA_TICKERS` env var (or `data/ca_tickers.txt` if CA_TICKERS=FILE). For each ticker it uses yfinance to fetch balance sheet, income statement and cashflow. It maps a small set of fields (see FIELDS dict in file) and inserts rows into `financials`. Behavior notes:

  - Skips tickers with missing IS or BS data
  - Uses `ON CONFLICT DO NOTHING` to avoid duplicate inserts
  - Removes the `.TO` suffix when inserting the `ticker` value

- scripts/load_simfin.py
  Loads SimFin CSV exports (semicolon-separated) for annual Income Statement and Balance Sheet. Mapped column names are defined in `INCOME_TAGS` and `BALANCE_TAGS`. The script expects CSV files like `Income_Statement_Annual.csv` and `Balance_Sheet_Annual.csv` in `SIMFIN_DATA_DIR`.

- scripts/yfinance_fields_explorer.py
  Interactive helper that queries yfinance for a supplied ticker and writes a CSV listing available fields (info, income, balance, cashflow) to help expand or adjust mappings.

Top-level runner

- update_financials.py runs the scripts listed in the SCRIPTS constant (by default it runs `scripts/load_yfinance_ca.py` and `scripts/load_simfin.py`). Run it to perform both imports:
  python update_financials.py

Data notes

- SimFin CSVs are semicolon-separated. The loader uses pandas.read_csv(sep=';')
- `data/ca_tickers.txt` should contain one ticker per line ending with `.TO` for Yahoo

Troubleshooting

- Database connection errors: verify .env variables and that Postgres accepts connections from your host.
- Missing tickers: set CA_TICKERS=FILE and generate `data/ca_tickers.txt` using `download_ca_tickers.py`.
- yfinance missing fields / rate limits: yfinance can return empty DataFrames for some tickers. The scripts skip missing values and continue. Consider running smaller batches or adding retries.

Extending

- Add more field mappings in `FIELDS` (for `load_yfinance_ca.py`) or `INCOME_TAGS/BALANCE_TAGS` (`load_simfin.py`) to capture additional metrics.
- You can modify insert behavior (e.g. upsert rather than `ON CONFLICT DO NOTHING`) to update existing records.

License
No license specified. Add a LICENSE file if you plan to share the project publicly.

Contact / next steps
If you want, I can:

- Add a `Makefile` / PowerShell script to run the whole pipeline
- Create a simple test database and CI check
- Generate a Postgres migration from `sql/create_tables.sql`
>>>>>>> 7d8fb6c (Initial commit: LookThroughProfits stock price automation system)
