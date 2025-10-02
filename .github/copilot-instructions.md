## Purpose
Short, focused guide for AI coding agents to be immediately productive in this repository.

## Big picture (read these files first)
- `Orchestrator.py` — the end-to-end runner (listings → CA financials → US financials → US prices → CA prices).
- `ingestion/` — ingest scripts (SimFin bulk for US; yfinance for CA). Key files:
  - `ingest_simfin_financials_api_to_postgres_us.py` (SimFin bulk → normalized `financials`)
  - `ingest_simfin_prices_us.py` (SimFin shareprices → `stock_prices`)
  - `ingest_yfinance_financials_api_to_postgres_ca.py` and `ingest_yfinance_prices_ca.py` (yfinance CA flows)
- `scripts/get_tmx_listed_companies.py` — TMX issuer list extraction that produces `data/tmx_listed_companies.csv`.
- `utils/` — small helpers: `logger.py` (logging setup), `symbols.py` (symbol ↔ Yahoo mapping).
- `sql/` — authoritative DB schema; ingestion code relies on specific column names (see `create_tables*.sql`).

## Developer workflows and commands
- Create venv and install deps (PowerShell):
  python -m venv venv; .\venv\Scripts\Activate.ps1; python -m pip install -r requirements.txt
- One-shot pipeline: `python Orchestrator.py` (use `--truncate` to clear tables first). Or run ingesters individually:
  - `python scripts/get_tmx_listed_companies.py`
  - `python ingestion/ingest_simfin_financials_api_to_postgres_us.py`
  - `python ingestion/ingest_simfin_prices_us.py`
  - `python ingestion/ingest_yfinance_prices_ca.py`
- Environment: use a `.env` at repo root. Required DB vars: `DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD`.
  SimFin: `SIMFIN_API_KEY` (ingesters skip SimFin steps when missing). Common optional vars: `SIMFIN_MARKET, SIMFIN_PRICES_VARIANT, PRICES_INSERT_BATCH, SIMFIN_INSERT_BATCH, CLEAR_STOCK_PRICES, LOG_LEVEL`.

## Project-specific conventions & patterns (important to preserve)
- SimFin bulk API: ingestion scripts expect a ZIP containing a semicolon-delimited CSV. See `fetch_bulk_dataset()` in SimFin ingesters.
- Insert-only policy for prices: scripts do NOT perform upserts; they insert only when incoming row is newer than stored data. The gating uses `as_of` (timestamp) if present; otherwise `latest_day` (date).
- Batch inserts: ingestion uses `psycopg2.extras.execute_values` for performance. Batch sizes controlled by env `SIMFIN_INSERT_BATCH` and `PRICES_INSERT_BATCH`.
- Tag maps: the financials ingester uses tag → candidate column lists (INCOME_TAGS, BALANCE_TAGS, CASHFLOW_TAGS). Changes to these maps directly change what gets persisted.
- Logging and telemetry: ingesters write an `ingest_logs` entry at end/start/failure. Use these rows to summarize runs.
- Robust column detection: scripts defensively probe CSV/Excel structure (see `ensure_ticker_column()` and header detection in `get_tmx_listed_companies.py`).

## Integration points & external dependencies
- SimFin (bulk ZIP API) — used for US financials and prices. Requires `SIMFIN_API_KEY`.
- Yahoo Finance (via `yfinance`) — used for CA financials and prices; concurrency controlled by `YF_MAX_WORKERS`.
- Postgres / Supabase — authoritative sink. The SQL in `sql/` defines table names/columns the code expects (financials, stock_prices, instrument_meta, ingest_logs).

## Debugging and modification guidance
- Use `LOG_LEVEL` to surface more logs; `utils.get_logger()` centralizes logging config.
- When changing DB-related code, first inspect `sql/create_tables*.sql` to ensure column names (especially `as_of`, `latest_day`, `previous_close`) are preserved.
- SimFin failures: scripts log errors and exit non‑zero. Orchestrator will skip SimFin steps if `SIMFIN_API_KEY` is not set.
- To test incremental price gating, check `table_has_column(conn, 'stock_prices', 'as_of')` logic in `ingest_simfin_prices_us.py` — this controls timestamp vs date gating.
- For symbol mapping and Yahoo variants, inspect `utils/symbols.py` (functions `tmx_symbol`, `yahoo_base_from_symbol`, `yahoo_variants_all`) and `instrument_meta` lookups.

## When you touch ingestion code — checklist
1. Keep `SIMFIN_*` env semantics intact (don't rename without updating Orchestrator and README).
2. Preserve tag maps shape (dict[tag] -> list[candidate_columns]) or update documentation & tests.
3. Maintain insert-only semantics for `stock_prices` unless you update SQL and tests.
4. Update `ingest_logs` entries so operational dashboards remain meaningful.

## Files to inspect for examples
- Orchestrator.py (process orchestration, error handling, PYTHONPATH injection)
- ingestion/ingest_simfin_financials_api_to_postgres_us.py (tag mapping, batch insert pattern)
- ingestion/ingest_simfin_prices_us.py (bulk CSV -> filter -> batch insert; gating logic)
- scripts/get_tmx_listed_companies.py (Excel parsing heuristics)
- utils/logger.py and utils/symbols.py
- sql/*.sql (DB schema expectations)

If anything above is unclear or you'd like more examples (small unit tests, a smoke test runner, or more detailed DB column references), tell me which area to expand and I'll iterate.
