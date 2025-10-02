"""
Ingest SimFin US financials by downloading datasets via the SimFin Bulk API
into in-memory pandas DataFrames (no disk writes), then inserting rows into
Postgres. API-only ingestion, no local CSV fallback.

Env vars:
- SIMFIN_API_KEY   (required) authorization for SimFin API
- SIMFIN_MARKET    (optional) defaults to 'us'

DB connection uses: DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
"""

import os, sys, math, io, zipfile, time, json
import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()

# Tag maps: logical tag -> list of candidate column names in SimFin CSVs.
# Using candidates makes it robust across banks vs industrial datasets.
INCOME_TAGS = {
    "SharesOutstanding": ["Shares (Basic)", "Shares (Diluted)"],
    "Revenue": ["Revenue", "Total Revenue", "Sales"],
    "GrossProfit": ["Gross Profit"],
    "OperatingProfit": ["Operating Income (Loss)"],
    "NetIncome": ["Net Income", "Net Income (Common)"]
}

BALANCE_TAGS = {
    "Equity": ["Total Equity"],
    "Liabilities": ["Total Liabilities"],
    "CashAndEquivalents": ["Cash, Cash Equivalents & Short Term Investments"],
    "Assets": ["Total Assets"],
}

# Cash Flow tags: keep it compact and robust with multiple candidate names.
CASHFLOW_TAGS = {
    "OperatingCashFlow": [
        "Net Cash from Operating Activities",
        "Operating Cash Flow",
        "Cash From Operations",
        "Net Cash Provided by Operating Activities",
    ],
    "FreeCashFlow": [
        "Free Cash Flow",
    ],
    "CapitalExpenditure": [
        "Capital Expenditures",
        "Capital Expenditure",
        "Purchase Of PPE",
    ],
    "ChangeInCash": [
        "Change in Cash",
        "Changes In Cash",
        "Net Change in Cash",
    ],
}


def get_env(name: str, default=None):
    v = os.getenv(name)
    if v is None:
        return default
    v = str(v).strip()
    return v if v else default


def get_env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None:
        return default
    try:
        i = int(str(v).strip())
        return i if i > 0 else default
    except Exception:
        return default


def safe(val):
    if pd.isna(val):
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        # Try string with thousands separators
        try:
            return float(str(val).replace(',', ''))
        except Exception:
            return None


def fetch_bulk_dataset(dataset: str, market: str, variant: str, api_key: str, timeout: int = 90) -> pd.DataFrame:
    """Fetch a SimFin bulk dataset (zip with a semicolon-delimited CSV) into memory and return a pandas DataFrame."""
    base_url = "https://prod.simfin.com/api/bulk-download/s3"
    params = {"dataset": dataset, "market": market, "variant": variant}
    headers = {"Authorization": f"api-key {api_key}"}
    r = requests.get(base_url, params=params, headers=headers, timeout=timeout)
    if not r.ok:
        raise RuntimeError(f"Bulk API error {r.status_code}: {r.text[:300]}")
    with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
        names = [n for n in zf.namelist() if n.lower().endswith('.csv')]
        if not names:
            raise RuntimeError("Bulk zip contained no CSV file")
        with zf.open(names[0]) as f:
            return pd.read_csv(f, sep=';', header=0, low_memory=False)


def ensure_ticker_column(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure we have a 'Ticker' column even if it was an index level."""
    if 'Ticker' in df.columns:
        return df
    idx_names = getattr(df.index, 'names', None)
    if df.index.name == 'Ticker' or (idx_names and 'Ticker' in idx_names):
        try:
            return df.reset_index()
        except Exception:
            return df
    return df


def pick_first(row: pd.Series, candidates: list[str]):
    for c in candidates:
        if c in row.index:
            val = safe(row.get(c))
            if val is not None:
                return val
    return None


def load_df(conn, df: pd.DataFrame, stmt_type: str, tag_map: dict[str, list[str]], inserted_tickers: set | None = None):
    """Insert rows for a given statement DataFrame using provided tag mapping.

    Optimized for performance using batched inserts (execute_values).
    Batch size can be controlled via SIMFIN_INSERT_BATCH (default 5000 rows).
    """
    df = ensure_ticker_column(df)
    if 'Ticker' not in df.columns:
        print(f"[warn] No 'Ticker' column present. Skipping {stmt_type}.")
        return 0

    total = len(df)
    cur = conn.cursor()
    conn.autocommit = True
    last_pct = -1
    batch_size = get_env_int('SIMFIN_INSERT_BATCH', 5000)
    buffer = []
    inserted_count = 0

    def flush_buffer():
        if not buffer:
            return
        sql = (
            "INSERT INTO financials "
            "(ticker, exchange, fy_end_date, stmt_type, tag, value, unit, source) VALUES %s"
        )
        execute_values(cur, sql, buffer, template="(%s,%s,%s,%s,%s,%s,%s,%s)")
        buffer.clear()

    print(f"[load] {stmt_type} start: rows={total} batch_size={batch_size}")

    for i, (_, row) in enumerate(df.iterrows()):
        ticker = row.get('Ticker')
        if pd.isna(ticker) or not str(ticker).strip():
            # Some aggregated rows might be empty / malformed.
            continue

        try:
            fy_end = pd.to_datetime(row.get('Report Date')).date()
        except Exception:
            # Invalid or missing date → skip the row.
            continue

        currency = row.get('Currency', 'USD')

        tkr = str(ticker).upper()
        for tag, cols in tag_map.items():
            val = pick_first(row, cols)
            if val is None:
                continue
            buffer.append((tkr, "US", fy_end, stmt_type, tag, val, currency, "SimFin"))
            inserted_count += 1
            if inserted_tickers is not None:
                inserted_tickers.add(tkr)
            if len(buffer) >= batch_size:
                flush_buffer()

        pct = math.floor((i + 1) / total * 100) if total else 100
        if pct != last_pct:
            print(f"[{stmt_type}] {pct}% ({i + 1}/{total})")
            last_pct = pct

    # Final flush
    flush_buffer()
    cur.close()
    print(f"[ingest_simfin_api] Done {stmt_type}. inserted_rows={inserted_count}")
    return inserted_count
def main():
    # Connect to Postgres
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT", 5432),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
    )

    api_key = get_env('SIMFIN_API_KEY')
    market = get_env('SIMFIN_MARKET', 'us')
    # Always use annual statements (business rule: NEVER ingest quarterly)
    variant = 'annual'
    print(f"[config] market={market} variant={variant} batch={get_env_int('SIMFIN_INSERT_BATCH', 5000)}")
    # API-only ingestion - no local CSV fallback.

    # API ingestion using in-memory DataFrames.
    did_api = False
    # Logging: start timer and basic context
    script_name = os.path.basename(__file__)
    start_ts = time.time()
    log_details = {
        "market": market,
        "variant": variant,
        "batch": get_env_int('SIMFIN_INSERT_BATCH', 5000),
    }

    def log_run(status: str, message: str = None, details: dict | None = None):
        ended = time.time()
        duration_ms = int((ended - start_ts) * 1000)
        d = details or {}
        # Merge base details
        merged = {**log_details, **d}
        cur = conn.cursor()
        cur.execute(
            """
            insert into ingest_logs
            (script, status, message, details, started_at, ended_at, duration_ms)
            values (%s,%s,%s,%s, to_timestamp(%s), to_timestamp(%s), %s)
            """,
            (
                script_name,
                status,
                message,
                json.dumps(merged),
                start_ts,
                ended,
                duration_ms,
            ),
        )
        conn.commit()

    try:
        # Income: banks, insurance, then general
        df_income_parts = []
        income_datasets = ['income-banks', 'income-insurance', 'income']
        print(f"[stage] Income datasets → {', '.join(income_datasets)}")
        for ds in income_datasets:
            try:
                print(f"[download] {ds} …")
                _df = fetch_bulk_dataset(ds, market, variant, api_key)
                print(f"[download] {ds} ok: rows={len(_df)}")
                df_income_parts.append(_df)
            except Exception as e:
                print(f"[ingest_simfin_api] API fetch failed for {ds}: {e}")
        inserted_tickers: set[str] = set()
        total_candidate_tickers: set[str] = set()
        if df_income_parts:
            pre_rows = sum(len(x) for x in df_income_parts)
            df_income = pd.concat(df_income_parts, ignore_index=True)
            # Drop possible duplicates by Ticker + Report Date
            if {'Ticker', 'Report Date'}.issubset(df_income.columns):
                df_income = df_income.drop_duplicates(subset=['Ticker', 'Report Date'])
            print(f"[merge] IS concat_rows={pre_rows} dedup_rows={len(df_income)} key=['Ticker','Report Date']")
            total_candidate_tickers.update(set(ensure_ticker_column(df_income)['Ticker'].dropna().astype(str).str.upper().unique()))
            is_inserted = load_df(conn, df_income, 'IS', INCOME_TAGS, inserted_tickers)
            print(f"[summary] IS inserted_rows={is_inserted}")

        # Balance: banks, insurance, then general
        df_balance_parts = []
        balance_datasets = ['balance-banks', 'balance-insurance', 'balance']
        print(f"[stage] Balance datasets → {', '.join(balance_datasets)}")
        for ds in balance_datasets:
            try:
                print(f"[download] {ds} …")
                _df = fetch_bulk_dataset(ds, market, variant, api_key)
                print(f"[download] {ds} ok: rows={len(_df)}")
                df_balance_parts.append(_df)
            except Exception as e:
                print(f"[ingest_simfin_api] API fetch failed for {ds}: {e}")
        if df_balance_parts:
            pre_rows = sum(len(x) for x in df_balance_parts)
            df_balance = pd.concat(df_balance_parts, ignore_index=True)
            if {'Ticker', 'Report Date'}.issubset(df_balance.columns):
                df_balance = df_balance.drop_duplicates(subset=['Ticker', 'Report Date'])
            print(f"[merge] BS concat_rows={pre_rows} dedup_rows={len(df_balance)} key=['Ticker','Report Date']")
            total_candidate_tickers.update(set(ensure_ticker_column(df_balance)['Ticker'].dropna().astype(str).str.upper().unique()))
            bs_inserted = load_df(conn, df_balance, 'BS', BALANCE_TAGS, inserted_tickers)
            print(f"[summary] BS inserted_rows={bs_inserted}")

        # Cash Flow: banks, insurance, then general
        df_cash_parts = []
        cash_datasets = ['cashflow-banks', 'cashflow-insurance', 'cashflow']
        print(f"[stage] Cash Flow datasets → {', '.join(cash_datasets)}")
        for ds in cash_datasets:
            try:
                print(f"[download] {ds} …")
                _df = fetch_bulk_dataset(ds, market, variant, api_key)
                print(f"[download] {ds} ok: rows={len(_df)}")
                df_cash_parts.append(_df)
            except Exception as e:
                print(f"[ingest_simfin_api] API fetch failed for {ds}: {e}")
        if df_cash_parts:
            pre_rows = sum(len(x) for x in df_cash_parts)
            df_cash = pd.concat(df_cash_parts, ignore_index=True)
            if {'Ticker', 'Report Date'}.issubset(df_cash.columns):
                df_cash = df_cash.drop_duplicates(subset=['Ticker', 'Report Date'])
            print(f"[merge] CF concat_rows={pre_rows} dedup_rows={len(df_cash)} key=['Ticker','Report Date']")
            total_candidate_tickers.update(set(ensure_ticker_column(df_cash)['Ticker'].dropna().astype(str).str.upper().unique()))
            cf_inserted = load_df(conn, df_cash, 'CF', CASHFLOW_TAGS, inserted_tickers)
            print(f"[summary] CF inserted_rows={cf_inserted}")

        did_api = bool(df_income_parts or df_balance_parts or df_cash_parts)
    except Exception as e:
        print(f"[ingest_simfin_api] API mode failed with error: {e}")
        try:
            log_run('error', message=str(e), details={
                "phase": "download_or_merge",
            })
        except Exception:
            pass
        conn.close()
        sys.exit(1)

    # No fallback: If API ingestion didn't fetch anything, exit with error.
    if not did_api:
        conn.close()
        print("[ingest_simfin_api] Error: No datasets fetched via API. Aborting (no local CSV fallback).")
        try:
            log_run('error', message='No datasets fetched via API', details={"phase": "empty_fetch"})
        except Exception:
            pass
        sys.exit(1)

    try:
        # Compute run status classification
        total_unique = len(total_candidate_tickers)
        inserted_unique = len(inserted_tickers)
        error_unique = max(total_unique - inserted_unique, 0)
        error_rate = (error_unique / total_unique) if total_unique else 0.0
        if error_unique == 0:
            status = 'success'
        elif error_rate <= 0.01:
            status = 'warning'
        else:
            status = 'error'
        msg = f"tickers total={total_unique} inserted={inserted_unique} errors={error_unique} rate={error_rate:.4f}"
        log_run(status, message=msg, details={
            "tickers": {
                "total": total_unique,
                "inserted": inserted_unique,
                "errors": error_unique,
                "error_rate": round(error_rate, 6),
            }
        })
    except Exception:
        pass
    conn.close()
    print("[ingest_simfin_api] All done.")


if __name__ == "__main__":
    sys.exit(main())
