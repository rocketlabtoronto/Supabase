"""
Ingest SimFin financials for Canadian tickers by downloading datasets via the
SimFin Bulk API into in-memory pandas DataFrames (no disk writes), then
inserting rows into Postgres. Annual statements only. No local CSV fallback.

Behavior:
- Downloads income, balance, and cash flow datasets, preferring banks then
  insurance then general variants.
- Filters rows to only include tickers from CA_TICKERS (env or data/ca_tickers.txt)
- Batches inserts with execute_values for speed.
- Logs a single run-level row into ingest_logs with SUCCESS/WARNING/ERROR based
  on error rate (≤1% → warning; >1% → error).

Env vars:
- SIMFIN_API_KEY        (required) authorization for SimFin API
- SIMFIN_MARKET         (optional) SimFin market code (default: 'world').
                         Set this to the correct SimFin market that contains
                         your Canadian tickers. Common values include 'us' and
                         'world'.
- SIMFIN_INSERT_BATCH   (optional) batch rows per insert (default: 5000)
- CA_TICKERS            (comma-separated) or 'FILE' to read data/ca_tickers.txt

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
INCOME_TAGS = {
    "SharesOutstanding": ["Shares (Basic)", "Shares (Diluted)"],
    "Revenue": ["Revenue", "Total Revenue", "Sales"],
    "GrossProfit": ["Gross Profit"],
    "OperatingProfit": ["Operating Income (Loss)"],
    "NetIncome": ["Net Income", "Net Income (Common)"],
}

BALANCE_TAGS = {
    "Equity": ["Total Equity"],
    "Liabilities": ["Total Liabilities"],
    "CashAndEquivalents": ["Cash, Cash Equivalents & Short Term Investments"],
    "Assets": ["Total Assets"],
}

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
        try:
            return float(str(val).replace(',', ''))
        except Exception:
            return None


def fetch_bulk_dataset(dataset: str, market: str, variant: str, api_key: str, timeout: int = 90) -> pd.DataFrame:
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


def get_ca_tickers() -> list[str]:
    env = os.getenv("CA_TICKERS", "").strip()
    if env.upper() == "FILE":
        path = os.path.join("data", "ca_tickers.txt")
        if not os.path.exists(path):
            print("[ingest_simfin_financials_ca] data/ca_tickers.txt not found.")
            return []
        return [l.strip().upper().replace('.TO', '') for l in open(path) if l.strip()]
    return [t.strip().upper().replace('.TO', '') for t in env.split(',') if t.strip()]


def load_df(conn, df: pd.DataFrame, stmt_type: str, tag_map: dict[str, list[str]], exchange: str, allow_tickers: set[str], inserted_tickers: set | None = None):
    df = ensure_ticker_column(df)
    if 'Ticker' not in df.columns:
        print(f"[warn] No 'Ticker' column present. Skipping {stmt_type}.")
        return 0

    # Filter to CA tickers of interest
    df['Ticker'] = df['Ticker'].astype(str).str.upper()
    if allow_tickers:
        df = df[df['Ticker'].isin(allow_tickers)]

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
            continue

        try:
            fy_end = pd.to_datetime(row.get('Report Date')).date()
        except Exception:
            continue

        currency = row.get('Currency', 'CAD')
        tkr = str(ticker).upper()

        for tag, cols in tag_map.items():
            val = pick_first(row, cols)
            if val is None:
                continue
            buffer.append((tkr, exchange, fy_end, stmt_type, tag, val, currency, "SimFin"))
            inserted_count += 1
            if inserted_tickers is not None:
                inserted_tickers.add(tkr)
            if len(buffer) >= batch_size:
                flush_buffer()

        pct = math.floor((i + 1) / total * 100) if total else 100
        if pct != last_pct:
            print(f"[{stmt_type}] {pct}% ({i + 1}/{total})")
            last_pct = pct

    flush_buffer()
    cur.close()
    print(f"[ingest_simfin_financials_ca] Done {stmt_type}. inserted_rows={inserted_count}")
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
    if not api_key:
        print("[ingest_simfin_financials_ca] Missing SIMFIN_API_KEY")
        sys.exit(1)

    # Default to 'world' unless you know a specific market that contains CA
    market = get_env('SIMFIN_MARKET', 'world')
    variant = 'annual'  # Annual-only per business rule
    exchange_label = 'CA'  # Label stored in the 'exchange' column in financials

    ca_tickers = set(get_ca_tickers())
    if not ca_tickers:
        print("[ingest_simfin_financials_ca] No CA tickers provided. Nothing to do.")
        # Log a no-op
        try:
            cur = conn.cursor()
            now = time.time()
            cur.execute(
                """
                insert into ingest_logs (script, status, message, details, started_at, ended_at, duration_ms)
                values (%s,%s,%s,%s, to_timestamp(%s), to_timestamp(%s), %s)
                """,
                (
                    os.path.basename(__file__),
                    'success',
                    'No CA tickers supplied',
                    json.dumps({"tickers": {"total": 0, "errors": 0, "error_rate": 0.0}}),
                    now, now, 0,
                ),
            )
            conn.commit(); cur.close(); conn.close()
        except Exception:
            pass
        return

    print(f"[config] market={market} variant={variant} batch={get_env_int('SIMFIN_INSERT_BATCH', 5000)} tickers={len(ca_tickers)}")

    # Logging context
    script_name = os.path.basename(__file__)
    start_ts = time.time()
    log_details = {
        "market": market,
        "variant": variant,
        "batch": get_env_int('SIMFIN_INSERT_BATCH', 5000),
        "tickers_filter": len(ca_tickers),
    }

    def log_run(status: str, message: str = None, details: dict | None = None):
        ended = time.time()
        duration_ms = int((ended - start_ts) * 1000)
        d = details or {}
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

    did_api = False
    inserted_tickers: set[str] = set()
    total_candidate_tickers: set[str] = set()

    try:
        # Income
        df_income_parts = []
        for ds in ['income-banks', 'income-insurance', 'income']:
            try:
                print(f"[download] {ds} …")
                _df = fetch_bulk_dataset(ds, market, variant, api_key)
                print(f"[download] {ds} ok: rows={len(_df)}")
                df_income_parts.append(_df)
            except Exception as e:
                print(f"[ingest_simfin_financials_ca] API fetch failed for {ds}: {e}")
        if df_income_parts:
            pre_rows = sum(len(x) for x in df_income_parts)
            df_income = pd.concat(df_income_parts, ignore_index=True)
            df_income = ensure_ticker_column(df_income)
            if {'Ticker', 'Report Date'}.issubset(df_income.columns):
                df_income = df_income.drop_duplicates(subset=['Ticker', 'Report Date'])
            # Track candidates (filtered to CA tickers only)
            df_income['Ticker'] = df_income['Ticker'].astype(str).str.upper()
            total_candidate_tickers.update(set(df_income[df_income['Ticker'].isin(ca_tickers)]['Ticker'].unique()))
            is_inserted = load_df(conn, df_income, 'IS', INCOME_TAGS, exchange_label, ca_tickers, inserted_tickers)
            print(f"[summary] IS inserted_rows={is_inserted}")

        # Balance
        df_balance_parts = []
        for ds in ['balance-banks', 'balance-insurance', 'balance']:
            try:
                print(f"[download] {ds} …")
                _df = fetch_bulk_dataset(ds, market, variant, api_key)
                print(f"[download] {ds} ok: rows={len(_df)}")
                df_balance_parts.append(_df)
            except Exception as e:
                print(f"[ingest_simfin_financials_ca] API fetch failed for {ds}: {e}")
        if df_balance_parts:
            pre_rows = sum(len(x) for x in df_balance_parts)
            df_balance = pd.concat(df_balance_parts, ignore_index=True)
            df_balance = ensure_ticker_column(df_balance)
            if {'Ticker', 'Report Date'}.issubset(df_balance.columns):
                df_balance = df_balance.drop_duplicates(subset=['Ticker', 'Report Date'])
            df_balance['Ticker'] = df_balance['Ticker'].astype(str).str.upper()
            total_candidate_tickers.update(set(df_balance[df_balance['Ticker'].isin(ca_tickers)]['Ticker'].unique()))
            bs_inserted = load_df(conn, df_balance, 'BS', BALANCE_TAGS, exchange_label, ca_tickers, inserted_tickers)
            print(f"[summary] BS inserted_rows={bs_inserted}")

        # Cash Flow
        df_cash_parts = []
        for ds in ['cashflow-banks', 'cashflow-insurance', 'cashflow']:
            try:
                print(f"[download] {ds} …")
                _df = fetch_bulk_dataset(ds, market, variant, api_key)
                print(f"[download] {ds} ok: rows={len(_df)}")
                df_cash_parts.append(_df)
            except Exception as e:
                print(f"[ingest_simfin_financials_ca] API fetch failed for {ds}: {e}")
        if df_cash_parts:
            pre_rows = sum(len(x) for x in df_cash_parts)
            df_cash = pd.concat(df_cash_parts, ignore_index=True)
            df_cash = ensure_ticker_column(df_cash)
            if {'Ticker', 'Report Date'}.issubset(df_cash.columns):
                df_cash = df_cash.drop_duplicates(subset=['Ticker', 'Report Date'])
            df_cash['Ticker'] = df_cash['Ticker'].astype(str).str.upper()
            total_candidate_tickers.update(set(df_cash[df_cash['Ticker'].isin(ca_tickers)]['Ticker'].unique()))
            cf_inserted = load_df(conn, df_cash, 'CF', CASHFLOW_TAGS, exchange_label, ca_tickers, inserted_tickers)
            print(f"[summary] CF inserted_rows={cf_inserted}")

        did_api = bool(df_income_parts or df_balance_parts or df_cash_parts)
    except Exception as e:
        print(f"[ingest_simfin_financials_ca] API mode failed with error: {e}")
        try:
            log_run('failure', message=str(e), details={
                "phase": "download_or_merge",
            })
        except Exception:
            pass

    if not did_api:
        conn.close()
        print("[ingest_simfin_financials_ca] Error: No datasets fetched via API. Aborting.")
        try:
            log_run('failure', message='No datasets fetched via API', details={"phase": "empty_fetch"})
        except Exception:
            pass
        sys.exit(1)

    try:
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
    print("[ingest_simfin_financials_ca] All done.")


if __name__ == "__main__":
    sys.exit(main())
