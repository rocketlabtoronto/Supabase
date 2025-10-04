import os
import sys
import math
import time
import pandas as pd
import yfinance as yf
import psycopg2
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from psycopg2 import OperationalError, InterfaceError
from psycopg2.extras import execute_values
from datetime import datetime
from dotenv import load_dotenv
from utils.logger import get_logger

load_dotenv(override=True)
log = get_logger("ingest_yfinance_financials_ca")


# ---------------- TMX issuers insertion (requested integration) -----------------

def _parse_float(v):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    try:
        return float(v)
    except Exception:
        try:
            return float(str(v).replace(',', ''))
        except Exception:
            return None


def _ensure_tmx_table(cur):
    cur.execute(
        """
        create table if not exists tmx_issuers (
          id serial primary key,
          symbol text not null,
          root_ticker text,
          co_id text,
          exchange text,
          name text,
          market_cap numeric,
          os_shares numeric,
          source_sheet text,
          inserted_at timestamp default now()
        );
        """
    )
    cur.execute(
        """
        create unique index if not exists idx_tmx_issuers_symbol_unique
        on tmx_issuers(symbol);
        """
    )


def insert_tmx_issuers_from_csv():
    """Read data/tsx_tsxv_all_symbols.csv (official TMX API list) and batch insert into tmx_issuers."""
    repo_root = os.path.dirname(os.path.dirname(__file__))
    csv_path = os.path.join(repo_root, 'data', 'tsx_tsxv_all_symbols.csv')
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Official TMX symbol list not found: {csv_path}. "
                                f"Generate it with: python scripts/download_tsx_symbols_from_api.py")

    df = pd.read_csv(csv_path, encoding='utf-8-sig')
    
    # Expected columns from official API export: symbol, exchange, name, parent_symbol
    required = ['symbol', 'exchange', 'name']
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise RuntimeError(f"Required columns missing from {csv_path}: {missing}")

    rows = []
    for _, row in df.iterrows():
        symbol_val = str(row.get('symbol')).strip() if pd.notna(row.get('symbol')) else None
        exchange_val = str(row.get('exchange')).strip().upper() if pd.notna(row.get('exchange')) else None
        name_val = str(row.get('name')).strip() if pd.notna(row.get('name')) else None
        parent_symbol = str(row.get('parent_symbol')).strip() if pd.notna(row.get('parent_symbol')) else symbol_val
        
        if not symbol_val or not exchange_val:
            continue
        
        # symbol already has Yahoo suffix (.TO/.V), use parent_symbol as root_ticker
        root_ticker = parent_symbol if parent_symbol else symbol_val.split('.')[0]
        
        # Extract co_id, market_cap, os_shares if they exist in the CSV
        co_id = str(row.get('co_id')).strip() if 'co_id' in df.columns and pd.notna(row.get('co_id')) else None
        market_cap = _parse_float(row.get('market_cap')) if 'market_cap' in df.columns else None
        os_shares = _parse_float(row.get('os_shares')) if 'os_shares' in df.columns else None
        source_sheet = str(row.get('source_sheet')).strip() if 'source_sheet' in df.columns and pd.notna(row.get('source_sheet')) else None
        
        rows.append((symbol_val, root_ticker, co_id, exchange_val, name_val, market_cap, os_shares, source_sheet))

    if not rows:
        log.info('[tmx_issuers] No rows to insert.')
        return

    conn = psycopg2.connect(
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT', 5432),
        dbname=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
    )
    conn.autocommit = False
    cur = conn.cursor()
    _ensure_tmx_table(cur)

    if os.getenv('CLEAR_TMX_ISSUERS', 'false').lower() == 'true':
        log.info('[tmx_issuers] Truncating table before insert…')
        cur.execute('TRUNCATE tmx_issuers')

    batch_size = int(os.getenv('ISSUERS_INSERT_BATCH', '1000'))
    total = len(rows)
    log.info("[tmx_issuers] Inserting %s rows (batch=%s)…", total, batch_size)

    def flush(batch):
        if not batch:
            return
        sql = (
            'INSERT INTO tmx_issuers (symbol, root_ticker, co_id, exchange, name, market_cap, os_shares, source_sheet) '
            'VALUES %s ON CONFLICT (symbol) DO NOTHING'
        )
        execute_values(cur, sql, batch)

    buf = []
    last_pct = -1
    for i, r in enumerate(rows, 1):
        buf.append(r)
        if len(buf) >= batch_size:
            flush(buf)
            buf.clear()
        pct = math.floor(i / total * 100)
        if pct != last_pct:
            log.info("[tmx_issuers] %s%% (%s/%s)", pct, i, total)
            last_pct = pct
    flush(buf)
    conn.commit()
    cur.close(); conn.close()
    log.info('[tmx_issuers] Insert complete.')


# ---------------- Optional legacy yfinance part (off by default) -----------------

INCOME_FIELDS = [
    'TotalRevenue', 'CostOfRevenue', 'GrossProfit', 'OperatingExpense', 'OperatingIncome',
    'NetIncome', 'EBIT', 'EBITDA', 'InterestExpense', 'IncomeTaxExpense'
]
BALANCE_FIELDS = [
    'CashAndCashEquivalents', 'ShortTermInvestments', 'TotalCurrentAssets', 'TotalAssets',
    'TotalCurrentLiabilities', 'TotalLiabilities', 'TotalEquity', 'RetainedEarnings'
]


def _safe_float(v):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    try:
        return float(v)
    except Exception:
        try:
            return float(str(v).replace(',', ''))
        except Exception:
            return None


def _extract_stmt_rows(df: pd.DataFrame, stmt_type: str):
    """From a yfinance statement DF, take the most recent period and return list[(fy_end_date, stmt_type, tag, value)]."""
    rows = []
    if df is None or df.empty:
        return rows
    # Columns are period end dates; pick the first (most recent) column
    col = df.columns[0]
    try:
        fy_end_date = pd.to_datetime(col).date()
    except Exception:
        fy_end_date = None
    series = df.iloc[:, 0]
    for tag, val in series.items():
        rows.append((fy_end_date, stmt_type, str(tag), _safe_float(val)))
    return rows


def run_yfinance_ingest():
    # Connect to DB and get CA symbols from tmx_issuers
    conn = psycopg2.connect(
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT', 5432),
        dbname=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=5,
    )
    conn.autocommit = False
    cur = conn.cursor()

    # Optional clean
    if os.getenv('CLEAR_FINANCIALS_CA', 'false').lower() == 'true':
        log.info('[yfinance] Clearing existing CA financials…')
        cur.execute("DELETE FROM financials WHERE exchange = 'CA'")
        conn.commit()

    cur.execute("SELECT symbol FROM tmx_issuers ORDER BY symbol")
    tickers = [r[0] for r in cur.fetchall()]
    # Optional cap for testing
    try:
        max_n = int(os.getenv('YFIN_MAX_TICKERS', '0'))
    except Exception:
        max_n = 0
    if max_n and max_n > 0:
        tickers = tickers[:max_n]
    if not tickers:
        log.warning('[yfinance] No symbols found in tmx_issuers; skipping')
        cur.close(); conn.close()
        return 2

    def _get_env_int(name: str, default: int) -> int:
        try:
            v = int(str(os.getenv(name, str(default))).strip())
            return v if v > 0 else default
        except Exception:
            return default

    batch_size = _get_env_int('YFIN_FIN_BATCH', 500)
    flush_secs = _get_env_int('YFIN_FLUSH_SECS', 30)
    workers = _get_env_int('YFIN_FIN_WORKERS', 32)
    log.info("[yfinance] Ingesting financials for %s CA symbols (batch=%s, workers=%s)…", len(tickers), batch_size, workers)

    rows = []  # (ticker, exchange, fy_end_date, stmt_type, tag, value, unit, source)
    last_flush = time.time()

    def _reconnect():
        nonlocal conn, cur
        try:
            if cur:
                try:
                    cur.close()
                except Exception:
                    pass
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass
        finally:
            conn = psycopg2.connect(
                host=os.getenv('DB_HOST'),
                port=os.getenv('DB_PORT', 5432),
                dbname=os.getenv('DB_NAME'),
                user=os.getenv('DB_USER'),
                password=os.getenv('DB_PASSWORD'),
                keepalives=1,
                keepalives_idle=30,
                keepalives_interval=10,
                keepalives_count=5,
            )
            conn.autocommit = False
            cur = conn.cursor()

    def flush():
        nonlocal rows, last_flush
        if not rows:
            return
        sql = (
            "INSERT INTO financials (ticker, exchange, fy_end_date, stmt_type, tag, value, unit, source) VALUES %s"
        )
        try:
            execute_values(cur, sql, rows)
            conn.commit()
        except (OperationalError, InterfaceError) as e:
            log.warning("[yfinance] flush: DB connection issue; attempting reconnect and retry once…", exc_info=e)
            _reconnect()
            execute_values(cur, sql, rows)
            conn.commit()
        rows = []
        last_flush = time.time()

    def _first_non_empty_df(*dfs):
        for df in dfs:
            try:
                if df is not None and isinstance(df, pd.DataFrame) and not df.empty:
                    return df
            except Exception:
                continue
        return None

    def fetch_one(sym: str):
        try:
            yt = yf.Ticker(sym)
            # Try multiple APIs to maximize chance of data
            try:
                is_df = getattr(yt, 'income_stmt', None)
                if callable(getattr(yt, 'get_income_stmt', None)):
                    tmp = yt.get_income_stmt(freq='a')
                    is_df = tmp if _first_non_empty_df(tmp) is not None else is_df
            except Exception:
                is_df = getattr(yt, 'income_stmt', None)

            try:
                bs_df = getattr(yt, 'balance_sheet', None)
                if callable(getattr(yt, 'get_balance_sheet', None)):
                    tmp = yt.get_balance_sheet(freq='a')
                    bs_df = tmp if _first_non_empty_df(tmp) is not None else bs_df
            except Exception:
                bs_df = getattr(yt, 'balance_sheet', None)

            try:
                cf_df = getattr(yt, 'cashflow', None)
                if callable(getattr(yt, 'get_cashflow', None)):
                    tmp = yt.get_cashflow(freq='a')
                    cf_df = tmp if _first_non_empty_df(tmp) is not None else cf_df
            except Exception:
                cf_df = getattr(yt, 'cashflow', None)

            # As a last resort, old .financials can resemble income statement
            fin_df = getattr(yt, 'financials', None)
            if _first_non_empty_df(is_df) is None and _first_non_empty_df(fin_df) is not None:
                is_df = fin_df

            is_rows = _extract_stmt_rows(is_df, 'IS')
            bs_rows = _extract_stmt_rows(bs_df, 'BS')
            cf_rows = _extract_stmt_rows(cf_df, 'CF')
            out = []
            for fy, st, tag, val in (is_rows + bs_rows + cf_rows):
                out.append((sym, 'CA', fy, st, tag, val, None, 'yfinance'))
            return out
        except Exception as e:
            # Keep errors lightweight; yfinance can be noisy
            return []

    processed = 0
    next_progress = 100
    with ThreadPoolExecutor(max_workers=workers) as exe:
        futures = [exe.submit(fetch_one, t) for t in tickers]
        for fut in as_completed(futures):
            processed += 1
            res = fut.result() or []
            if res:
                rows.extend(res)
            # Flush on size or time thresholds to avoid idle connection timeouts
            if len(rows) >= batch_size or (time.time() - last_flush) >= flush_secs:
                flush()
            if processed >= next_progress:
                log.info("[yfinance] Progress: %s/%s", processed, len(tickers))
                next_progress += 100

    flush()
    cur.close(); conn.close()
    log.info('[yfinance] Financials ingest complete.')
    return 0

def main():
    t0 = time.time()
    insert_tmx_issuers_from_csv()
    rc = run_yfinance_ingest()
    log.info("[ingest_yfinance_financials_api_to_postgres_ca] Done in %s ms", int((time.time()-t0)*1000))
    if isinstance(rc, int) and rc != 0:
        sys.exit(rc)


if __name__ == '__main__':
    main()