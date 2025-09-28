#!/usr/bin/env python
"""
Load latest stock prices for US tickers using SimFin Bulk API (shareprices).

Rules:
- SimFin only (no Yahoo fallback).
- No upserts. Insert only when the incoming record is newer than what's in DB.
- If SimFin provides time precision (DateTime/Timestamp), we use it for gating; otherwise gate by Date.

Env:
- SIMFIN_API_KEY (required)
- SIMFIN_MARKET (optional, default 'us')
- SIMFIN_PRICES_VARIANT (optional: 'latest' or 'daily'; default 'latest')
- PRICES_INSERT_BATCH (optional, default 1000)

DB connection uses: DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
"""

import os
import sys
import math
import time
import json
import io
import zipfile
import requests
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()


def safe(v):
    try:
        if v is None:
            return None
        if isinstance(v, float) and math.isnan(v):
            return None
        return float(v)
    except Exception:
        try:
            return float(str(v).replace(',', ''))
        except Exception:
            return None


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


def get_us_tickers_from_db(conn):
    cur = conn.cursor()
    cur.execute(
        """
        SELECT DISTINCT ticker
        FROM financials
        WHERE exchange = 'US'
        ORDER BY ticker
        """
    )
    tickers = [row[0] for row in cur.fetchall()]
    cur.close()
    return tickers


def table_has_column(conn, table: str, column: str) -> bool:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = %s AND column_name = %s
        """,
        (table, column),
    )
    exists = cur.fetchone() is not None
    cur.close()
    return exists


def get_existing_latest_map(conn, use_timestamp: bool):
    """Return a dict: symbol -> max(as_of) or max(latest_day) already stored for US."""
    cur = conn.cursor()
    if use_timestamp:
        cur.execute(
            """
            SELECT symbol, max(as_of)
            FROM stock_prices
            WHERE exchange = 'US'
            GROUP BY symbol
            """
        )
    else:
        cur.execute(
            """
            SELECT symbol, max(latest_day)
            FROM stock_prices
            WHERE exchange = 'US'
            GROUP BY symbol
            """
        )
    data = {row[0]: row[1] for row in cur.fetchall()}
    cur.close()
    return data


def fetch_bulk_dataset(dataset: str, market: str, variant: str, api_key: str, timeout: int = 90) -> pd.DataFrame:
    """Fetch a SimFin bulk dataset (zip with a semicolon-delimited CSV) into a pandas DataFrame."""
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


def pick_first(row: pd.Series, cols: list[str]):
    for c in cols:
        if c in row.index:
            v = row.get(c)
            if v is not None and not (isinstance(v, float) and math.isnan(v)):
                return v
    return None


def main():
    script_name = os.path.basename(__file__)
    start_ts = time.time()

    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT", 5432),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
    )
    conn.autocommit = True
    cur = conn.cursor()

    if os.getenv("CLEAR_STOCK_PRICES", "false").lower() == "true":
        print("[cleanup] Clearing existing stock price data...")
        cur.execute("DELETE FROM stock_prices WHERE exchange = 'US'")
        print(f"[cleanup] Deleted {cur.rowcount} existing US records")

    tickers = get_us_tickers_from_db(conn)
    if not tickers:
        print("[ingest_simfin_prices_us] No US tickers found in financials table.")
        # Log
        try:
            conn2 = psycopg2.connect(
                host=os.getenv("DB_HOST"), port=os.getenv("DB_PORT", 5432), dbname=os.getenv("DB_NAME"),
                user=os.getenv("DB_USER"), password=os.getenv("DB_PASSWORD")
            ); cur2 = conn2.cursor()
            now = time.time()
            cur2.execute(
                "insert into ingest_logs (script, status, message, details, started_at, ended_at, duration_ms) values (%s,%s,%s,%s,to_timestamp(%s),to_timestamp(%s),%s)",
                (script_name,'warning','No US tickers found in financials table', json.dumps({}), start_ts, now, int((now-start_ts)*1000))
            ); conn2.commit(); cur2.close(); conn2.close()
        except Exception:
            pass
        return 1

    # SimFin config
    api_key = get_env('SIMFIN_API_KEY')
    if not api_key:
        print("[ingest_simfin_prices_us] Missing SIMFIN_API_KEY")
        return 1
    market = get_env('SIMFIN_MARKET', 'us')
    variant = get_env('SIMFIN_PRICES_VARIANT', 'latest')  # 'latest' or 'daily'

    print(f"[ingest_simfin_prices_us] Fetching SimFin shareprices dataset: market={market} variant={variant}")
    try:
        df = fetch_bulk_dataset('shareprices', market, variant, api_key)
    except Exception as e:
        print(f"[error] Failed to download SimFin prices: {e}")
        # Log error
        try:
            conn2 = psycopg2.connect(
                host=os.getenv("DB_HOST"), port=os.getenv("DB_PORT", 5432), dbname=os.getenv("DB_NAME"),
                user=os.getenv("DB_USER"), password=os.getenv("DB_PASSWORD")
            ); cur2 = conn2.cursor()
            now = time.time()
            cur2.execute(
                "insert into ingest_logs (script, status, message, details, started_at, ended_at, duration_ms) values (%s,%s,%s,%s,to_timestamp(%s),to_timestamp(%s),%s)",
                (script_name,'error','Failed to download SimFin prices', json.dumps({"error": str(e)}), start_ts, now, int((now-start_ts)*1000))
            ); conn2.commit(); cur2.close(); conn2.close()
        except Exception:
            pass
        return 1

    # Normalize columns
    if 'Ticker' not in df.columns or 'Date' not in df.columns:
        print("[error] SimFin prices CSV missing required columns (Ticker/Date)")
        return 1

    df['Ticker'] = df['Ticker'].astype(str).str.upper()
    df['Date'] = pd.to_datetime(df['Date']).dt.date
    if 'DateTime' in df.columns:
        df['DateTime'] = pd.to_datetime(df['DateTime'], errors='coerce')
    elif 'Timestamp' in df.columns:
        df['DateTime'] = pd.to_datetime(df['Timestamp'], errors='coerce')
    else:
        df['DateTime'] = pd.NaT

    us_set = set(tickers)
    df = df[df['Ticker'].isin(us_set)]
    if df.empty:
        print("[ingest_simfin_prices_us] No matching US tickers present in SimFin shareprices dataset.")
        return 0

    latest_rows = df.sort_values(['Ticker', 'Date', 'DateTime']).groupby('Ticker', as_index=False).tail(1)

    prev_map = {}
    if variant.lower() == 'daily':
        prev_rows = df.sort_values(['Ticker', 'Date']).groupby('Ticker', as_index=False).tail(2)
        for tkr, g in prev_rows.groupby('Ticker'):
            if len(g) == 2:
                g_sorted = g.sort_values('Date')
                prev_close_val = pick_first(g_sorted.iloc[0], ['Close', 'Adj. Close'])
                prev_map[tkr] = safe(prev_close_val)

    has_as_of = table_has_column(conn, 'stock_prices', 'as_of')
    has_datetime_col = latest_rows['DateTime'].notna().any()
    existing_max = get_existing_latest_map(conn, has_as_of and has_datetime_col)

    total = len(latest_rows)
    successful = 0
    errors = 0
    last_pct = -1
    batch_size = get_env_int('PRICES_INSERT_BATCH', 1000)
    rows_buffer = []

    def flush_buffer():
        if not rows_buffer:
            return
        if has_as_of and has_datetime_col:
            sql = (
                "INSERT INTO stock_prices "
                "(symbol, exchange, open, high, low, price, volume, latest_day, as_of, previous_close, change, change_percent) VALUES %s"
            )
            execute_values(cur, sql, rows_buffer, template="(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)")
        else:
            sql = (
                "INSERT INTO stock_prices "
                "(symbol, exchange, open, high, low, price, volume, latest_day, previous_close, change, change_percent) VALUES %s"
            )
            execute_values(cur, sql, rows_buffer, template="(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)")
        rows_buffer.clear()

    print(f"[ingest_simfin_prices_us] Processing {total} US tickers from SimFin")

    for i, (_, row) in enumerate(latest_rows.iterrows()):
        try:
            tkr = row['Ticker']
            d = row['Date']
            as_of = row['DateTime'] if not pd.isna(row['DateTime']) else None

            last_seen = existing_max.get(tkr)
            if last_seen is not None:
                if has_as_of and has_datetime_col and as_of is not None:
                    if pd.to_datetime(as_of) <= pd.to_datetime(last_seen):
                        continue
                else:
                    if d <= last_seen:
                        continue

            open_price = safe(pick_first(row, ['Open']))
            high_price = safe(pick_first(row, ['High']))
            low_price = safe(pick_first(row, ['Low']))
            close_price = safe(pick_first(row, ['Close', 'Adj. Close']))
            volume = safe(pick_first(row, ['Volume']))

            previous_close = prev_map.get(tkr)
            change = None
            change_percent = None
            if previous_close is not None and close_price is not None and previous_close != 0:
                change = close_price - previous_close
                change_percent = f"{(change / previous_close * 100):.2f}%"

            if close_price is None:
                continue

            if has_as_of and has_datetime_col and as_of is not None:
                rows_buffer.append((
                    tkr,
                    'US',
                    open_price,
                    high_price,
                    low_price,
                    close_price,
                    int(volume) if volume is not None else None,
                    d,
                    as_of,
                    previous_close,
                    change,
                    change_percent,
                ))
            else:
                rows_buffer.append((
                    tkr,
                    'US',
                    open_price,
                    high_price,
                    low_price,
                    close_price,
                    int(volume) if volume is not None else None,
                    d,
                    previous_close,
                    change,
                    change_percent,
                ))

            if len(rows_buffer) >= batch_size:
                flush_buffer()

            successful += 1
        except Exception as e:
            print(f"[error] Failed to process {row.get('Ticker')}: {e}")
            errors += 1

        pct = math.floor((i + 1) / total * 100)
        if pct != last_pct:
            print(f"[progress] {pct}% complete ({i + 1}/{total})")
            last_pct = pct

    flush_buffer()
    conn.commit()
    cur.close()
    conn.close()

    print(f"[ingest_simfin_prices_us] Complete: {successful}/{total} prices loaded")
    error_rate = (errors / total) if total else 0.0
    status = 'success' if errors == 0 else ('warning' if error_rate <= 0.01 else 'error')
    message = f"symbols total={total} success={successful} errors={errors} rate={error_rate:.4f}"
    try:
        conn2 = psycopg2.connect(
            host=os.getenv("DB_HOST"), port=os.getenv("DB_PORT", 5432), dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"), password=os.getenv("DB_PASSWORD")
        ); cur2 = conn2.cursor()
        now = time.time()
        cur2.execute(
            "insert into ingest_logs (script, status, message, details, started_at, ended_at, duration_ms) values (%s,%s,%s,%s,to_timestamp(%s),to_timestamp(%s),%s)",
            (script_name, status, message, json.dumps({"symbols": {"total": total, "success": successful, "errors": errors, "error_rate": round(error_rate, 6)}}), start_ts, now, int((now-start_ts)*1000))
        ); conn2.commit(); cur2.close(); conn2.close()
    except Exception:
        pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
