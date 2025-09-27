#!/usr/bin/env python
"""
Load current stock prices using Yahoo Finance (yfinance) for US tickers.
"""

import os
import sys
import math
import time
import yfinance as yf
import psycopg2
from datetime import date
from dotenv import load_dotenv
load_dotenv()

def safe(v):
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return None
    return float(v)

def get_us_tickers_from_db(conn, resume_from=None):
    cursor = conn.cursor()
    if resume_from:
        cursor.execute("""
            SELECT DISTINCT ticker 
            FROM financials 
            WHERE exchange = 'US' AND ticker > %s
            ORDER BY ticker
        """, (resume_from,))
        print(f"[resume] Resuming from ticker: {resume_from}")
    else:
        cursor.execute("""
            SELECT DISTINCT ticker 
            FROM financials 
            WHERE exchange = 'US' 
            ORDER BY ticker
        """)
    tickers = [row[0] for row in cursor.fetchall()]
    cursor.close()
    return tickers

def get_last_processed_ticker(conn):
    cursor = conn.cursor()
    today = date.today()
    cursor.execute("""
        SELECT symbol 
        FROM stock_prices 
        WHERE exchange = 'US' 
        AND latest_day = %s 
        ORDER BY symbol DESC 
        LIMIT 1
    """, (today,))
    result = cursor.fetchone()
    cursor.close()
    return result[0] if result else None

def main():
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

    last_ticker = get_last_processed_ticker(conn)
    if last_ticker:
        print(f"[resume] Last processed ticker today: {last_ticker}")
        tickers = get_us_tickers_from_db(conn, resume_from=last_ticker)
        print(f"[resume] Resuming processing from after {last_ticker}")
    else:
        print("[start] No previous data found for today, starting from beginning")
        tickers = get_us_tickers_from_db(conn)

    if not tickers:
        if last_ticker:
            print("[complete] All US tickers have been processed today!")
            return 0
        else:
            print("[ingest_yahoo_prices_us] No US tickers found in financials table.")
            return 1

    total = len(tickers)
    successful = 0
    last_pct = -1
    request_delay = float(os.getenv("YFINANCE_DELAY", "1.0"))

    print(f"[ingest_yahoo_prices_us] Processing {total} US tickers")
    print(f"[ingest_yahoo_prices_us] Rate limiting: {request_delay} second delay between requests")

    for i, ticker in enumerate(tickers):
        try:
            if i > 0:
                time.sleep(request_delay)

            yf_symbol = ticker
            print(f"[fetch] {ticker}...")
            yf_ticker = yf.Ticker(yf_symbol)
            hist = yf_ticker.history(period="2d")

            if hist.empty:
                print(f"[skip] {ticker}: No price data available")
                continue

            latest_data = hist.iloc[-1]
            latest_date = hist.index[-1].date()

            current_price = safe(latest_data['Close'])
            open_price = safe(latest_data['Open'])
            high_price = safe(latest_data['High'])
            low_price = safe(latest_data['Low'])
            volume = safe(latest_data['Volume'])

            previous_close = None
            change = None
            change_percent = None
            if len(hist) > 1:
                previous_close = safe(hist.iloc[-2]['Close'])
                if previous_close and current_price:
                    change = current_price - previous_close
                    change_percent = f"{(change / previous_close * 100):.2f}%"

            if not current_price:
                print(f"[skip] {ticker}: No valid price data")
                continue

            print(f"[price] {ticker}: ${current_price:.2f} USD ({change_percent or 'N/A'})")

            cur.execute("""
                INSERT INTO stock_prices 
                (symbol, exchange, open, high, low, price, volume, latest_day, previous_close, change, change_percent)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (symbol, latest_day, exchange) DO UPDATE SET
                    open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    price = EXCLUDED.price,
                    volume = EXCLUDED.volume,
                    previous_close = EXCLUDED.previous_close,
                    change = EXCLUDED.change,
                    change_percent = EXCLUDED.change_percent,
                    inserted_at = now()
            """, (
                ticker,
                'US',
                open_price,
                high_price,
                low_price,
                current_price,
                int(volume) if volume else None,
                latest_date,
                previous_close,
                change,
                change_percent
            ))

            successful += 1

        except Exception as e:
            print(f"[error] Failed to process {ticker}: {e}")
            continue

        pct = math.floor((i + 1) / total * 100)
        if pct != last_pct:
            print(f"[progress] {pct}% complete ({i + 1}/{total})")
            last_pct = pct

    conn.commit()
    cur.close()
    conn.close()

    print(f"[ingest_yahoo_prices_us] Complete: {successful}/{total} prices loaded")
    return 0

if __name__ == "__main__":
    sys.exit(main())
