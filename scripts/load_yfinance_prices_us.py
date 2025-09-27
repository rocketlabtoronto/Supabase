#!/usr/bin/env python
"""
Load current stock prices using Yahoo Finance (yfinance) for US tickers.
Simple approach - process one ticker at a time, similar to load_yfinance_ca.py
"""

import os
import sys
import math
import time
import yfinance as yf
import psycopg2
from datetime import datetime, date
from dotenv import load_dotenv
load_dotenv()

def safe(v):
    """Safely convert value to float, returning None for NaN/None values"""
    if v is None or (isinstance(v, float) and math.isnan(v)): 
        return None
    return float(v)

def get_us_tickers_from_db(conn, resume_from=None):
    """Get US tickers from financials table, optionally resuming from a specific ticker"""
    cursor = conn.cursor()
    
    if resume_from:
        # Get tickers starting from resume_from ticker (alphabetically)
        cursor.execute("""
            SELECT DISTINCT ticker 
            FROM financials 
            WHERE exchange = 'US' AND ticker > %s
            ORDER BY ticker
        """, (resume_from,))
        print(f"[resume] Resuming from ticker: {resume_from}")
    else:
        # Get all US tickers
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
    """Get the last ticker that was processed today for US stocks"""
    cursor = conn.cursor()
    
    # Get today's date (market day)
    today = date.today()
    
    # Find the last ticker processed today, ordered alphabetically
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
    # Get US tickers from database
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT", 5432),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
    )
    conn.autocommit = True
    cur = conn.cursor()
    
    # Clear existing stock prices if requested
    if os.getenv("CLEAR_STOCK_PRICES", "false").lower() == "true":
        print("[cleanup] Clearing existing stock price data...")
        cur.execute("DELETE FROM stock_prices WHERE exchange = 'US'")
        deleted_count = cur.rowcount
        print(f"[cleanup] Deleted {deleted_count} existing US records")
    
    # Check if we should resume from where we left off
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
            print("[load_yfinance_prices_us] No US tickers found in financials table.")
            return 1
    
    total = len(tickers)
    successful = 0
    last_pct = -1
    request_delay = float(os.getenv("YFINANCE_DELAY", "1.0"))  # Default 1 second delay
    
    print(f"[load_yfinance_prices_us] Processing {total} US tickers")
    print(f"[load_yfinance_prices_us] Rate limiting: {request_delay} second delay between requests")
    
    for i, ticker in enumerate(tickers):
        try:
            # Add delay between requests (except for first request)
            if i > 0:
                time.sleep(request_delay)
            
            # US stocks don't need suffix, use ticker as-is
            yf_symbol = ticker
            
            print(f"[fetch] {ticker}...")
            yf_ticker = yf.Ticker(yf_symbol)
            
            # Get current price data
            info = yf_ticker.info
            hist = yf_ticker.history(period="2d")  # Get last 2 days to ensure we have data
            
            if hist.empty:
                print(f"[skip] {ticker}: No price data available")
                continue
            
            # Get the most recent day's data
            latest_data = hist.iloc[-1]
            latest_date = hist.index[-1].date()
            
            # Extract price information
            current_price = safe(latest_data['Close'])
            open_price = safe(latest_data['Open'])
            high_price = safe(latest_data['High'])
            low_price = safe(latest_data['Low'])
            volume = safe(latest_data['Volume'])
            
            # Calculate previous close and change
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
            
            # Insert into database
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
        
        # Show progress
        pct = math.floor((i + 1) / total * 100)
        if pct != last_pct:
            print(f"[progress] {pct}% complete ({i + 1}/{total})")
            last_pct = pct
    
    conn.commit()
    cur.close()
    conn.close()
    
    print(f"[load_yfinance_prices_us] Complete: {successful}/{total} prices loaded")
    return 0

if __name__ == "__main__":
    sys.exit(main())
