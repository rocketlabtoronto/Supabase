#!/usr/bin/env python
"""
Load current stock prices using Yahoo Finance (yfinance) for Canadian tickers.
Simple approach - process one ticker at a time, similar to load_yfinance_ca.py
"""

import os
import sys
import math
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

def get_ca_tickers():
    """Get Canadian tickers from environment or file"""
    env = os.getenv("CA_TICKERS", "").strip()
    if env.upper() == "FILE":
        path = os.path.join("data", "ca_tickers.txt")
        if not os.path.exists(path):
            print("[load_yfinance_prices] data/ca_tickers.txt not found.")
            return []
        return [l.strip() for l in open(path) if l.strip()]
    return [t.strip() for t in env.split(",") if t.strip()]

def get_ca_tickers_from_db(conn):
    """Get Canadian tickers from financials table"""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT DISTINCT ticker 
        FROM financials 
        WHERE exchange = 'CA' 
        ORDER BY ticker
    """)
    tickers = [row[0] for row in cursor.fetchall()]
    cursor.close()
    return tickers

def main():
    # Get Canadian tickers from database
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
        cur.execute("DELETE FROM stock_prices WHERE exchange = 'CA'")
        deleted_count = cur.rowcount
        print(f"[cleanup] Deleted {deleted_count} existing Canadian records")
    
    tickers = get_ca_tickers_from_db(conn)
    if not tickers:
        print("[load_yfinance_prices] No Canadian tickers found in financials table.")
        return 1
    
    total = len(tickers)
    successful = 0
    last_pct = -1
    
    print(f"[load_yfinance_prices] Processing {total} Canadian tickers")
    
    for i, ticker in enumerate(tickers):
        try:
            # Add .TO suffix for Yahoo Finance if not already present
            yf_symbol = ticker if ticker.endswith(('.TO', '.V')) else f"{ticker}.TO"
            
            print(f"[fetch] {ticker} ({yf_symbol})...")
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
            
            print(f"[price] {ticker}: ${current_price:.2f} CAD ({change_percent or 'N/A'})")
            
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
                'CA',
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
    
    print(f"[load_yfinance_prices] Complete: {successful}/{total} prices loaded")
    return 0

if __name__ == "__main__":
    sys.exit(main())
