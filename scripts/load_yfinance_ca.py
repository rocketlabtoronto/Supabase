import os, sys, math
import psycopg2
import numpy as np
import yfinance as yf
import pandas as pd
from dotenv import load_dotenv
load_dotenv()

FIELDS = {
    "IS": {
        "Revenue":           ("Total Revenue", "IS"),
        "GrossProfit":       ("Gross Profit", "IS"),
        "OperatingProfit":   ("Operating Income", "IS"),
        "NetIncome":         ("Net Income", "IS"),
        "FreeCashFlow":      ("Free Cash Flow", "IS"),
    },
    "BS": {
        "Equity":            ("Stockholders Equity", "BS"),
        "Liabilities":       ("Total Liabilities Net Minority Interest", "BS"),
        "CashAndEquivalents":("Cash And Cash Equivalents", "BS"),
        "Assets":            ("Total Assets", "BS"),
    },
    "GENERAL": {
        "SharesOutstanding": ("sharesOutstanding", "INFO")
    }    
}

def safe(v):
    if v is None or (isinstance(v, (float, np.floating)) and math.isnan(v)): return None
    return float(v)

def get_ca_tickers():
    env = os.getenv("CA_TICKERS", "").strip()
    if env.upper() == "FILE":
        path = os.path.join("data", "ca_tickers.txt")
        if not os.path.exists(path):
            print("[load_yfinance_ca] data/ca_tickers.txt not found.")
            return []
        return [l.strip() for l in open(path) if l.strip()]
    return [t.strip() for t in env.split(",") if t.strip()]

def main():
    tickers = get_ca_tickers()
    if not tickers:
        print("[load_yfinance_ca] No tickers supplied.")
        return

    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT", 5432),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
    )
    conn.autocommit = True
    cur = conn.cursor()
    total = len(tickers)
    last_pct = -1

    for i, ticker in enumerate(tickers):
        yf_tkr = yf.Ticker(ticker)
        bs = yf_tkr.balance_sheet
        is_ = yf_tkr.financials
        cf = yf_tkr.cashflow
        info_fields = yf_tkr.info
        if bs.empty or is_.empty:
            print(f"[warn] {ticker} has no IS or BS data. Skipping.")
            continue

        fy_date = bs.columns[0].date()
        print(f"[load_yfinance_ca] Inserting {ticker} – FY {fy_date}")

        for tag, (yf_key, stmt) in FIELDS["IS"].items():
            raw_val = None
            if yf_key in is_.index:
                raw_val = is_.loc[yf_key, bs.columns[0]]
            elif tag == "FreeCashFlow" and not cf.empty and "Free Cash Flow" in cf.index:
                raw_val = cf.loc["Free Cash Flow", cf.columns[0]]
            elif tag == "SharesOutstanding" and "Basic" in is_.index:
                raw_val = is_.loc["Basic", bs.columns[0]]
            val = safe(raw_val)
            if val is None:
                print(f"[debug] {ticker} {tag} is None/NaN — skipping.")
                continue
            print(f"[insert] {ticker} {tag}: {val}")
            cur.execute("""
                INSERT INTO financials
                (ticker, exchange, fy_end_date, stmt_type, tag, value, unit, source)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING;
            """, (ticker.replace(".TO", ""), "CA", fy_date, stmt, tag, val, "CAD", "Yahoo"))

        for tag, (yf_key, stmt) in FIELDS["BS"].items():
            raw_val = None
            if yf_key in bs.index:
                raw_val = bs.loc[yf_key, bs.columns[0]]
            val = safe(raw_val)
            if val is None:
                print(f"[debug] {ticker} {tag} is None/NaN — skipping.")
                continue
            print(f"[insert] {ticker} {tag}: {val}")
            cur.execute("""
                INSERT INTO financials
                (ticker, exchange, fy_end_date, stmt_type, tag, value, unit, source)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING;
            """, (ticker.replace(".TO", ""), "CA", fy_date, stmt, tag, val, "CAD", "Yahoo"))
            
        for tag, (yf_key, stmt) in FIELDS["GENERAL"].items():
            raw_val = info_fields.get(yf_key)
            val = safe(raw_val)
            if val is None:
                print(f"[debug] {ticker} {tag} is None/NaN — skipping.")
                continue
            print(f"[insert] {ticker} {tag}: {val}")
            cur.execute("""
                INSERT INTO financials
                (ticker, exchange, fy_end_date, stmt_type, tag, value, unit, source)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING;
            """, (ticker.replace(".TO", ""), "CA", fy_date, stmt, tag, val, "CAD", "Yahoo"))
            
        pct = math.floor((i + 1) / total * 100)
        if pct != last_pct:
            print(f"[load_yfinance_ca] {pct}%")
            last_pct = pct

    conn.commit()
    cur.close()
    conn.close()
    print("[load_yfinance_ca] Done.")

if __name__ == "__main__":
    sys.exit(main())
