import os, sys, math
import psycopg2
import pandas as pd
from dotenv import load_dotenv
load_dotenv()

INCOME_TAGS = {
    "SharesOutstanding": "Shares (Basic)",
    "Revenue": "Revenue",
    "GrossProfit": "Gross Profit",
    "OperatingProfit": "Operating Income (Loss)",
    "NetIncome": "Net Income"
}

BALANCE_TAGS = {
    "Equity": "Total Equity",
    "Liabilities": "Total Liabilities",
    "CashAndEquivalents": "Cash, Cash Equivalents & Short Term Investments",
    "Assets": "Total Assets"
}

def safe(val):
    if pd.isna(val): return None
    try: return float(val)
    except ValueError: return None

def load_csv(conn, path, stmt_type, tag_map):
    if not os.path.exists(path):
        print(f"[ingest_simfin_csv] File missing → {path}")
        return
    print(f"[ingest_simfin_csv] Loading {os.path.basename(path)} …")

    df = pd.read_csv(path, sep=";")
    total = len(df)
    cur = conn.cursor()
    conn.autocommit = True
    last_pct = -1
    for i, (_, row) in enumerate(df.iterrows()):
        ticker = row.get("Ticker")
        if pd.isna(ticker):
            print(f"[warn] Row {i}: missing ticker. Skipping.")
            continue

        try:
            fy_end = pd.to_datetime(row["Report Date"]).date()
        except Exception as e:
            print(f"[warn] Row {i}: invalid Report Date: {e}. Skipping.")
            continue

        currency = row.get("Currency", "USD")

        for tag, col in tag_map.items():
            val = safe(row.get(col))
            if val is None:
                print(f"[debug] {ticker} {tag} missing or NaN → skipping")
                continue

            print(f"[insert] {ticker} – {tag} = {val}")
            cur.execute("""
                INSERT INTO financials
                (ticker, exchange, fy_end_date, stmt_type, tag, value, unit, source)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                RETURNING 1;
            """, (ticker, "US", fy_end, stmt_type, tag, val, currency, "SimFin"))

        pct = math.floor((i + 1) / total * 100)
        if pct != last_pct:
            print(f"[{stmt_type}] {pct}%")
            last_pct = pct

    cur.close()
    print(f"[ingest_simfin_csv] Done {stmt_type}.")

def main():
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT", 5432),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
    )
    data_dir = os.getenv("SIMFIN_DATA_DIR", "data")

    load_csv(conn, os.path.join(data_dir, "Income_Statement_Annual.csv"), "IS", INCOME_TAGS)
    load_csv(conn, os.path.join(data_dir, "Balance_Sheet_Annual.csv"), "BS", BALANCE_TAGS)

    conn.close()
    print("[ingest_simfin_csv] All done.")

if __name__ == "__main__":
    sys.exit(main())
