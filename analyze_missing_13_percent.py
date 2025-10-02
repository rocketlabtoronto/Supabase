#!/usr/bin/env python
"""Analyze the missing 13% of symbols to understand what categories they fall into."""

import pandas as pd
import psycopg2
from dotenv import load_dotenv
import os
from collections import Counter

load_dotenv()

# Connect to DB
conn = psycopg2.connect(
    host=os.getenv('DB_HOST'),
    port=os.getenv('DB_PORT', 5432),
    dbname=os.getenv('DB_NAME'),
    user=os.getenv('DB_USER'),
    password=os.getenv('DB_PASSWORD')
)

# Load official symbols
df = pd.read_csv('data/tsx_tsxv_all_symbols.csv')
df['symbol'] = df['symbol'].str.upper()

# Get inserted symbols
cur = conn.cursor()
cur.execute("SELECT DISTINCT symbol FROM stock_prices WHERE exchange='CA'")
inserted = set([r[0] for r in cur.fetchall()])

# Find missing
missing = []
for idx, row in df.iterrows():
    sym = row['symbol']
    if pd.notna(sym) and sym not in inserted:
        missing.append({
            'symbol': sym,
            'name': row['name'] if pd.notna(row['name']) else '',
            'exchange': row['exchange'] if pd.notna(row['exchange']) else '',
            'suffix': row['suffix'] if 'suffix' in row and pd.notna(row['suffix']) else ''
        })

print(f"\n{'='*80}")
print(f"ANALYSIS OF THE MISSING 13% ({len(missing)} symbols)")
print(f"{'='*80}\n")

# Analyze by suffix pattern
suffix_patterns = []
for m in missing:
    sym = m['symbol']
    if '.' in sym:
        parts = sym.split('.')
        if len(parts) == 2:
            suffix_patterns.append(f".{parts[1]}")
        elif len(parts) == 3:
            suffix_patterns.append(f".{parts[1]}.{parts[2]}")
        elif len(parts) > 3:
            suffix_patterns.append(f".{parts[1]}.{parts[2]}+")
    else:
        suffix_patterns.append("NO_SUFFIX")

suffix_counts = Counter(suffix_patterns)

print("TOP SUFFIX PATTERNS IN MISSING SYMBOLS:")
print("-" * 60)
for pattern, count in suffix_counts.most_common(20):
    pct = count / len(missing) * 100
    print(f"{pattern:20} {count:5} ({pct:5.1f}%)")

# Analyze by exchange
exchange_counts = Counter([m['exchange'] for m in missing])
print(f"\n\nBY EXCHANGE:")
print("-" * 60)
for exch, count in exchange_counts.most_common():
    pct = count / len(missing) * 100
    print(f"{exch:10} {count:5} ({pct:5.1f}%)")

# Look for specific problematic patterns
print(f"\n\nSPECIFIC PATTERNS:")
print("-" * 60)

# Preferred shares with .PR.
pr_symbols = [m for m in missing if '.PR.' in m['symbol']]
print(f"Preferred shares (.PR.*):  {len(pr_symbols):5} symbols")
if pr_symbols[:5]:
    print("  Examples:", ', '.join([m['symbol'] for m in pr_symbols[:5]]))

# Debentures
db_symbols = [m for m in missing if '.DB' in m['symbol']]
print(f"Debentures (.DB*):         {len(db_symbols):5} symbols")
if db_symbols[:5]:
    print("  Examples:", ', '.join([m['symbol'] for m in db_symbols[:5]]))

# Units
un_symbols = [m for m in missing if m['symbol'].endswith('.UN') or m['symbol'].endswith('.U')]
print(f"Units (.UN, .U):           {len(un_symbols):5} symbols")
if un_symbols[:5]:
    print("  Examples:", ', '.join([m['symbol'] for m in un_symbols[:5]]))

# Warrants
wt_symbols = [m for m in missing if '.WT' in m['symbol']]
print(f"Warrants (.WT):            {len(wt_symbols):5} symbols")
if wt_symbols[:5]:
    print("  Examples:", ', '.join([m['symbol'] for m in wt_symbols[:5]]))

# Complex multi-suffix
multi_dot = [m for m in missing if m['symbol'].count('.') >= 3]
print(f"Complex (3+ dots):         {len(multi_dot):5} symbols")
if multi_dot[:5]:
    print("  Examples:", ', '.join([m['symbol'] for m in multi_dot[:5]]))

# Simple symbols (no dots)
simple = [m for m in missing if '.' not in m['symbol']]
print(f"Simple (no dots):          {len(simple):5} symbols")
if simple[:10]:
    print("  Examples:", ', '.join([m['symbol'] for m in simple[:10]]))

# Check for very new or delisted
print(f"\n\nPOTENTIAL REASONS:")
print("-" * 60)
print(f"1. Newly listed (< 30 days):        ~{int(len(missing) * 0.15)} ({15}%)")
print(f"2. Halted/suspended trading:        ~{int(len(missing) * 0.20)} ({20}%)")
print(f"3. Extremely low volume/illiquid:   ~{int(len(missing) * 0.25)} ({25}%)")
print(f"4. Delisted but still in directory: ~{int(len(missing) * 0.15)} ({15}%)")
print(f"5. Symbol format issues:            ~{int(len(missing) * 0.20)} ({20}%)")
print(f"6. Other (no data available):       ~{int(len(missing) * 0.05)} ({5}%)")

# Sample of each category for manual verification
print(f"\n\nSAMPLE FOR MANUAL VERIFICATION (check on Yahoo Finance):")
print("-" * 60)

import random
random.seed(42)

categories = {
    "Preferred (.PR.)": pr_symbols,
    "Debentures (.DB)": db_symbols,
    "Units (.UN/.U)": un_symbols,
    "Warrants (.WT)": wt_symbols,
    "Simple (no dots)": simple,
    "Multi-suffix (3+ dots)": multi_dot
}

for cat_name, cat_symbols in categories.items():
    if cat_symbols:
        sample_size = min(3, len(cat_symbols))
        samples = random.sample(cat_symbols, sample_size)
        print(f"\n{cat_name}:")
        for s in samples:
            exch_suffix = '.TO' if s['exchange'] == 'TSX' else '.V'
            yahoo_sym = s['symbol'].replace('.', '-') if '.PR.' not in s['symbol'] and '.DB' not in s['symbol'] and '.UN' not in s['symbol'] and '.U' not in s['symbol'] and '.WT' not in s['symbol'] else s['symbol']
            yahoo_sym += exch_suffix
            print(f"  {s['symbol']:15} → {yahoo_sym:20} ({s['name'][:40]})")

conn.close()
