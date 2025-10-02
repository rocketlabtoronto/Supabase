#!/usr/bin/env python
"""Check which symbols from official list were not inserted."""

import pandas as pd
import psycopg2
from dotenv import load_dotenv
import os
import random

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
df_official = pd.read_csv('data/tsx_tsxv_all_symbols.csv')
official_syms = set(df_official['symbol'].str.upper())

# Get inserted symbols
cur = conn.cursor()
cur.execute("SELECT DISTINCT symbol FROM stock_prices WHERE exchange='CA'")
inserted = set([r[0] for r in cur.fetchall()])

# Find missing
missing = official_syms - inserted

print(f'Total official symbols: {len(official_syms)}')
print(f'Inserted symbols: {len(inserted)}')
print(f'Missing symbols: {len(missing)}')
print(f'Percentage inserted: {len(inserted)/len(official_syms)*100:.1f}%')

# Random sample (filter out NaN values first)
missing_clean = [s for s in missing if isinstance(s, str)]
sample = random.sample(missing_clean, min(10, len(missing_clean)))
print(f'\nRandom sample of 10 missing symbols:')
for s in sample:
    # Get full details from CSV
    row = df_official[df_official['symbol'].str.upper() == s]
    if not row.empty:
        name = row.iloc[0]['name']
        exchange = row.iloc[0]['exchange']
        print(f"  {s:12} - {name:40} ({exchange})")

conn.close()
