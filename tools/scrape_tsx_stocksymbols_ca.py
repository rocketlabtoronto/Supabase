"""
Scrape every TSX stock symbol (no prices) from EODData (A‑Z pages) and write data/ca_tickers.txt
Uses a regex: /stockquote/TSX/<SYMBOL>.htm
"""

import re, time, os, requests

BASE = "https://www.eoddata.com/stocklist/TSX/{}.htm"
LETTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"

# Regex: capture the symbol between /TSX/ and .htm
SYM_RE = re.compile(r"/stockquote/TSX/([A-Z0-9\.]+)\.htm", re.IGNORECASE)

def scrape_eod_symbols_ca():
    symbols = set()
    headers = {"User-Agent": "Mozilla/5.0"}

    for letter in LETTERS:
        url = BASE.format(letter)
        print(f"[fetching] {url}")
        try:
            html = requests.get(url, headers=headers, timeout=10).text
            matches = SYM_RE.findall(html)
            symbols.update(matches)
            time.sleep(0.5)  # be polite
        except Exception as e:
            print(f"[error] {e} for {url}")

    # Append .TO suffix for Yahoo Finance compatibility
    symbols_to = sorted(f"{t}.TO" for t in symbols)

    os.makedirs("data", exist_ok=True)
    out_path = "data/ca_tickers.txt"
    with open(out_path, "w") as f:
        for t in symbols_to:
            f.write(t + "\n")

    print(f"\n✅ Saved {len(symbols_to)} TSX symbols to {out_path}")

if __name__ == "__main__":
    scrape_eod_symbols_ca()
