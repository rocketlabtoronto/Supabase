#!/usr/bin/env python
"""Top-level runner that refreshes both US and Canadian data."""
import subprocess
import pathlib
import sys

SCRIPTS = [
    "scripts/load_yfinance_ca.py",
    "scripts/load_simfin.py"
]

def main():
    root = pathlib.Path(__file__).resolve().parent
    for script in SCRIPTS:
        path = root / script
        if path.exists():
            print(f"[update_financials] Running {path} …")
            subprocess.run([sys.executable, str(path)], check=True)
        else:
            print(f"[update_financials] Missing {path} – skipping.")

if __name__ == "__main__":
    main()
