#!/usr/bin/env python
"""Orchestrator: run SimFin financials (CA/US) and SimFin prices (US/CA)."""
import subprocess
import pathlib
import sys

SCRIPTS = [
    "ingestion/ingest_simfin_financials_csv_to_postgres_ca.py",
    "ingestion/ingest_simfin_financials_csv_to_postgres_us.py",
    "ingestion/ingest_simfin_prices_us.py",
    "ingestion/ingest_simfin_prices_ca.py",
]

def main():
    root = pathlib.Path(__file__).resolve().parent
    for script in SCRIPTS:
        path = root / script
        if path.exists():
            print(f"[orchestrator] Running {path} …")
            subprocess.run([sys.executable, str(path)], check=True)
        else:
            print(f"[orchestrator] Missing {path} – skipping.")

if __name__ == "__main__":
    main()
