#!/usr/bin/env python
"""Orchestrator: load CA Yahoo financials and SimFin CSV statements."""
import subprocess
import pathlib
import sys

SCRIPTS = [
    "ingestion/ingest_yahoo_financials_postgres_ca.py",
    "ingestion/ingest_simfin_financials_csv_to_postgres_us.py",
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
