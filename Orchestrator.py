#!/usr/bin/env python
"""Orchestrator: end-to-end run for financials and prices.

Steps:
1) Download official TMX symbol list (tsx_tsxv_all_symbols.csv from TMX API)
2) Derive instrument types (ETFs, trusts, etc.) → instrument_meta table
3) Ingest CA financials (yfinance → normalized financials table)
4) Ingest US financials (SimFin bulk → normalized financials table)
5) Ingest US daily prices (SimFin bulk → stock_prices)
6) Ingest CA daily prices (yfinance → stock_prices)

The orchestrator handles all dependencies automatically - just run and go!

Note: ALWAYS truncates financials and stock_prices tables at the start of each run
      to ensure clean data with no duplicates.
"""
import os
import subprocess
import pathlib
import sys
import argparse
import logging

import psycopg2
from dotenv import load_dotenv
from utils.logger import get_logger

SCRIPTS = [
    "scripts/download_tsx_symbols_from_api.py",
    "ingestion/derive_instrument_types_ca.py",
    "ingestion/ingest_yfinance_financials_api_to_postgres_ca.py",
    "ingestion/ingest_simfin_financials_api_to_postgres_us.py",
    "ingestion/ingest_simfin_prices_us.py",
    "ingestion/ingest_yfinance_prices_ca.py",
]

def _truncate_tables():
    """Truncate financials and stock_prices once at the beginning when requested.
    If TRUNCATE fails (e.g., due to FKs), falls back to DELETE.
    """
    load_dotenv(override=True)
    log = get_logger("orchestrator")
    conn = None
    try:
        conn = psycopg2.connect(
            host=os.getenv('DB_HOST'),
            port=os.getenv('DB_PORT', 5432),
            dbname=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
        )
        conn.autocommit = True
        cur = conn.cursor()
        log.info("Truncating tables: financials, stock_prices …")
        try:
            cur.execute("TRUNCATE TABLE financials, stock_prices")
        except Exception as e:
            log.warning("TRUNCATE failed; falling back to DELETE", exc_info=e)
            cur.execute("DELETE FROM financials")
            cur.execute("DELETE FROM stock_prices")
        finally:
            cur.close()
        log.info("Truncate completed.")
    except Exception as e:
        # Fail fast if explicit truncate was requested and cannot be completed
        log.error("Database truncate failed", exc_info=e)
        raise RuntimeError(f"Database truncate failed: {e}")
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def main():
    log = get_logger("orchestrator")
    parser = argparse.ArgumentParser(description="Run the full LookThroughProfits data pipeline")
    args = parser.parse_args()

    root = pathlib.Path(__file__).resolve().parent

    # ALWAYS truncate financials and stock_prices at the beginning of each run
    log.info("Truncating financials and stock_prices tables before run...")
    try:
        _truncate_tables()
    except Exception as e:
        log.error("Truncate step failed; aborting run", exc_info=e)
        sys.exit(1)

    max_errors = 3
    try:
        max_errors = int(os.getenv('ORCH_MAX_ERRORS', '3'))
    except Exception:
        max_errors = 3
    errors = 0

    for script in SCRIPTS:
        path = root / script
        if not path.exists():
            log.warning("Missing script; skipping", extra={"script": str(path)})
            continue
        # Skip SimFin steps if API key is missing
        if 'simfin' in path.name.lower() and not os.getenv('SIMFIN_API_KEY'):
            log.warning("Skipping %s because SIMFIN_API_KEY is not set", path.name)
            continue

        log.info("Running script … %s", path)
        try:
            # Inherit environment so .env variables apply to child processes
            env = os.environ.copy()
            # Ensure repo root is on PYTHONPATH so child scripts can import utils/
            root_str = str(root)
            existing_pp = env.get('PYTHONPATH', '')
            env['PYTHONPATH'] = root_str + (os.pathsep + existing_pp if existing_pp else '')
            result = subprocess.run([sys.executable, str(path)], check=False, env=env)
            if result.returncode != 0:
                # Exit code 2 means a 'not found' condition we consider critical → abort immediately
                if result.returncode == 2:
                    log.critical("Critical failure (not found) in %s. Aborting immediately.", path.name)
                    sys.exit(2)
                errors += 1
                log.error("Step failed: %s (exit=%s). Error count %s/%s",
                          path.name, result.returncode, errors, max_errors)
                if errors >= max_errors:
                    log.critical("Too many step failures (errors=%s >= max=%s). Aborting run.", errors, max_errors)
                    sys.exit(1)
            else:
                log.info("Finished %s with return code %s", path.name, result.returncode)
        except subprocess.CalledProcessError as e:
            errors += 1
            log.error("%s raised CalledProcessError", path, extra={"returncode": e.returncode})
            if errors >= max_errors:
                log.critical("Too many step failures (errors=%s >= max=%s). Aborting run.", errors, max_errors)
                sys.exit(1)
    
    log.info("="*80)
    log.info("ORCHESTRATOR COMPLETE")
    log.info("="*80)

if __name__ == "__main__":
    main()
