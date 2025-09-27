#!/usr/bin/env python
"""Top-level runner that refreshes US price data using SimFin API."""
import subprocess
import pathlib
import sys
import datetime

SCRIPTS = [
    "scripts/load_simfin_prices.py"
]

def run_script(script_path):
    """Run a script and return True if successful, False otherwise."""
    try:
        print(f"[update_price] Running {script_path} …")
        result = subprocess.run([sys.executable, str(script_path)], 
                              check=True, capture_output=True, text=True)
        print(f"[update_price] ✅ {script_path} completed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"[update_price] ❌ {script_path} failed with return code {e.returncode}")
        if e.stdout:
            print(f"[stdout] {e.stdout}")
        if e.stderr:
            print(f"[stderr] {e.stderr}")
        return False
    except Exception as e:
        print(f"[update_price] ❌ {script_path} failed with error: {e}")
        return False

def main():
    print(f"Starting stock price update process using SimFin API at {datetime.datetime.now()}")
    root = pathlib.Path(__file__).resolve().parent
    
    for script in SCRIPTS:
        path = root / script
        if path.exists():
            success = run_script(path)
            if success:
                print(f"[update_price] ✅ Price update completed successfully using SimFin")
                return
            else:
                print(f"[update_price] ❌ SimFin price update failed")
                break
        else:
            print(f"[update_price] Missing {path} – cannot proceed.")
    
    print("[update_price] ❌ SimFin price update failed!")
    sys.exit(1)

if __name__ == "__main__":
    main()
