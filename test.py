"""
test.py — Minimal, self-contained demo that calls the SimFin API directly
to fetch annual Income Statement data in-memory (no local CSV files written),
then extracts the latest Revenue and Net Income for a given ticker.

Design goals:
- Use the official SimFin bulk API endpoint for fundamentals (CSV-in-zip),
	but load and parse it entirely in RAM so the workspace stays clean.
- Keep the flow simple and robust: request -> unzip -> pandas read -> filter.
- Be explicit about assumptions and provide helpful comments to future readers.

Important behavior note:
- The SimFin bulk endpoint returns the ENTIRE dataset for the given
	market/variant (all companies), not a single company. This script downloads
	that whole CSV into memory and then filters for the requested ticker.
	Nothing is written to disk, but RAM usage depends on dataset size.

Environment variables (via .env or shell):
- SIMFIN_API_KEY     (required) API key string.
- TEST_TICKER        (optional) Default 'JPM'; the ticker we want to look up.
- SIMFIN_MARKET      (optional) Default 'us'; the dataset market.
- SIMFIN_VARIANT     (optional) Default 'annual'; 'annual' or 'quarterly' etc.

Notes on financial-sector datasets:
- SimFin ships separate datasets for banks and insurance companies because
	their statements differ from industrials. For large US banks (like JPM),
	the "income-banks" dataset is the correct place to look.
	We therefore try 'income-banks' first and then fall back to 'income'.
"""

import os
import sys
import io
import zipfile
from typing import Optional, Tuple

import requests
import pandas as pd
from dotenv import load_dotenv


def get_env(key: str, default: Optional[str] = None) -> Optional[str]:
	"""Retrieve an environment variable (or .env value),
	returning `default` if missing or blank.

	Example: get_env('SIMFIN_MARKET', 'us') -> 'us'
	"""
	v = os.getenv(key)
	if v is None:
		return default
	v = v.strip()
	return v if v else default


def fetch_bulk_dataset(dataset: str, market: str, variant: str, api_key: str) -> pd.DataFrame:
	"""Fetch a fundamentals dataset from the SimFin Bulk API and return it
	as a pandas DataFrame, entirely in memory (no disk writes).

	Parameters:
	- dataset: e.g. 'income-banks', 'income', 'balance', 'cashflow', etc.
	- market:  e.g. 'us', 'de' (supported markets depend on your access level)
	- variant: e.g. 'annual', 'quarterly', 'ttm'
	- api_key: your SimFin API key string

	The Bulk API response is a ZIP archive containing one CSV (semicolon
	delimited). We unzip in-memory and parse directly with pandas.
	This means the FULL dataset (all companies for that market/variant)
	is loaded into memory—there is no per-ticker server-side filter in
	the bulk endpoint.

	Raises SystemExit with a short error message if the HTTP request fails,
	so callers can handle it at a single boundary.
	"""
	base_url = "https://prod.simfin.com/api/bulk-download/s3"
	params = {
		"dataset": dataset,
		"market": market,
		"variant": variant,
	}
	headers = {"Authorization": f"api-key {api_key}"}

	# Perform the HTTP GET with an Authorization header. The server returns
	# a zip file with the selected dataset. We keep a modest timeout (60s)
	# for reliability; feel free to adjust if your connection is slow.
	r = requests.get(base_url, params=params, headers=headers, timeout=60)
	if not r.ok:
		raise SystemExit(f"SimFin API error {r.status_code}: {r.text[:400]}")

	# Response is a zip file. Read in-memory and find the CSV inside.
	# Open the response bytes as a zip file without touching disk.
	with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
		csv_names = [n for n in zf.namelist() if n.lower().endswith('.csv')]
		if not csv_names:
			raise SystemExit("Zip response contained no CSV file.")
		# Pick the first CSV (there should only be one)
		with zf.open(csv_names[0]) as f:
			# SimFin CSVs use semicolon separators.
			# Load ONLY the first six columns for efficiency per request.
			# This uses positional indices (0..5). Column names are preserved for those six.
			# Note: downstream fields like Revenue/Net Income might not be available
			# if they do not fall within the first six columns of the dataset.
			df = pd.read_csv(
				f,
				sep=';',
				header=0,
				low_memory=False,
				usecols=range(6),
			)
			return df


def extract_latest_income(df: pd.DataFrame, ticker: str) -> Optional[Tuple[Optional[float], Optional[float]]]:
	"""Filter a SimFin Income Statement DataFrame to a given ticker,
	then return the pair (latest_revenue, latest_net_income).

	Implementation details:
	- We accept that 'Ticker' might not be a column if the file was saved
		with it as an index elsewhere; we attempt reset_index() gracefully.
	- We define "latest" as the row with the most recent Report Date when
		available; otherwise we fall back to the highest Fiscal Year.
	- Column names can vary slightly across datasets; we probe a few likely
		options for both revenue and net income.
	"""
	if 'Ticker' not in df.columns:
		# Some CSVs may already have been saved with an index; handle best-effort
		if df.index.name == 'Ticker' or (isinstance(df.index, pd.MultiIndex) and 'Ticker' in df.index.names):
			try:
				df = df.reset_index()
			except Exception:
				pass

	if 'Ticker' not in df.columns:
		return None

	# Case-insensitive ticker match; ensure the value is string before upper().
	sub = df[df['Ticker'].astype(str).str.upper() == ticker.upper()].copy()
	if sub.empty:
		return None

	# Determine how to sort to get the latest row.
	sort_cols = []
	if 'Report Date' in sub.columns:
		# Parse dates safely (coerce invalid), then use that as a temp sort key.
		try:
			sub['__rd'] = pd.to_datetime(sub['Report Date'], errors='coerce')
			sort_cols.append('__rd')
		except Exception:
			pass
	if not sort_cols and 'Fiscal Year' in sub.columns:
		sort_cols.append('Fiscal Year')

	if sort_cols:
		sub = sub.sort_values(sort_cols, ascending=True)

	latest = sub.iloc[-1]

	# Try to find revenue and net income fields across variants
	revenue_cols = ['Revenue', 'Total Revenue', 'Sales']
	ni_cols = ['Net Income', 'Net Income (Common)', 'Profit (Loss)']

	def pick_float(row, candidates):
		for c in candidates:
			if c in row.index:
				v = row[c]
				try:
					return float(str(v).replace(',', ''))
				except Exception:
					continue
		return None

	rev = pick_float(latest, revenue_cols)
	ni = pick_float(latest, ni_cols)
	return (rev, ni)


def main():
	"""Entrypoint for running this script directly.

	Steps:
	1) Load .env (for local development) and read the SimFin API key.
	2) Determine ticker/market/variant from env with sensible defaults.
	3) Try income-banks first (covers JPM), then fallback to income.
	4) Print a compact JSON-like dict with the result, or a clear error.
	"""
	load_dotenv()

	# 1) API key (required)
	api_key = get_env('SIMFIN_API_KEY')
	if not api_key or api_key == 'SIMFIN_API_KEY':
		# Fail fast with a friendly hint.
		raise SystemExit('SIMFIN_API_KEY is not set. Put it in your .env or environment.')

	# 2) Inputs (ticker defaults to JPM; market/variant are typical defaults)
	ticker = get_env('TEST_TICKER', 'JPM')
	market = get_env('SIMFIN_MARKET', 'us')
	variant = get_env('SIMFIN_VARIANT', 'annual')

	# 3) Try banks first, then general income. Track which datasets we attempt
	# so we can print a helpful error if nothing is found.
	tried = []
	for dataset in ['income-banks', 'income']:
		tried.append(dataset)
		try:
			df = fetch_bulk_dataset(dataset=dataset, market=market, variant=variant, api_key=api_key)
		except SystemExit:
			# If the HTTP call failed with an explicit API error, bubble it up.
			raise
		except Exception:
			# Network hiccup, unexpected zip format, parsing issue, etc.
			# Continue to the next dataset attempt.
			continue

		# 4) Extract latest Revenue & Net Income for the selected ticker.
		#    Reminder: at this point we have the full dataset in memory and
		#    we are filtering client-side for the specific ticker.
		res = extract_latest_income(df, ticker)
		if res is not None:
			revenue, net_income = res
			print({
				'ticker': ticker,
				'dataset': dataset,
				'market': market,
				'variant': variant,
				'revenue': revenue,
				'net_income': net_income,
			})
			return

	# If we reach here, neither income-banks nor income contained the ticker.
	print(f"[error] {ticker} not found in datasets tried: {', '.join(tried)}")
	sys.exit(1)


if __name__ == '__main__':
	# Execute the top-level routine. Use PowerShell or your IDE's Run action.
	# Examples (PowerShell):
	#   $env:SIMFIN_API_KEY = '...'; python .\test.py
	#   $env:TEST_TICKER = 'AAPL'; python .\test.py
	main()

