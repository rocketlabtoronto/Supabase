#!/usr/bin/env python
"""
Fetch latest stock prices for Canadian tickers using yfinance and insert into stock_prices (batch).

Data source for tickers: data/tsx_tsxv_all_symbols.csv (official TMX API symbol list)
                         with columns: symbol, name, parent_symbol, parent_name, suffix, exchange

Rules:
- Use ONLY symbols from the official TMX API list (no guessing variants)
- Use yfinance for market data.
- Insert only if the incoming latest_day is newer than what exists for that symbol+exchange (no upserts).
- Batch insert using psycopg2.extra				else:
					errors += 1

				log_progress(pct_tracker)

	elif use_fast_info:_values.

Env:
- PRICES_INSERT_BATCH (optional, default 2000)
- YF_DL_BATCH (optional, default 50)      # legacy: chunk size for yf.download fallback
- YF_QUOTE_BATCH (optional, default 100)  # chunk size for Yahoo quote API
 - YF_USE_QUOTES (optional, default false)  # fast path using Yahoo quote API
 - YF_USE_FAST_INFO (optional, default true)  # fastest path using yfinance Ticker.fast_info with concurrency
 - YF_USE_HISTORY (optional, default false)    # robust concurrent path using Ticker.history(period='1d')
 - YF_FALLBACKS (optional, default false)      # per-symbol fallbacks and Yahoo variant tries (slow with official list)
 - YF_MAX_WORKERS (optional, default 150)      # thread pool size for fast paths (optimal for I/O-bound network calls)
- YFIN_MAX_TICKERS (optional)             # cap for smoke testing
- CLEAR_STOCK_PRICES ('true' to delete existing CA rows before insert)
- DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
"""

import os
import sys
import math
import time
import json
from typing import Dict, List, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import yfinance as yf
import requests
from dotenv import load_dotenv
from utils.logger import get_logger
from utils.symbols import load_instrument_meta_map, yahoo_variants_all
from utils.symbols import tmx_symbol

load_dotenv(override=True)
log = get_logger("ingest_yfinance_prices_ca")


def get_env_int(name: str, default: int) -> int:
	v = os.getenv(name)
	if not v:
		return default
	try:
		i = int(str(v).strip())
		return i if i > 0 else default
	except Exception:
		return default


def get_existing_latest_map(conn) -> Dict[str, pd.Timestamp]:
	cur = conn.cursor()
	cur.execute(
		"""
		SELECT symbol, max(latest_day)
		FROM stock_prices
		WHERE exchange = 'CA'
		GROUP BY symbol
		"""
	)
	data = {row[0]: row[1] for row in cur.fetchall()}
	cur.close()
	return data


def yahoo_symbol(root: str, exchange: str) -> str:
	root = str(root).strip().upper()
	# Yahoo uses '-' instead of '.' for class/series (e.g., BAM.A -> BAM-A)
	root = root.replace('.', '-')
	ex = (exchange or '').strip().upper()
	if not root:
		return ''
	if ex in ('TSX', 'TSX-MKT', 'TORONTO'):
		return f"{root}.TO"
	if ex in ('TSXV', 'TSX-V', 'VENTURE'):
		return f"{root}.V"
	if ex in ('CSE', 'CN', 'CANADIAN SECURITIES EXCHANGE'):
		return f"{root}.CN"
	if ex in ('NEO', 'NEO-L', 'NEO EXCHANGE'):
		return f"{root}.NE"
	# default: return root without suffix (may succeed for some instruments)
	return root


def yahoo_variants(ysym: str) -> List[str]:
	"""Generate common Yahoo variants for TSX/TSXV trusts/ETFs when base symbol returns no data.
	Example: IGBT.TO -> IGBT-UN.TO, IGBT-U.TO
	"""
	try:
		if '.' not in ysym:
			return []
		base, ext = ysym.split('.', 1)
		ext = '.' + ext
		return [f"{base}-UN{ext}", f"{base}-U{ext}"]
	except Exception:
		return []


def load_official_tsx_symbols(csv_path: str, cap: int | None = None) -> List[Tuple[str, str, str, str]]:
	"""Load official TSX/TSXV symbols from TMX API export.
	
	Returns list of tuples: (tmx_symbol, exchange, yahoo_symbol, parent_symbol)
	
	Args:
		csv_path: Path to tsx_tsxv_all_symbols.csv (from TMX API)
		cap: Optional limit on number of symbols (for testing)
	
	CSV columns: symbol, name, parent_symbol, parent_name, suffix, exchange
	Example rows:
		ACO.X,ATCO Ltd. Cl I NV,ACO.X,ATCO Ltd.,X,TSX
		ACO.Y,ATCO Ltd. Cl II,ACO.X,ATCO Ltd.,Y,TSX
	"""
	df = pd.read_csv(csv_path)
	
	# Validate required columns
	required_cols = {'symbol', 'exchange', 'parent_symbol'}
	if not required_cols.issubset(df.columns):
		raise RuntimeError(f"CSV missing required columns. Expected: {required_cols}, Got: {set(df.columns)}")
	
	# Clean and prepare data
	df = df.dropna(subset=['symbol', 'exchange'])
	df['symbol'] = df['symbol'].astype(str).str.strip().str.upper()
	df['exchange'] = df['exchange'].astype(str).str.strip().str.upper()
	df['parent_symbol'] = df['parent_symbol'].astype(str).str.strip().str.upper()
	
	# Filter out empty symbols
	df = df[df['symbol'] != '']
	
	# Convert TMX symbols to Yahoo Finance format
	def tmx_to_yahoo(row):
		"""Convert TMX symbol to Yahoo Finance format with special handling for complex securities.
		
		Examples:
		  ACO.X -> ACO-X.TO (multi-class)
		  BN.PF.E -> BN.PR.E.TO (preferred shares - note PF -> PR)
		  BN.PR.A -> BN.PR.A.TO (preferred shares - keep PR as-is)
		  TD.PR.P -> TD.PR.P.TO (preferred shares)
		  AAAJ.P -> AAAJ.P.V (SPACs with just .P - very rare)
		  ACD.DB -> ACD.DB.TO (debentures)
		"""
		tmx_sym = row['symbol']
		exchange = row['exchange']
		
		# Handle preferred shares: .PF. -> .PR. (Yahoo convention)
		if '.PF.' in tmx_sym:
			yahoo_base = tmx_sym.replace('.PF.', '.PR.')
		
		# Handle preferred shares with .PR. - keep as-is (dots intact)
		elif '.PR.' in tmx_sym:
			yahoo_base = tmx_sym  # Keep: TD.PR.A, BN.PR.P, etc.
		
		# Handle debentures: .DB -> .DB (keep dots)
		elif '.DB' in tmx_sym:
			yahoo_base = tmx_sym
		
		# Handle warrants: .WT -> .WT (keep dots)
		elif '.WT' in tmx_sym:
			yahoo_base = tmx_sym
		
		# Handle rights: .RT -> .RT (keep dots)
		elif '.RT' in tmx_sym:
			yahoo_base = tmx_sym
		
		# Handle units: .UN or .U at end (keep dots)
		elif tmx_sym.endswith('.UN') or (tmx_sym.endswith('.U') and len(tmx_sym.split('.')[-1]) == 1):
			yahoo_base = tmx_sym
		
		# Standard multi-class stocks: Replace dots with hyphens
		else:
			yahoo_base = tmx_sym.replace('.', '-')
		
		# Add exchange suffix
		if exchange == 'TSX':
			return f"{yahoo_base}.TO"
		elif exchange == 'TSXV':
			return f"{yahoo_base}.V"
		else:
			# Default to .TO for unknown exchanges
			return f"{yahoo_base}.TO"
	
	df['yahoo_symbol'] = df.apply(tmx_to_yahoo, axis=1)
	
	# Create result tuples: (tmx_symbol, exchange, yahoo_symbol, parent_symbol)
	rows = list(df[['symbol', 'exchange', 'yahoo_symbol', 'parent_symbol']].itertuples(index=False, name=None))
	
	# Apply cap if specified
	if cap is not None and cap > 0:
		rows = rows[:cap]
	
	log.info(f"Loaded {len(rows)} official symbols from TMX API export")
	
	return rows


def load_tmx_symbols(csv_path: str, cap: int | None = None) -> List[Tuple[str, str, str]]:
	"""DEPRECATED: Use load_official_tsx_symbols instead.
	
	Old function kept for backward compatibility but logs warning.
	Returns list of tuples: (root, exchange, yahoo)
	"""
	log.warning("load_tmx_symbols is DEPRECATED - using load_official_tsx_symbols instead")
	
	# Try new format first
	official_csv = csv_path.replace('tmx_listed_companies.csv', 'tsx_tsxv_all_symbols.csv')
	if os.path.exists(official_csv):
		official_rows = load_official_tsx_symbols(official_csv, cap=cap)
		# Convert to old format: (parent_symbol, exchange_code, yahoo_symbol)
		result = []
		for tmx_sym, exchange, yahoo_sym, parent_sym in official_rows:
			# Map exchange: TSX->TSX, TSXV->TSXV
			result.append((parent_sym, exchange, yahoo_sym))
		return result
	
	# Fall back to old CSV format
	df = pd.read_csv(csv_path)
	# Normalize column names we need
	if 'Root Ticker' not in df.columns or 'Exchange' not in df.columns:
		raise RuntimeError("tmx_listed_companies.csv missing 'Root Ticker' or 'Exchange' columns")
	tmp = (
		df[['Root Ticker', 'Exchange']]
		.rename(columns={'Root Ticker': 'root', 'Exchange': 'ex'})
		.dropna()
	)
	tmp['root'] = tmp['root'].astype(str).str.strip().str.upper()
	tmp['ex'] = tmp['ex'].astype(str).str.strip().str.upper()
	tmp = tmp[tmp['root'] != '']
	tmp = tmp.drop_duplicates(subset=['root', 'ex'])
	tmp['yahoo'] = tmp.apply(lambda r: yahoo_symbol(r['root'], r['ex']), axis=1)
	tmp = tmp[tmp['yahoo'] != '']
	rows = list(tmp[['root', 'ex', 'yahoo']].itertuples(index=False, name=None))
	if cap is not None and cap > 0:
		rows = rows[:cap]
	return rows


def safe_num(v):
	try:
		if v is None or (isinstance(v, float) and math.isnan(v)):
			return None
		return float(v)
	except Exception:
		try:
			return float(str(v).replace(',', ''))
		except Exception:
			return None


def fetch_quotes_batch(symbols: List[str], timeout: int = 15) -> Dict[str, dict]:
	"""Fetch Yahoo quotes in one request for multiple symbols. Returns map: symbol -> quote dict.
	Uses public v7/finance/quote endpoint.
	"""
	if not symbols:
		return {}
	url = "https://query2.finance.yahoo.com/v7/finance/quote"
	# Yahoo supports up to ~400 symbols; keep conservative default via YF_QUOTE_BATCH
	params = {"symbols": ",".join(symbols)}
	headers = {
		"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
	}
	r = requests.get(url, params=params, headers=headers, timeout=timeout)
	r.raise_for_status()
	data = r.json()
	results = data.get("quoteResponse", {}).get("result", []) or data.get("quoteSummary", {}).get("result", [])
	out: Dict[str, dict] = {}
	for item in results:
		sym = item.get("symbol")
		if not sym:
			continue
		out[sym] = item
	return out


def main():
	script_name = os.path.basename(__file__)
	start_ts = time.time()

	# DB connect
	conn = psycopg2.connect(
		host=os.getenv('DB_HOST'),
		port=os.getenv('DB_PORT', 5432),
		dbname=os.getenv('DB_NAME'),
		user=os.getenv('DB_USER'),
		password=os.getenv('DB_PASSWORD'),
	)
	conn.autocommit = True
	cur = conn.cursor()

	if os.getenv('CLEAR_STOCK_PRICES', 'false').lower() == 'true':
		log.info('Clearing existing CA rows from stock_prices…')
		cur.execute("DELETE FROM stock_prices WHERE exchange='CA'")
		log.info("Deleted %s rows", cur.rowcount)

	# Load official TMX symbol list (no more guessing!)
	official_csv_path = os.path.join('data', 'tsx_tsxv_all_symbols.csv')
	
	# Check if official list exists, otherwise fall back to old CSV
	if os.path.exists(official_csv_path):
		log.info("Using official TMX API symbol list: %s", official_csv_path)
		max_tickers = get_env_int('YFIN_MAX_TICKERS', 0)
		cap = max_tickers if max_tickers > 0 else None
		try:
			# official_rows: list[(tmx_symbol, exchange, yahoo_symbol, parent_symbol)]
			official_rows = load_official_tsx_symbols(official_csv_path, cap=cap)
		except Exception as e:
			log.error("Failed to load official TSX symbol list", exc_info=e)
			return 1
		
		if not official_rows:
			log.warning('No symbols loaded from official TSX list')
			return 0
		
		# Build symbol maps
		# ym: yahoo_symbol -> (tmx_symbol, exchange)
		ym: Dict[str, Tuple[str, str]] = {}
		suffixed: List[str] = []
		
		for tmx_sym, exchange, yahoo_sym, parent_sym in official_rows:
			# Map exchange names to DB format
			if exchange == 'TSX':
				db_exchange = 'CA'  # Use 'CA' for all Canadian stocks in DB
			elif exchange == 'TSXV':
				db_exchange = 'CA'
			else:
				db_exchange = 'CA'
			
			ym[yahoo_sym] = (tmx_sym, db_exchange)
			suffixed.append(yahoo_sym)
		
		log.info("Loaded %s official symbols (no variant guessing needed)", len(suffixed))
		
	else:
		# Fallback to old CSV format with variant guessing
		log.warning("Official symbol list not found, falling back to old CSV: %s", 
		            os.path.join('data', 'tmx_listed_companies.csv'))
		csv_path = os.path.join('data', 'tmx_listed_companies.csv')
		max_tickers = get_env_int('YFIN_MAX_TICKERS', 0)
		cap = max_tickers if max_tickers > 0 else None
		try:
			rows = load_tmx_symbols(csv_path, cap=cap)
		except Exception as e:
			log.error("Failed to load TMX CSV", exc_info=e)
			return 1

		if not rows:
			log.warning('No tickers loaded from TMX CSV')
			return 0

		# Build maps, prefer instrument_meta.yahoo_symbol when available
		meta_map: Dict[str, str] = {}
		try:
			meta_map = load_instrument_meta_map()
			log.info("Loaded %s instrument_meta mappings", len(meta_map))
		except Exception as e:
			log.warning("Failed to load instrument_meta map; proceeding without", exc_info=e)

		# rows: list[(root, ex, yahoo)]
		ym: Dict[str, Tuple[str, str]] = {}
		suffixed: List[str] = []
		for root, ex, y in rows:
			tmx_sym = tmx_symbol(root, ex)
			y_final = meta_map.get(tmx_sym, y)
			ym[y_final] = (root, ex)
			suffixed.append(y_final)

	# Existing latest map (gating) by root symbol
	existing_latest = get_existing_latest_map(conn)

	dl_batch = get_env_int('YF_DL_BATCH', 50)
	quote_batch = get_env_int('YF_QUOTE_BATCH', 100)
	use_quotes = os.getenv('YF_USE_QUOTES', 'false').lower() == 'true'
	use_fast_info = os.getenv('YF_USE_FAST_INFO', 'true').lower() == 'true'
	use_history = os.getenv('YF_USE_HISTORY', 'false').lower() == 'true'
	use_fallbacks = os.getenv('YF_FALLBACKS', 'false').lower() == 'true'
	max_workers = get_env_int('YF_MAX_WORKERS', 150)
	ins_batch = get_env_int('PRICES_INSERT_BATCH', 2000)
	
	log.info("Mode selection: use_fast_info=%s, use_history=%s, use_quotes=%s, use_fallbacks=%s", 
	         use_fast_info, use_history, use_quotes, use_fallbacks)

	def chunks(lst, n):
		for i in range(0, len(lst), n):
			yield lst[i : i + n]

	def log_progress(pct_tracker):
		"""Log progress only every 5% to reduce overhead"""
		pct = math.floor(processed / total_syms * 100)
		if pct >= pct_tracker['last'] + 5:
			log.info("progress %s%% (%s/%s)", pct, processed, total_syms)
			pct_tracker['last'] = pct

	rows_buffer = []
	total_syms = len(suffixed)
	processed = 0
	successful = 0
	errors = 0
	pct_tracker = {'last': -5}

	if use_fast_info:
		log.info("Using yfinance fast_info with %s workers for %s symbols (with history() fallback for low-volume)", max_workers, total_syms)

		def process_symbol_with_fallback(ysym: str):
			"""Try fast_info first, fall back to history() for low-volume securities."""
			# Try fast_info first (fastest)
			try:
				t = yf.Ticker(ysym)
				fi = t.fast_info  # dict-like
				if fi:
					# Try multiple key variants for robustness across yfinance versions
					price = safe_num(
						(fi.get('lastPrice') if isinstance(fi, dict) else None)
						or (fi.get('last_price') if isinstance(fi, dict) else None)
						or (fi.get('regularMarketPrice') if isinstance(fi, dict) else None)
					)
					if price is not None:
						ts = (
							(fi.get('regularMarketTime') if isinstance(fi, dict) else None)
							or (fi.get('lastMarketTime') if isinstance(fi, dict) else None)
						)
						if not ts:
							# fall back to today
							latest_day = pd.Timestamp.utcnow().date()
						else:
							latest_day = pd.to_datetime(int(ts), unit='s', utc=True).date()

						root, ex = ym[ysym]
						last_seen = existing_latest.get(root)
						if last_seen is not None and latest_day <= last_seen:
							return None

						open_p = safe_num((fi.get('open') if isinstance(fi, dict) else None))
						high_p = safe_num((fi.get('dayHigh') if isinstance(fi, dict) else None) or (fi.get('regularMarketDayHigh') if isinstance(fi, dict) else None))
						low_p = safe_num((fi.get('dayLow') if isinstance(fi, dict) else None) or (fi.get('regularMarketDayLow') if isinstance(fi, dict) else None))
						vol_raw = (
							(fi.get('lastVolume') if isinstance(fi, dict) else None)
							or (fi.get('regularMarketVolume') if isinstance(fi, dict) else None)
						)
						vol = int(vol_raw) if vol_raw is not None and not pd.isna(vol_raw) else None
						
						# Skip change calculations for speed
						return (
							root,
							'CA',
							open_p,
							high_p,
							low_p,
							price,
							vol,
							latest_day,
							None,  # previous_close
							None,  # change
							None,  # change_pct
						)
			except Exception:
				pass  # Fall through to history() fallback
			
			# Fallback: Try history() for low-volume / illiquid securities
			try:
				t = yf.Ticker(ysym)
				df = t.history(period='5d', auto_adjust=False)  # Get up to 5 days for stale data
				if df is not None and not df.empty:
					last = df.tail(1)
					latest_day = pd.to_datetime(last.index[-1]).date()
					row = last.iloc[-1]
					close_p = safe_num(row.get('Close'))
					if close_p is not None:
						root, ex = ym[ysym]
						last_seen = existing_latest.get(root)
						if last_seen is not None and latest_day <= last_seen:
							return None
						
						open_p = safe_num(row.get('Open'))
						high_p = safe_num(row.get('High'))
						low_p = safe_num(row.get('Low'))
						vol = row.get('Volume')
						vol = int(vol) if vol is not None and not pd.isna(vol) else None
						
						return (
							root,
							'CA',
							open_p,
							high_p,
							low_p,
							close_p,
							vol,
							latest_day,
							None,
							None,
							None,
						)
			except Exception:
				pass
			
			return None

		with ThreadPoolExecutor(max_workers=max_workers) as exe:
			futures = {exe.submit(process_symbol_with_fallback, y): y for y in suffixed}
			for fut in as_completed(futures):
				processed += 1
				res = fut.result()
				if res is not None:
					rows_buffer.append(res)
					if len(rows_buffer) >= ins_batch:
						execute_values(
							cur,
							"INSERT INTO stock_prices (symbol, exchange, open, high, low, price, volume, latest_day, previous_close, change, change_percent) VALUES %s",
							rows_buffer,
							template="(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
						)
						rows_buffer.clear()
					successful += 1
				else:
					errors += 1

				log_progress(pct_tracker)

	elif use_quotes:
		log.info("Using Yahoo quote API for %s symbols (chunks of %s)", total_syms, quote_batch)
		for batch in chunks(suffixed, quote_batch):
			try:
				quotes = fetch_quotes_batch(batch)
			except Exception as e:
				log.error("quote batch failed", extra={"batch_size": len(batch)}, exc_info=e)
				# if unauthorized, fall back to yf.download path for remainder
				if '401' in str(e):
					log.warning('switching to yfinance download path due to 401 from quote API')
					use_quotes = False
					# process remaining including this batch via download path
					remaining = batch + [s for s in suffixed if s not in batch]
					# reset counters for accurate progress
					processed -= len(batch)  # undo processed increment scheduled below
					# use download path
					for dl_batch_syms in chunks(remaining, dl_batch):
						try:
							df = yf.download(
								tickers=dl_batch_syms,
								period='2d',
								interval='1d',
								auto_adjust=False,
								group_by='ticker',
								threads=True,
								progress=False,
								actions=False,
							)
						except Exception as e2:
							log.error("yfinance download failed", extra={"batch_size": len(dl_batch_syms)}, exc_info=e2)
							errors += len(dl_batch_syms)
							processed += len(dl_batch_syms)
							continue

						per = {}
						if isinstance(df.columns, pd.MultiIndex):
							for t in dl_batch_syms:
								if t in getattr(df.columns, 'levels', [[], []])[0]:
									try:
										sub = df[t]
										if isinstance(sub, pd.DataFrame):
											per[t] = sub
									except Exception:
										continue
						else:
							if len(dl_batch_syms) == 1:
								per[dl_batch_syms[0]] = df

						for ysym, sub in per.items():
							processed += 1
							try:
								if sub is None or sub.empty:
									continue
								sub2 = sub.tail(2)
								sub2 = sub2[['Open', 'High', 'Low', 'Close', 'Volume']].copy()
								sub2 = sub2.dropna(how='all')
								if sub2.empty:
									continue
								latest = sub2.iloc[-1]
								latest_day = pd.to_datetime(sub2.index[-1]).date()
								prev_close = None
								if len(sub2) >= 2:
									prev = sub2.iloc[0]
									if not pd.isna(prev.get('Close')):
										prev_close = safe_num(prev.get('Close'))

								root, ex = ym[ysym]
								last_seen = existing_latest.get(root)
								if last_seen is not None and latest_day <= last_seen:
									continue

								open_p = safe_num(latest.get('Open'))
								high_p = safe_num(latest.get('High'))
								low_p = safe_num(latest.get('Low'))
								close_p = safe_num(latest.get('Close'))
								vol = latest.get('Volume')
								vol = int(vol) if not pd.isna(vol) else None

								if close_p is None:
									continue

								change = None
								change_pct = None
								if prev_close is not None and prev_close != 0:
									change = close_p - prev_close
									change_pct = f"{(change / prev_close * 100):.2f}%"

								rows_buffer.append((
									root,
									'CA',
									open_p,
									high_p,
									low_p,
									close_p,
									vol,
									latest_day,
									prev_close,
									change,
									change_pct,
								))
								successful += 1
							except Exception as e3:
								log.error("processing symbol failed", extra={"symbol": ysym}, exc_info=e3)
								errors += 1

							log_progress(pct_tracker)
					break
				else:
					errors += len(batch)
					processed += len(batch)
					continue

			for ysym in batch:
				processed += 1
				try:
					q = quotes.get(ysym)
					if not q:
						continue
					price = safe_num(q.get('regularMarketPrice'))
					if price is None:
						continue
					# Times are epoch seconds
					ts = q.get('regularMarketTime') or q.get('postMarketTime') or q.get('preMarketTime')
					if not ts:
						continue
					latest_day = pd.to_datetime(int(ts), unit='s').date()

					root, ex = ym[ysym]
					last_seen = existing_latest.get(root)
					if last_seen is not None and latest_day <= last_seen:
						continue

					open_p = safe_num(q.get('regularMarketOpen'))
					high_p = safe_num(q.get('regularMarketDayHigh'))
					low_p = safe_num(q.get('regularMarketDayLow'))
					vol = q.get('regularMarketVolume')
					vol = int(vol) if vol is not None else None
					prev_close = safe_num(q.get('regularMarketPreviousClose'))
					change = None
					change_pct = None
					if prev_close is not None and prev_close != 0:
						change = price - prev_close
						change_pct = f"{(change / prev_close * 100):.2f}%"

					rows_buffer.append((
						root,
						'CA',
						open_p,
						high_p,
						low_p,
						price,
						vol,
						latest_day,
						prev_close,
						change,
						change_pct,
					))

					if len(rows_buffer) >= ins_batch:
						execute_values(
							cur,
							"INSERT INTO stock_prices (symbol, exchange, open, high, low, price, volume, latest_day, previous_close, change, change_percent) VALUES %s",
							rows_buffer,
							template="(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
						)
						rows_buffer.clear()
					successful += 1
				except Exception as e:
					log.error("processing symbol failed", extra={"symbol": ysym}, exc_info=e)
					errors += 1

				log_progress(pct_tracker)
	else:
		log.info("Using yfinance download for %s symbols (chunks of %s)", total_syms, dl_batch)
		for batch in chunks(suffixed, dl_batch):
			try:
				df = yf.download(
					tickers=batch,
					period='2d',
					interval='1d',
					auto_adjust=False,
					group_by='ticker',
					threads=True,
					progress=False,
				)
			except Exception as e:
				log.error("yfinance download failed", extra={"batch_size": len(batch)}, exc_info=e)
				errors += len(batch)
				processed += len(batch)
				continue

			per = {}
			if isinstance(df.columns, pd.MultiIndex):
				for t in batch:
					if t in getattr(df.columns, 'levels', [[], []])[0]:
						try:
							sub = df[t]
							if isinstance(sub, pd.DataFrame):
								per[t] = sub
						except Exception:
							continue
			else:
				if len(batch) == 1:
					per[batch[0]] = df

			for ysym, sub in per.items():
				processed += 1
				try:
					if sub is None or sub.empty:
						continue
					sub2 = sub.tail(2)
					sub2 = sub2[['Open', 'High', 'Low', 'Close', 'Volume']].copy()
					sub2 = sub2.dropna(how='all')
					if sub2.empty:
						continue
					latest = sub2.iloc[-1]
					latest_day = pd.to_datetime(sub2.index[-1]).date()
					prev_close = None
					if len(sub2) >= 2:
						prev = sub2.iloc[0]
						if not pd.isna(prev.get('Close')):
							prev_close = safe_num(prev.get('Close'))

					root, ex = ym[ysym]
					last_seen = existing_latest.get(root)
					if last_seen is not None and latest_day <= last_seen:
						continue

					open_p = safe_num(latest.get('Open'))
					high_p = safe_num(latest.get('High'))
					low_p = safe_num(latest.get('Low'))
					close_p = safe_num(latest.get('Close'))
					vol = latest.get('Volume')
					vol = int(vol) if not pd.isna(vol) else None

					if close_p is None:
						continue

					change = None
					change_pct = None
					if prev_close is not None and prev_close != 0:
						change = close_p - prev_close
						change_pct = f"{(change / prev_close * 100):.2f}%"

					rows_buffer.append((
						root,
						'CA',
						open_p,
						high_p,
						low_p,
						close_p,
						vol,
						latest_day,
						prev_close,
						change,
						change_pct,
					))

					if len(rows_buffer) >= ins_batch:
						execute_values(
							cur,
							"INSERT INTO stock_prices (symbol, exchange, open, high, low, price, volume, latest_day, previous_close, change, change_percent) VALUES %s",
							rows_buffer,
							template="(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
						)
						rows_buffer.clear()
					successful += 1
				except Exception as e:
					log.error("processing symbol failed", extra={"symbol": ysym}, exc_info=e)
					errors += 1

				log_progress(pct_tracker)

	# Flush remaining
	if rows_buffer:
		execute_values(
			cur,
			"INSERT INTO stock_prices (symbol, exchange, open, high, low, price, volume, latest_day, previous_close, change, change_percent) VALUES %s",
			rows_buffer,
			template="(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
		)
		rows_buffer.clear()

	conn.commit()
	cur.close()
	conn.close()

	log.info("Complete: %s/%s attempted symbols inserted (post-gating)", successful, total_syms)
	error_rate = (errors / total_syms) if total_syms else 0.0
	status = 'success' if errors == 0 else ('warning' if error_rate <= 0.01 else 'error')
	message = f"symbols total={total_syms} success={successful} errors={errors} rate={error_rate:.4f}"
	try:
		conn2 = psycopg2.connect(
			host=os.getenv('DB_HOST'), port=os.getenv('DB_PORT', 5432), dbname=os.getenv('DB_NAME'),
			user=os.getenv('DB_USER'), password=os.getenv('DB_PASSWORD')
		); cur2 = conn2.cursor()
		now = time.time()
		cur2.execute(
			"insert into ingest_logs (script, status, message, details, started_at, ended_at, duration_ms) values (%s,%s,%s,%s,to_timestamp(%s),to_timestamp(%s),%s)",
			(
				os.path.basename(__file__),
				status,
				message,
				json.dumps({"symbols": {"total": total_syms, "success": successful, "errors": errors, "error_rate": round(error_rate, 6)}}),
				start_ts,
				now,
				int((now - start_ts) * 1000),
			),
		); conn2.commit(); cur2.close(); conn2.close()
	except Exception:
		pass
	# If any symbols errored and FAIL_ON_NOT_FOUND, exit code 2 to signal critical
	if errors > 0 and os.getenv('FAIL_ON_NOT_FOUND', 'true').lower() == 'true':
		log.error("One or more tickers not found or failed (errors=%s). Exiting with code 2.", errors)
		return 2
	return 0


if __name__ == '__main__':
	sys.exit(main())
