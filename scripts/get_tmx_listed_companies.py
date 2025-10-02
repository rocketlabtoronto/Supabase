import os
import sys
import re
from io import BytesIO
from typing import Optional, List

import requests
import pandas as pd


RESOURCE_URL = "https://www.tsx.com/en/resource/571"


def guess_extension(content_type: str, url: str, dispo: str) -> str:
	ct = (content_type or "").lower()
	url_l = (url or "").lower()
	dispo_l = (dispo or "").lower()
	m = re.search(r'filename="?([^";]+)"?', dispo or "")
	if m:
		fn = m.group(1).lower()
		if fn.endswith((".csv", ".xlsx", ".xls")):
			return os.path.splitext(fn)[1]
	if "csv" in ct or url_l.endswith(".csv"):
		return ".csv"
	if "sheet" in ct or "excel" in ct or url_l.endswith(".xlsx"):
		return ".xlsx"
	if url_l.endswith(".xls"):
		return ".xls"
	return ""


def download_resource() -> tuple[bytes, str, dict]:
	headers = {"User-Agent": "tmx-list-fetcher/1.0"}
	urls_to_try = [RESOURCE_URL, RESOURCE_URL.rstrip("/") + "/download"]
	last_err: Optional[Exception] = None
	for url in urls_to_try:
		try:
			print(f"Fetching: {url}")
			resp = requests.get(url, headers=headers, timeout=60)
			resp.raise_for_status()
			return resp.content, resp.url, dict(resp.headers)
		except Exception as e:
			print(f"Failed: {e}")
			last_err = e
			continue
	raise RuntimeError(f"Unable to download resource. Last error: {last_err}")


def save_raw(content: bytes, eff_url: str, headers: dict, out_dir: str) -> str:
	os.makedirs(out_dir, exist_ok=True)
	ext = guess_extension(headers.get("Content-Type", ""), eff_url, headers.get("Content-Disposition", ""))
	if not ext:
		ext = ".bin"
	raw_path = os.path.join(out_dir, f"tmx_listed_companies_raw{ext}")
	with open(raw_path, "wb") as f:
		f.write(content)
	print(f"Saved raw TMX issuer list to {raw_path}")
	return raw_path


def _clean_columns(cols: List[str]) -> List[str]:
	cleaned: List[str] = []
	for c in cols:
		c = str(c) if c is not None else ""
		c = c.replace("\n", " ").strip()
		c = re.sub(r"\s+", " ", c)
		cleaned.append(c)
	return cleaned


def _find_header_row(df_raw: pd.DataFrame) -> Optional[int]:
	# Look for a row that contains the expected header token 'Co_ID' and 'Exchange' and 'Name'
	for i in range(min(len(df_raw), 100)):
		row_vals = [str(x) if x is not None else "" for x in df_raw.iloc[i].tolist()]
		if "Co_ID" in row_vals and "Exchange" in row_vals and "Name" in row_vals:
			return i
	return None


def _read_issuer_sheet(xl: pd.ExcelFile, sheet_name: str) -> pd.DataFrame:
	# Read without header to manually detect the proper header row
	df_raw = xl.parse(sheet_name=sheet_name, header=None, dtype=object, engine="openpyxl")
	hdr_idx = _find_header_row(df_raw)
	if hdr_idx is None:
		raise RuntimeError(f"Could not locate header row in sheet '{sheet_name}'")
	headers = _clean_columns(df_raw.iloc[hdr_idx].tolist())
	df = df_raw.iloc[hdr_idx + 1 :].copy()
	df.columns = headers[: len(df.columns)]
	# Drop fully empty rows
	df = df.dropna(how="all")
	# Keep first six columns by position
	df = df.iloc[:, :6]
	# Re-clean column names after slicing
	df.columns = _clean_columns(df.columns.tolist())
	return df


def to_csv_from_raw(raw_path: str, out_dir: str) -> str:
	os.makedirs(out_dir, exist_ok=True)
	out_csv = os.path.join(out_dir, "tmx_listed_companies.csv")

	ext = os.path.splitext(raw_path)[1].lower()
	if ext not in (".xlsx", ".xls"):
		raise RuntimeError(f"Unexpected raw file extension '{ext}', expected an Excel workbook")

	xl = pd.ExcelFile(raw_path, engine="openpyxl")
	# Only process issuer sheets; skip hidden/cache sheets
	sheets = [s for s in xl.sheet_names if "Issuer" in s or "Issuers" in s]
	if not sheets:
		# Fallback: process all non-hidden sheets that don't start with underscore
		sheets = [s for s in xl.sheet_names if not s.startswith("_")]

	frames: List[pd.DataFrame] = []
	for s in sheets:
		try:
			df_s = _read_issuer_sheet(xl, s)
			# Add a column to record source sheet (optional, helps debugging)
			df_s.insert(0, "SourceSheet", s)
			frames.append(df_s)
		except Exception as e:
			print(f"Warning: skipping sheet '{s}': {e}")
			continue

	if not frames:
		raise RuntimeError("No usable sheets found in workbook")

	df_all = pd.concat(frames, ignore_index=True)
	# Write to CSV with utf-8-sig
	df_all.to_csv(out_csv, index=False, encoding="utf-8-sig")
	print(f"Converted Excel to CSV (first six columns) at {out_csv}")
	return out_csv


def main():
	repo_root = os.path.dirname(os.path.dirname(__file__))
	data_dir = os.path.join(repo_root, "data")
	content, eff_url, headers = download_resource()
	raw_path = save_raw(content, eff_url, headers, data_dir)
	csv_path = to_csv_from_raw(raw_path, data_dir)
	print(f"Done. CSV ready at: {csv_path}")


if __name__ == "__main__":
	try:
		main()
	except Exception as e:
		print(str(e), file=sys.stderr)
		sys.exit(1)
