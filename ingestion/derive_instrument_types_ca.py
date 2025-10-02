#!/usr/bin/env python
"""Classify CA instruments (equities, ETFs, funds, trusts, etc.) and persist metadata.

Reads symbols from tmx_issuers and uses yfinance info/quoteType to derive an asset type taxonomy.
Writes results to instrument_meta table. Safe to re-run; upserts by symbol.
"""
import os
import sys
import time
import json
import logging
from typing import Dict, Any

import psycopg2
from psycopg2.extras import execute_values
import yfinance as yf
from dotenv import load_dotenv
from utils.logger import get_logger

load_dotenv(override=True)
log = get_logger("derive_instrument_types_ca")


def classify(info: Dict[str, Any]) -> Dict[str, Any]:
    qtype = (info or {}).get('quoteType') or (info or {}).get('quote_type')
    long_name = (info or {}).get('longName') or (info or {}).get('long_name')
    category = (info or {}).get('category')
    fund_family = (info or {}).get('fundFamily')
    currency = (info or {}).get('currency')
    is_etf = bool((info or {}).get('isEtf') or (info or {}).get('isETF'))
    is_mutual_fund = bool((info or {}).get('isMutualFund'))
    is_cef = 'closed-end' in str(category or '').lower() or 'closed end' in str(category or '').lower()
    legal_type = None
    is_trust = False
    # Heuristics for trusts (e.g., "Trust" in long name, or TSX suffix -UN/-U often indicates trust units)
    if long_name and ('trust' in long_name.lower()):
        is_trust = True
        legal_type = 'trust'
    asset_type = None
    if is_etf:
        asset_type = 'etf'
    elif is_mutual_fund:
        asset_type = 'mutual_fund'
    elif is_cef:
        asset_type = 'closed_end_fund'
    elif is_trust:
        asset_type = 'trust'
    else:
        # fall back to quoteType mapping
        ql = str(qtype or '').lower()
        if ql in ('equity', 'stock', 'company'):
            asset_type = 'equity'
        elif ql in ('fund'):
            asset_type = 'fund'
        elif ql in ('etf'):
            asset_type = 'etf'
        elif ql in ('mutualfund'):
            asset_type = 'mutual_fund'
        elif ql in ('index'):
            asset_type = 'index'
        elif ql:
            asset_type = ql
        else:
            asset_type = 'unknown'

    # Extract numeric metrics where applicable
    def num(k):
        try:
            v = info.get(k)
            if v is None:
                return None
            return float(v)
        except Exception:
            return None

    meta = {
        'quote_type': qtype,
        'asset_type': asset_type,
        'is_etf': is_etf,
        'is_mutual_fund': is_mutual_fund,
        'is_closed_end_fund': is_cef,
        'is_trust': is_trust,
        'is_index': str(info.get('quoteType', '')).lower() == 'index',
        'category': category,
        'fund_family': fund_family,
        'legal_type': legal_type,
        'currency': currency,
        'underlying_symbol': info.get('underlyingSymbol') or info.get('underlying_symbol'),
        'nav_price': num('navPrice'),
        'expense_ratio': num('annualReportExpenseRatio'),
        'total_assets': num('totalAssets'),
        'yield': num('yield'),
        'ytd_return': num('ytdReturn'),
        'three_year_avg_return': num('threeYearAverageReturn'),
        'five_year_avg_return': num('fiveYearAverageReturn'),
        'beta_3y': num('beta3Year'),
        'long_name': long_name,
        'attributes': info,
    }
    return meta


def upsert_meta(rows):
    conn = psycopg2.connect(
        host=os.getenv('DB_HOST'), port=os.getenv('DB_PORT', 5432), dbname=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'), password=os.getenv('DB_PASSWORD')
    )
    conn.autocommit = True
    cur = conn.cursor()
    # ensure table exists
    cur.execute(
        """
        create table if not exists instrument_meta (
          symbol text primary key,
          exchange text,
          yahoo_symbol text,
          quote_type text,
          asset_type text,
          is_etf boolean,
          is_mutual_fund boolean,
          is_closed_end_fund boolean,
          is_trust boolean,
          is_index boolean,
          category text,
          fund_family text,
          legal_type text,
          currency text,
          underlying_symbol text,
          nav_price numeric,
          expense_ratio numeric,
          total_assets numeric,
          yield numeric,
          ytd_return numeric,
          three_year_avg_return numeric,
          five_year_avg_return numeric,
          beta_3y numeric,
          long_name text,
          attributes jsonb,
          updated_at timestamp default now()
        )
        """
    )
    sql = (
        "insert into instrument_meta (symbol, exchange, yahoo_symbol, quote_type, asset_type, is_etf, is_mutual_fund, is_closed_end_fund, is_trust, is_index, category, fund_family, legal_type, currency, underlying_symbol, nav_price, expense_ratio, total_assets, yield, ytd_return, three_year_avg_return, five_year_avg_return, beta_3y, long_name, attributes) values %s "
        "on conflict (symbol) do update set exchange=excluded.exchange, yahoo_symbol=excluded.yahoo_symbol, quote_type=excluded.quote_type, asset_type=excluded.asset_type, is_etf=excluded.is_etf, is_mutual_fund=excluded.is_mutual_fund, is_closed_end_fund=excluded.is_closed_end_fund, is_trust=excluded.is_trust, is_index=excluded.is_index, category=excluded.category, fund_family=excluded.fund_family, legal_type=excluded.legal_type, currency=excluded.currency, underlying_symbol=excluded.underlying_symbol, nav_price=excluded.nav_price, expense_ratio=excluded.expense_ratio, total_assets=excluded.total_assets, yield=excluded.yield, ytd_return=excluded.ytd_return, three_year_avg_return=excluded.three_year_avg_return, five_year_avg_return=excluded.five_year_avg_return, beta_3y=excluded.beta_3y, long_name=excluded.long_name, attributes=excluded.attributes, updated_at=now()"
    )
    try:
        execute_values(cur, sql, rows)
    except Exception as e:
        log.error("Upsert into instrument_meta failed for batch of %d", len(rows), exc_info=e)
        raise
    cur.close(); conn.close()


def yahoo_symbol_from_symbol(symbol: str) -> str:
    """Convert a TMX-derived symbol (e.g., AAA.P.V or IGBT.TO) to a Yahoo-friendly base symbol.
    - Replace '.' in the root with '-' before the exchange suffix.
    """
    s = str(symbol or '')
    if '.' not in s:
        return s
    try:
        root, ex = s.rsplit('.', 1)
        return root.replace('.', '-') + '.' + ex
    except Exception:
        return s


def yahoo_variants(ysym: str):
    """Generate common Yahoo variants for TSX/TSXV trusts/classes (-UN, -U, -A, -B, -X, -Y)."""
    if '.' not in ysym:
        return []
    try:
        base, ext = ysym.rsplit('.', 1)
        ext = '.' + ext
        return [
            f"{base}-UN{ext}", f"{base}-U{ext}",
            f"{base}-A{ext}", f"{base}-B{ext}",
            f"{base}-X{ext}", f"{base}-Y{ext}",
        ]
    except Exception:
        return []


def main():
    conn = psycopg2.connect(
        host=os.getenv('DB_HOST'), port=os.getenv('DB_PORT', 5432), dbname=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'), password=os.getenv('DB_PASSWORD')
    )
    conn.autocommit = True
    cur = conn.cursor()
    try:
        cur.execute("select symbol, 'CA' as exchange, name from tmx_issuers order by symbol")
    except Exception as e:
        log.error("Failed to read tmx_issuers", exc_info=e)
        raise
    symbols = cur.fetchall()
    cur.close(); conn.close()

    cap = os.getenv('YFIN_MAX_TICKERS')
    if cap:
        try:
            n = int(cap)
            if n > 0:
                symbols = symbols[:n]
        except Exception:
            pass

    rows = []
    processed = 0
    not_found = 0
    start = time.time()
    def ordered_candidates(sym: str, name: str | None):
        ysym = yahoo_symbol_from_symbol(sym)
        nm = (name or '').lower()
        prefer = []
        # REITs/funds/trusts often use unit suffixes first
        if any(k in nm for k in ['reit', 'trust', 'fund']):
            prefer.extend(['-UN', '-U'])
        # Classes
        if 'class b' in nm:
            prefer.append('-B')
        if 'class a' in nm:
            prefer.append('-A')
        # Build unique list of variants
        tail = ['-B', '-A', '-X', '-Y']
        suffixes = []
        seen = set()
        for s in prefer + tail:
            if s not in seen:
                seen.add(s); suffixes.append(s)
        variants = [ysym] + [ysym.rsplit('.',1)[0] + s + '.' + ysym.rsplit('.',1)[1] for s in suffixes]
        return variants

    for (sym, ex, name) in symbols:
        try:
            candidates = ordered_candidates(sym, name)
            ysym = candidates[0]
            info = None
            for cand in candidates:
                try:
                    yt = yf.Ticker(cand)
                    info = yt.get_info() if hasattr(yt, 'get_info') else getattr(yt, 'info', {})
                    if info:
                        ysym = cand
                        break
                except Exception:
                    log.debug("candidate lookup failed", extra={"symbol": sym, "candidate": cand})
                    continue
            if not info:
                not_found += 1
                log.error("ticker not found on Yahoo after variants", extra={"symbol": sym, "issuer_name": name, "candidates": candidates})
            else:
                meta = classify(info or {})
                rows.append((sym, ex, ysym, meta['quote_type'], meta['asset_type'], meta['is_etf'], meta['is_mutual_fund'], meta['is_closed_end_fund'], meta['is_trust'], meta['is_index'], meta['category'], meta['fund_family'], meta['legal_type'], meta['currency'], meta['underlying_symbol'], meta['nav_price'], meta['expense_ratio'], meta['total_assets'], meta['yield'], meta['ytd_return'], meta['three_year_avg_return'], meta['five_year_avg_return'], meta['beta_3y'], meta['long_name'], json.dumps(meta['attributes'] or {})))
        except Exception as e:
            # keep going on individual failures
            log.warning("failed to classify symbol; continuing", extra={"symbol": sym}, exc_info=e)
        processed += 1
        if len(rows) >= 250:
            try:
                upsert_meta(rows)
            except Exception as e:
                log.error("batch upsert failed; dropping batch", exc_info=e)
            
            rows.clear()
        if processed % 250 == 0:
            log.info("progress %s/%s", processed, len(symbols))
    if rows:
        try:
            upsert_meta(rows)
        except Exception as e:
            log.error("final upsert failed; some rows lost", exc_info=e)

    dur = time.time() - start
    log.info("done %s in %.1fs (not_found=%s)", processed, dur, not_found)
    # Treat any not-found as critical failure so Orchestrator aborts early.
    if not_found > 0 and (os.getenv('FAIL_ON_NOT_FOUND', 'true').lower() == 'true'):
        sys.exit(2)


if __name__ == '__main__':
    sys.exit(main())
