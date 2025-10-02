from __future__ import annotations

import os
from typing import Dict, List, Tuple, Optional

import psycopg2


def tmx_symbol(root: str, exchange: str) -> str:
    root = (root or '').strip().upper()
    ex = (exchange or '').strip().upper()
    if ex in ('TSX', 'TSX-MKT', 'TORONTO'):
        return f"{root}.TO"
    if ex in ('TSXV', 'TSX-V', 'VENTURE'):
        return f"{root}.V"
    if ex in ('CSE', 'CN', 'CANADIAN SECURITIES EXCHANGE'):
        return f"{root}.CN"
    if ex in ('NEO', 'NEO-L', 'NEO EXCHANGE'):
        return f"{root}.NE"
    return root


def yahoo_base_from_symbol(symbol: str) -> str:
    s = str(symbol or '')
    if '.' not in s:
        return s
    try:
        root, ex = s.rsplit('.', 1)
        return root.replace('.', '-') + '.' + ex
    except Exception:
        return s


def yahoo_variants_all(ysym: str, name_hint: Optional[str] = None) -> List[str]:
    if '.' not in ysym:
        return []
    base, ext = ysym.rsplit('.', 1)
    ext = '.' + ext
    prefer: List[str] = []
    nm = (name_hint or '').lower()
    if any(k in nm for k in ['reit', 'trust', 'fund']):
        prefer.extend(['-UN', '-U'])
    if 'class b' in nm:
        prefer.append('-B')
    if 'class a' in nm:
        prefer.append('-A')
    tail = ['-B', '-A', '-X', '-Y']
    suffixes: List[str] = []
    seen = set()
    for s in prefer + tail:
        if s not in seen:
            seen.add(s)
            suffixes.append(s)
    return [f"{base}{s}{ext}" for s in suffixes]


def load_instrument_meta_map() -> Dict[str, str]:
    """Load mapping from instrument_meta.symbol (e.g., AGF.TO) to yahoo_symbol (e.g., AGF-B.TO)."""
    conn = psycopg2.connect(
        host=os.getenv('DB_HOST'), port=os.getenv('DB_PORT', 5432), dbname=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'), password=os.getenv('DB_PASSWORD')
    )
    cur = conn.cursor()
    cur.execute("select symbol, yahoo_symbol from instrument_meta where yahoo_symbol is not null")
    rows = cur.fetchall()
    cur.close(); conn.close()
    return {sym: ysym for (sym, ysym) in rows}
