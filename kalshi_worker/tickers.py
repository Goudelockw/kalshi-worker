"""Kalshi earnings-mention symbols vs exchange tickers.

A KXEARNINGSMENTION<symbol> series symbol is usually the company's ticker, but not always
(KXEARNINGSMENTIONADOBE is ADBE, ARITZIA trades as ATZ on the TSX). company_map.ticker holds
the ticker per Kalshi symbol; TICKER_ALIASES seeds it (filings fills company_map) and is the
fallback for symbols without a row yet. Lookups against outside sources (Fool sitemap slugs,
Fortune pages, Equibles paths, SEC) use the ticker; rows are always stored under the Kalshi
symbol.
"""
from __future__ import annotations

TICKER_ALIASES = {
    "ADOBE": "ADBE", "COINBASE": "COIN", "ALIBABA": "BABA", "TSMC": "TSM", "DOLLARGENERAL": "DG",
    "LUCID": "LCID", "ARITZIA": "ATZ", "AC": "AC",
}
TSX_ONLY = {"ARITZIA": "Aritzia Inc.", "AC": "Air Canada"}   # no SEC filer: cik stays null


def alias(symbol: str) -> str:
    return TICKER_ALIASES.get(symbol, symbol)


def mention_symbols(c) -> list[str]:
    """Every Kalshi symbol with a KXEARNINGSMENTION<symbol> series."""
    with c.cursor() as cur:
        cur.execute("""SELECT DISTINCT substring(series_ticker FROM '^KXEARNINGSMENTION(.+)$')
                       FROM markets WHERE series_ticker LIKE 'KXEARNINGSMENTION%'""")
        return sorted(r[0] for r in cur.fetchall() if r[0])


def mention_tickers(c) -> dict[str, str]:
    """{Kalshi symbol: ticker} for every tracked symbol: company_map.ticker, else the alias."""
    symbols = mention_symbols(c)
    with c.cursor() as cur:
        cur.execute("SELECT symbol, ticker FROM company_map WHERE symbol = ANY(%s) AND ticker IS NOT NULL", (symbols,))
        known = dict(cur.fetchall())
    return {s: known.get(s) or alias(s) for s in symbols}


def canonical(sym_tickers: dict[str, str]) -> dict[str, str]:
    """One Kalshi symbol per ticker, for fetching a company's documents once: the symbol equal to
    the ticker when there is one (ADBE over ADOBE), else the first alphabetically."""
    by_ticker: dict[str, list[str]] = {}
    for s, t in sym_tickers.items():
        by_ticker.setdefault(t, []).append(s)
    out = {}
    for t, syms in by_ticker.items():
        s = t if t in syms else sorted(syms)[0]
        out[s] = t
    return out
