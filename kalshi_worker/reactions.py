"""1-minute candles around 8-K earnings press releases.

For every filing in kalshi.filings filed in the last N days, take the company's
KXEARNINGSMENTION<symbol> markets that close within 36 hours after filed_at and store
1-minute candles from filed_at - 3 hours to close_time + 5 minutes. Live-tier markets go
through the batch candlestick endpoint; markets settled before the historical cutoff go one
at a time through /historical/markets/{ticker}/candlesticks. kalshi.v_release_reaction reads
the result.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta

from . import db
from .client import KalshiClient
from .jobs import MINUTE, _batch_candles, _epoch, _now, _stage

log = logging.getLogger(__name__)

DEFAULT_DAYS = 3
CLOSE_WITHIN = timedelta(hours=36)
BEFORE = timedelta(hours=3)
AFTER_CLOSE = timedelta(minutes=5)
COVER_SLACK = timedelta(minutes=2)
HISTORICAL_CHUNK = 5000          # candles per historical call
STATE_PREFIX = "reaction_candles:"


def _floor_minute(dt: datetime) -> datetime:
    return dt.replace(second=0, microsecond=0)


def _ceil_minute(dt: datetime) -> datetime:
    f = _floor_minute(dt)
    return f if f == dt else f + timedelta(minutes=1)


# ------------------------------------------------------------------------------ db
def _pairs(c, days: int) -> list[dict]:
    """(filing, market) pairs: markets in the filer's KXEARNINGSMENTION series closing within
    36 hours after filed_at, for filings from the last `days` days."""
    with c.cursor() as cur:
        cur.execute("""
            SELECT f.accession, f.symbol, f.filed_at, m.ticker, m.series_ticker, m.close_time, m.settled_time
            FROM filings f
            JOIN markets m ON m.series_ticker = 'KXEARNINGSMENTION' || f.symbol
             AND m.close_time >= f.filed_at AND m.close_time <= f.filed_at + %s
            WHERE f.filed_at >= now() - %s
            ORDER BY f.filed_at DESC, m.ticker""", (CLOSE_WITHIN, timedelta(days=days)))
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


def windows(pairs: list[dict], now: datetime) -> dict[str, dict]:
    """{ticker: {start, end, complete, settled_time, close_time}}: filed_at - 3h (one extra
    minute so the candle ending exactly at -3h is included) to close_time + 5min, capped at now.
    A ticker paired with more than one filing gets the union of their windows. `complete` is
    False while close_time + 5min is still in the future."""
    out: dict[str, dict] = {}
    for p in pairs:
        start = _floor_minute(p["filed_at"] - BEFORE) - timedelta(minutes=1)
        want_end = _ceil_minute(p["close_time"] + AFTER_CLOSE)
        w = out.get(p["ticker"])
        if w:
            start, want_end = min(start, w["start"]), max(want_end, w["want_end"])
        out[p["ticker"]] = {"start": start, "want_end": want_end, "end": min(want_end, _floor_minute(now)),
                            "complete": want_end <= now, "settled_time": p["settled_time"],
                            "close_time": p["close_time"]}
    return out


def _covered(c, wins: dict[str, dict]) -> set[str]:
    """Tickers whose window is already done: a completed-fetch marker in sync_state that spans
    the window, or stored 1-minute candles reaching both ends of it (within two minutes)."""
    if not wins:
        return set()
    tickers = list(wins)
    done: set[str] = set()
    with c.cursor() as cur:
        cur.execute("SELECT job, meta FROM sync_state WHERE job = ANY(%s)", ([STATE_PREFIX + t for t in tickers],))
        for job, meta in cur.fetchall():
            t, w = job[len(STATE_PREFIX):], wins.get(job[len(STATE_PREFIX):])
            if w and w["complete"] and meta and meta.get("start", 1 << 62) <= _epoch(w["start"]) \
                    and meta.get("end", 0) >= _epoch(w["end"]):
                done.add(t)
        cur.execute("""
            SELECT w.t, min(c.ts), max(c.ts)
            FROM unnest(%s::text[], %s::timestamptz[], %s::timestamptz[]) AS w(t, s, e)
            JOIN candles c ON c.ticker = w.t AND c.period_minutes = 1 AND c.ts >= w.s AND c.ts <= w.e
            GROUP BY w.t""", (tickers, [wins[t]["start"] for t in tickers], [wins[t]["end"] for t in tickers]))
        for t, lo, hi in cur.fetchall():
            w = wins[t]
            if w["complete"] and lo <= w["start"] + COVER_SLACK and hi >= w["end"] - COVER_SLACK:
                done.add(t)
    return done


def _mark(c, tickers, wins: dict[str, dict]) -> None:
    """Record a completed fetch so sparse markets (few candles) are not re-fetched every run."""
    for t in tickers:
        w = wins[t]
        if w["complete"]:
            db.set_state(c, STATE_PREFIX + t, meta={"start": _epoch(w["start"]), "end": _epoch(w["end"])})


def _historical(k: KalshiClient, c, ticker: str, start: int, end: int) -> int:
    n, step = 0, MINUTE * 60 * HISTORICAL_CHUNK
    for s in range(start, end, step):
        candles = k.candlesticks(ticker, s, min(s + step, end), MINUTE, historical=True)
        if candles:
            n += db.insert_candles(c, ticker, MINUTE, candles)
    return n


# ------------------------------------------------------------------------------ job
def run(k: KalshiClient, c, days: int = DEFAULT_DAYS) -> None:
    with db.run_log(c, "reactions") as stats:
        now = _now()
        pairs = _pairs(c, days)
        wins = windows(pairs, now)
        covered = _covered(c, wins)
        todo = {t: w for t, w in wins.items() if t not in covered}
        cutoff = k.cutoff()
        cutoff_ts = db._ts(cutoff.get("market_settled_ts") or cutoff.get("settled_ts"))

        def old(w: dict) -> bool:
            end = w["settled_time"] or w["close_time"]
            return bool(cutoff_ts and end and end < cutoff_ts)

        live = {t: w for t, w in todo.items() if not old(w)}
        historical = {t: w for t, w in todo.items() if old(w)}
        log.info("reactions (last %d days): %d filings, %d markets closing within 36h, %d already covered; "
                 "%d to fetch (%d batch, %d historical)", days, len({p["accession"] for p in pairs}), len(wins),
                 len(covered), len(todo), len(live), len(historical))

        groups: dict[tuple[int, int], list[str]] = {}
        for t, w in live.items():
            groups.setdefault((_epoch(w["start"]), _epoch(w["end"])), []).append(t)
        with _stage("reactions", f"batch 1-minute candles ({len(live)} markets, {len(groups)} windows)"):
            for (s, e), tickers in groups.items():
                stats["rows"] += _batch_candles(k, c, tickers, MINUTE, s, e, on_done=lambda chunk: _mark(c, chunk, wins))

        failed = 0
        with _stage("reactions", f"historical 1-minute candles ({len(historical)} markets)"):
            for t, w in historical.items():
                try:
                    stats["rows"] += _historical(k, c, t, _epoch(w["start"]), _epoch(w["end"]))
                    _mark(c, [t], wins)
                    c.commit()
                except Exception as e:  # noqa: BLE001
                    c.rollback()
                    failed += 1
                    log.warning("reactions: historical candles failed for %s: %s", t, e)

        incomplete = sum(1 for w in todo.values() if not w["complete"])
        log.info("reactions: done; %d candles written, %d historical failures, %d markets still open "
                 "(re-fetched next run)", stats["rows"], failed, incomplete)
