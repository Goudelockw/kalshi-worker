"""Hourly candles around earnings calls for KXEARNINGSMENTION markets.

Settled calls: an event's call end is min(markets.close_time) over its settled markets (the
markets close when the call ends). Each market gets period=60 candles for
[call_end - 48h, call_end + 1h]. Markets settled before GET /historical/cutoff's
market_settled_ts go one at a time through /historical/markets/{ticker}/candlesticks; the rest
share one batch call per event through jobs._batch_candles. A market is done once a fetch
succeeded (sync_state 'precall_done:<ticker>') or it already has >= MIN_CANDLES hourly candles
in its window. Newest events first; commits per chunk / per historical market.

Upcoming calls: upcoming_tickers() lists the open KXEARNINGSMENTION markets whose event date
(events.sub_title "On Mon DD, YYYY", else the date in the event ticker) is today or within the
next 2 days in New York; sync() fetches their last 3 hours of hourly candles every run.
"""
from __future__ import annotations

import logging
import re
from datetime import date, datetime, timedelta

from . import db
from .client import KalshiClient
from .jobs import HOUR, OPEN, _batch_candles, _epoch, _now, _stage
from .transcripts import MONTHS, parse_date

log = logging.getLogger(__name__)

SERIES_LIKE = "KXEARNINGSMENTION%"
BEFORE = timedelta(hours=48)
AFTER = timedelta(hours=1)
MIN_CANDLES = 40
STATE_PREFIX = "precall_done:"
UPCOMING_DAYS = 2
TICKER_DATE_RE = re.compile(r"-(\d{2})([A-Z]{3})(\d{2})$")


def _hour_floor(dt: datetime) -> datetime:
    return dt.replace(minute=0, second=0, microsecond=0)


def _hour_ceil(dt: datetime) -> datetime:
    f = _hour_floor(dt)
    return f if f == dt else f + timedelta(hours=1)


# ---------------------------------------------------------------------- settled calls
def events(c, settled_days: int | None = None) -> list[dict]:
    """Settled KXEARNINGSMENTION events, newest call first: {event_ticker, call_end, start,
    end, markets: [{ticker, settled_at}]}. `settled_days` keeps events whose markets settled in
    the last N days."""
    with c.cursor() as cur:
        cur.execute("""SELECT m.event_ticker, m.ticker, m.close_time, coalesce(m.settled_time, m.close_time)
                       FROM markets m
                       WHERE m.series_ticker LIKE %s AND m.result <> '' AND m.close_time IS NOT NULL
                         AND m.event_ticker IS NOT NULL""", (SERIES_LIKE,))
        rows = cur.fetchall()
    by_event: dict[str, dict] = {}
    for ev, ticker, close, settled in rows:
        e = by_event.setdefault(ev, {"event_ticker": ev, "call_end": close, "settled": settled, "markets": []})
        e["call_end"] = min(e["call_end"], close)
        e["settled"] = max(e["settled"], settled)
        e["markets"].append({"ticker": ticker, "settled_at": settled})
    since = _now() - timedelta(days=settled_days) if settled_days is not None else None
    out = []
    for e in by_event.values():
        if since and e["settled"] < since:
            continue
        e["start"] = _hour_floor(e["call_end"] - BEFORE)
        e["end"] = _hour_ceil(e["call_end"] + AFTER)
        out.append(e)
    return sorted(out, key=lambda e: e["call_end"], reverse=True)


def _covered(c, evs: list[dict]) -> set[str]:
    """Tickers already done: a precall_done marker, or >= MIN_CANDLES hourly candles in the window."""
    done = db.states_with_prefix(c, STATE_PREFIX)
    tickers, starts, ends = [], [], []
    for e in evs:
        for m in e["markets"]:
            if m["ticker"] not in done:
                tickers.append(m["ticker"])
                starts.append(e["start"])
                ends.append(e["end"])
    if tickers:
        with c.cursor() as cur:
            cur.execute("""SELECT w.t FROM unnest(%s::text[], %s::timestamptz[], %s::timestamptz[]) AS w(t, s, e)
                           JOIN candles k ON k.ticker = w.t AND k.period_minutes = %s AND k.ts >= w.s AND k.ts <= w.e
                           GROUP BY w.t HAVING count(*) >= %s""", (tickers, starts, ends, HOUR, MIN_CANDLES))
            done |= {r[0] for r in cur.fetchall()}
    return done


def _mark(c, tickers) -> None:
    for t in tickers:
        db.set_state(c, STATE_PREFIX + t, meta={"period": HOUR})


def fill(k: KalshiClient, c, limit: int | None = None, settled_days: int | None = None) -> dict:
    """Fetch the 48h-before / 1h-after hourly window for settled markets not yet done. `limit`
    caps the events looked at. Returns {"markets", "candles", "live", "historical", "failed"}."""
    evs = events(c, settled_days)
    if limit is not None:
        evs = evs[:limit]
    covered = _covered(c, evs)
    cutoff = k.cutoff()
    cutoff_ts = db._ts(cutoff.get("market_settled_ts") or cutoff.get("settled_ts"))
    todo = [(e, [m for m in e["markets"] if m["ticker"] not in covered]) for e in evs]
    todo = [(e, ms) for e, ms in todo if ms]
    n_live = sum(1 for _, ms in todo for m in ms if not (cutoff_ts and m["settled_at"] < cutoff_ts))
    n_hist = sum(len(ms) for _, ms in todo) - n_live
    log.info("precall: %d settled events%s, %d markets to fetch (%d live batch, %d historical), %d already done",
             len(evs), f" (settled in the last {settled_days} days)" if settled_days is not None else "",
             n_live + n_hist, n_live, n_hist, sum(len(e["markets"]) for e in evs) - n_live - n_hist)
    stats = {"markets": 0, "candles": 0, "live": 0, "historical": 0, "failed": 0}

    def done(chunk):
        _mark(c, chunk)
        stats["markets"] += len(chunk)
        stats["live"] += len(chunk)

    with _stage("precall", f"hourly windows ({len(todo)} events)"):
        for e, ms in todo:
            s, end = _epoch(e["start"]), _epoch(min(e["end"], _now()))
            live = [m["ticker"] for m in ms if not (cutoff_ts and m["settled_at"] < cutoff_ts)]
            hist = [m["ticker"] for m in ms if cutoff_ts and m["settled_at"] < cutoff_ts]
            if live:
                before = stats["live"]
                stats["candles"] += _batch_candles(k, c, live, HOUR, s, end, on_done=done)
                stats["failed"] += len(live) - (stats["live"] - before)
            for t in hist:
                try:
                    candles = k.candlesticks(t, s, end, HOUR, historical=True)
                    stats["candles"] += db.insert_candles(c, t, HOUR, candles) if candles else 0
                    _mark(c, [t])
                    c.commit()
                    stats["markets"] += 1
                    stats["historical"] += 1
                except Exception as ex:  # noqa: BLE001
                    c.rollback()
                    stats["failed"] += 1
                    log.warning("precall: historical candles failed for %s: %s", t, ex)
    log.info("precall: done: %d markets (%d live, %d historical), %d candles written, %d failed",
             stats["markets"], stats["live"], stats["historical"], stats["candles"], stats["failed"])
    return stats


def run(k: KalshiClient, c, limit: int | None = None) -> None:
    with db.run_log(c, "precall") as stats:
        stats["rows"] += fill(k, c, limit=limit)["candles"]


# --------------------------------------------------------------------- upcoming calls
def ticker_date(event_ticker: str | None) -> date | None:
    """'KXEARNINGSMENTIONAAPL-26JUL30' -> 2026-07-30."""
    m = TICKER_DATE_RE.search(event_ticker or "")
    if not m or m.group(2).lower() not in MONTHS:
        return None
    try:
        return date(2000 + int(m.group(1)), MONTHS[m.group(2).lower()], int(m.group(3)))
    except ValueError:
        return None


def event_date(sub_title: str | None, event_ticker: str | None) -> date | None:
    """The call date: events.sub_title ('On Jul 30, 2026'), else the date in the event ticker."""
    return parse_date(sub_title) or ticker_date(event_ticker)


def upcoming_tickers(c, days: int = UPCOMING_DAYS) -> list[str]:
    """Open KXEARNINGSMENTION markets whose call is today or within the next `days` days (New York)."""
    with c.cursor() as cur:
        cur.execute("SELECT (now() AT TIME ZONE 'America/New_York')::date")
        today = cur.fetchone()[0]
        cur.execute(f"""SELECT m.ticker, m.event_ticker, e.sub_title FROM markets m
                        LEFT JOIN events e ON e.event_ticker = m.event_ticker
                        WHERE m.series_ticker LIKE %s AND m.{OPEN}""", (SERIES_LIKE,))
        rows = cur.fetchall()
    out = []
    for ticker, ev, sub in rows:
        d = event_date(sub, ev)
        if d and today <= d <= today + timedelta(days=days):
            out.append(ticker)
    return sorted(out)
