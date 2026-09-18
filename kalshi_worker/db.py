"""Postgres access. One connection per job run (Neon pooled endpoint)."""
from __future__ import annotations

import json
import os
import socket
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Iterable
from urllib.parse import urlparse

import psycopg
from psycopg.types.json import Jsonb

DATABASE_URL = os.environ["DATABASE_URL"]


def _ipv4_hostaddr(url: str) -> str | None:
    host = urlparse(url).hostname
    try:
        infos = socket.getaddrinfo(host, 5432, socket.AF_INET, socket.SOCK_STREAM)
        return infos[0][4][0] if infos else None
    except socket.gaierror:
        return None


@contextmanager
def conn():
    kwargs = {"autocommit": False}
    if (addr := _ipv4_hostaddr(DATABASE_URL)):
        kwargs["hostaddr"] = addr          # force IPv4; SNI/TLS still uses the hostname
    with psycopg.connect(DATABASE_URL, **kwargs) as c:
        with c.cursor() as cur:
            cur.execute("SET search_path TO kalshi, public")
        yield c


# ---------------------------------------------------------------- helpers
def _ts(v: Any) -> datetime | None:
    if v in (None, ""):
        return None
    if isinstance(v, (int, float)):
        return datetime.fromtimestamp(v, tz=timezone.utc)
    return datetime.fromisoformat(str(v).replace("Z", "+00:00"))


def _num(v: Any) -> str | None:
    """Fixed-point strings pass straight through to numeric columns."""
    return None if v in (None, "") else str(v)


# --------------------------------------------------------------- reference
def upsert_series(c, rows: Iterable[dict]) -> int:
    sql = """
    INSERT INTO series (ticker, title, category, frequency, settlement_sources, tags, raw, updated_at)
    VALUES (%s,%s,%s,%s,%s,%s,%s,now())
    ON CONFLICT (ticker) DO UPDATE SET title=EXCLUDED.title, category=EXCLUDED.category,
      frequency=EXCLUDED.frequency, settlement_sources=EXCLUDED.settlement_sources,
      tags=EXCLUDED.tags, raw=EXCLUDED.raw, updated_at=now()"""
    params = [(s["ticker"], s.get("title"), s.get("category"), s.get("frequency"),
               Jsonb(s.get("settlement_sources")), s.get("tags"), Jsonb(s)) for s in rows]
    if params:
        with c.cursor() as cur:
            cur.executemany(sql, params)
    return len(params)


def upsert_events(c, rows: Iterable[dict]) -> int:
    sql = """
    INSERT INTO events (event_ticker, series_ticker, title, sub_title, category, mutually_exclusive,
                        strike_date, strike_period, raw, updated_at)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,now())
    ON CONFLICT (event_ticker) DO UPDATE SET series_ticker=EXCLUDED.series_ticker, title=EXCLUDED.title,
      sub_title=EXCLUDED.sub_title, category=EXCLUDED.category, mutually_exclusive=EXCLUDED.mutually_exclusive,
      strike_date=EXCLUDED.strike_date, strike_period=EXCLUDED.strike_period, raw=EXCLUDED.raw, updated_at=now()"""
    params = [(e["event_ticker"], e.get("series_ticker"), e.get("title"), e.get("sub_title"),
               e.get("category"), e.get("mutually_exclusive"), _ts(e.get("strike_date")),
               e.get("strike_period"), Jsonb(e)) for e in rows]
    if params:
        with c.cursor() as cur:
            cur.executemany(sql, params)
    return len(params)


def upsert_markets(c, rows: Iterable[dict]) -> int:
    sql = """
    INSERT INTO markets (ticker, event_ticker, series_ticker, market_type, title, subtitle, yes_sub_title,
        no_sub_title, rules_primary, rules_secondary, status, result, created_time, updated_time, open_time,
        close_time, expiration_time, expected_expiration_time, latest_expiration_time, settled_time,
        can_close_early, strike_type, floor_strike, cap_strike, yes_bid, yes_ask, no_bid, no_ask, last_price,
        volume, open_interest, settlement_value, expiration_value, raw, updated_at)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,now())
    ON CONFLICT (ticker) DO UPDATE SET
      event_ticker=EXCLUDED.event_ticker, series_ticker=COALESCE(EXCLUDED.series_ticker, markets.series_ticker),
      market_type=EXCLUDED.market_type, title=EXCLUDED.title, subtitle=EXCLUDED.subtitle,
      yes_sub_title=EXCLUDED.yes_sub_title, no_sub_title=EXCLUDED.no_sub_title,
      rules_primary=EXCLUDED.rules_primary, rules_secondary=EXCLUDED.rules_secondary,
      status=EXCLUDED.status, result=EXCLUDED.result, created_time=EXCLUDED.created_time,
      updated_time=EXCLUDED.updated_time, open_time=EXCLUDED.open_time, close_time=EXCLUDED.close_time,
      expiration_time=EXCLUDED.expiration_time, expected_expiration_time=EXCLUDED.expected_expiration_time,
      latest_expiration_time=EXCLUDED.latest_expiration_time, settled_time=EXCLUDED.settled_time,
      can_close_early=EXCLUDED.can_close_early, strike_type=EXCLUDED.strike_type,
      floor_strike=EXCLUDED.floor_strike, cap_strike=EXCLUDED.cap_strike,
      yes_bid=EXCLUDED.yes_bid, yes_ask=EXCLUDED.yes_ask, no_bid=EXCLUDED.no_bid, no_ask=EXCLUDED.no_ask,
      last_price=EXCLUDED.last_price, volume=EXCLUDED.volume, open_interest=EXCLUDED.open_interest,
      settlement_value=EXCLUDED.settlement_value, expiration_value=EXCLUDED.expiration_value,
      raw=EXCLUDED.raw, updated_at=now()"""
    params = []
    for m in rows:
        if m["ticker"].startswith("KXMVE"):
            continue
        ev = m.get("event_ticker") or ""
        series = m.get("series_ticker") or (ev.rsplit("-", 1)[0] if "-" in ev else None)
        params.append((
            m["ticker"], ev or None, series, m.get("market_type"), m.get("title"), m.get("subtitle"),
            m.get("yes_sub_title"), m.get("no_sub_title"), m.get("rules_primary"), m.get("rules_secondary"),
            m.get("status"), m.get("result", ""), _ts(m.get("created_time")), _ts(m.get("updated_time")),
            _ts(m.get("open_time")), _ts(m.get("close_time")), _ts(m.get("expiration_time")),
            _ts(m.get("expected_expiration_time")), _ts(m.get("latest_expiration_time")),
            _ts(m.get("settlement_ts")), m.get("can_close_early"), m.get("strike_type"),
            m.get("floor_strike"), m.get("cap_strike"),
            _num(m.get("yes_bid_dollars")), _num(m.get("yes_ask_dollars")),
            _num(m.get("no_bid_dollars")), _num(m.get("no_ask_dollars")), _num(m.get("last_price_dollars")),
            _num(m.get("volume_fp")), _num(m.get("open_interest_fp")),
            _num(m.get("settlement_value_dollars")), m.get("expiration_value"), Jsonb(m)))
    if params:
        with c.cursor() as cur:
            cur.executemany(sql, params)
    return len(params)


# ------------------------------------------------------------- time series
def _d(obj: dict | None, key: str) -> str | None:
    """Pull `<key>_dollars` (preferred) or `<key>` from a candle sub-object."""
    if not obj:
        return None
    return _num(obj.get(f"{key}_dollars", obj.get(key)))


def insert_candles(c, ticker: str, period: int, candles: Iterable[dict]) -> int:
    sql = """
    INSERT INTO candles (ticker, period_minutes, ts, price_open, price_high, price_low, price_close, price_mean,
                         yes_bid_close, yes_ask_close, volume, open_interest, raw)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (ticker, period_minutes, ts) DO UPDATE SET
      price_open=EXCLUDED.price_open, price_high=EXCLUDED.price_high, price_low=EXCLUDED.price_low,
      price_close=EXCLUDED.price_close, price_mean=EXCLUDED.price_mean, yes_bid_close=EXCLUDED.yes_bid_close,
      yes_ask_close=EXCLUDED.yes_ask_close, volume=EXCLUDED.volume, open_interest=EXCLUDED.open_interest,
      raw=EXCLUDED.raw"""
    params = []
    for k in candles:
        p, yb, ya = k.get("price") or {}, k.get("yes_bid") or {}, k.get("yes_ask") or {}
        params.append((ticker, period, _ts(k["end_period_ts"]), _d(p, "open"), _d(p, "high"),
                       _d(p, "low"), _d(p, "close"), _d(p, "mean"), _d(yb, "close"), _d(ya, "close"),
                       _num(k.get("volume_fp", k.get("volume"))),
                       _num(k.get("open_interest_fp", k.get("open_interest"))), Jsonb(k)))
    if params:
        with c.cursor() as cur:
            cur.executemany(sql, params)
    return len(params)


def insert_trades(c, trades: Iterable[dict]) -> int:
    sql = """
    INSERT INTO trades (trade_id, ticker, ts, yes_price, no_price, count, taker_side, is_block_trade, raw)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (trade_id) DO NOTHING"""
    params = [(t["trade_id"], t["ticker"], _ts(t.get("created_time")),
               _num(t.get("yes_price_dollars")), _num(t.get("no_price_dollars")),
               _num(t.get("count_fp", t.get("count"))), t.get("taker_side"),
               t.get("is_block_trade"), Jsonb(t)) for t in trades]
    if params:
        with c.cursor() as cur:
            cur.executemany(sql, params)
    return len(params)


def insert_book(c, ticker: str, ts: datetime, book: dict) -> int:
    """Kalshi returns yes bids and no bids as [[price, count], ...]. Prefer *_dollars keys."""
    sql = """INSERT INTO book_snapshots (ticker, ts, side, price, quantity)
             VALUES (%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING"""
    params = []
    for side in ("yes", "no"):
        levels = book.get(f"{side}_dollars") or book.get(side) or []
        for level in levels:
            price, qty = level[0], level[1]
            params.append((ticker, ts, side, _num(price), _num(qty)))
    if params:
        with c.cursor() as cur:
            cur.executemany(sql, params)
    return len(params)


# --------------------------------------------------------------- bookkeeping
def get_state(c, job: str) -> dict:
    with c.cursor() as cur:
        cur.execute("SELECT cursor, watermark, meta FROM sync_state WHERE job=%s", (job,))
        row = cur.fetchone()
    return {"cursor": row[0], "watermark": row[1], "meta": row[2] or {}} if row else {"cursor": None, "watermark": None, "meta": {}}


def set_state(c, job: str, cursor: str | None = None, watermark: datetime | None = None, meta: dict | None = None) -> None:
    with c.cursor() as cur:
        cur.execute("""INSERT INTO sync_state (job, cursor, watermark, meta, updated_at)
                       VALUES (%s,%s,%s,%s,now())
                       ON CONFLICT (job) DO UPDATE SET cursor=EXCLUDED.cursor, watermark=EXCLUDED.watermark,
                         meta=COALESCE(EXCLUDED.meta, sync_state.meta), updated_at=now()""",
                    (job, cursor, watermark, Jsonb(meta) if meta is not None else None))


def watchlist(c) -> list[dict]:
    with c.cursor() as cur:
        cur.execute("SELECT series_ticker, trades, minute_candles, book_snapshots FROM watchlist")
        return [dict(series_ticker=r[0], trades=r[1], minute_candles=r[2], book_snapshots=r[3]) for r in cur.fetchall()]


@contextmanager
def run_log(c, job: str):
    """Heartbeat row per run. Commit happens regardless of the job's outcome."""
    with c.cursor() as cur:
        cur.execute("INSERT INTO ingest_runs (job) VALUES (%s) RETURNING id", (job,))
        run_id = cur.fetchone()[0]
    c.commit()
    stats = {"rows": 0}
    try:
        yield stats
        with c.cursor() as cur:
            cur.execute("UPDATE ingest_runs SET finished_at=now(), ok=true, rows_written=%s WHERE id=%s",
                        (stats["rows"], run_id))
        c.commit()
    except Exception as e:  # noqa: BLE001
        c.rollback()
        with c.cursor() as cur:
            cur.execute("UPDATE ingest_runs SET finished_at=now(), ok=false, rows_written=%s, error=%s WHERE id=%s",
                        (stats["rows"], str(e)[:2000], run_id))
        c.commit()
        raise
