"""Ingestion jobs. Each is idempotent and resumable via sync_state."""
from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta, timezone

from . import db
from .client import KalshiClient

log = logging.getLogger(__name__)
UTC = timezone.utc
DAY, HOUR, MINUTE = 1440, 60, 1


def _now() -> datetime:
    return datetime.now(UTC)


def _epoch(dt: datetime) -> int:
    return int(dt.timestamp())


# --------------------------------------------------------------------------- reference
def sync_reference(k: KalshiClient, c) -> int:
    """Series + events. Small; safe to do fully every run."""
    n = 0
    for page, _ in k.series():
        n += db.upsert_series(c, page)
    c.commit()
    for page, _ in k.events(with_nested_markets=False):
        n += db.upsert_events(c, page)
    c.commit()
    return n


# ---------------------------------------------------------------------------- candles
def _candle_windows(m: dict, period: int, since: datetime | None) -> tuple[int, int]:
    start = since or m["open_time"] or (m["close_time"] - timedelta(days=90))
    end = m["settled_time"] or m["close_time"] or _now()
    end = min(end, _now())
    return _epoch(start), _epoch(end)


def load_candles(k: KalshiClient, c, m: dict, period: int, historical: bool, since: datetime | None = None) -> int:
    start, end = _candle_windows(m, period, since)
    if start >= end:
        return 0
    # Kalshi caps returned candles per call; walk in chunks (5000 candles per chunk).
    step = period * 60 * 5000
    n = 0
    for s in range(start, end, step):
        e = min(s + step, end)
        candles = k.candlesticks(m["ticker"], s, e, period, historical=historical, series_ticker=m["series_ticker"])
        if candles:
            n += db.insert_candles(c, m["ticker"], period, candles)
    return n


def _market_rows(c, where: str, params: tuple = ()) -> list[dict]:
    with c.cursor() as cur:
        cur.execute(f"SELECT ticker, series_ticker, open_time, close_time, settled_time, status FROM markets WHERE {where}", params)
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


# --------------------------------------------------------------------------- backfill
def backfill(k: KalshiClient, c, with_candles: bool = True) -> None:
    """One-time: every settled market from the historical tier, then daily candles for each.
    Resumable: cursor lives in sync_state('backfill_markets'); per-ticker candle progress in
    sync_state('backfill_candles').meta['done_through'] (ticker ordering)."""
    with db.run_log(c, "backfill") as stats:
        sync_reference(k, c)

        st = db.get_state(c, "backfill_markets")
        if st["meta"].get("complete"):
            log.info("historical markets already complete; skipping")
        else:
            cursor = st["cursor"]
            for page, nxt in k.historical_markets(cursor=cursor):
                stats["rows"] += db.upsert_markets(c, page)
                db.set_state(c, "backfill_markets", cursor=nxt)
                c.commit()
                log.info("historical markets: +%d (total %d)", len(page), stats["rows"])
            # live tier settled markets too (recent 3 months)
            for page, _ in k.markets(status="settled"):
                stats["rows"] += db.upsert_markets(c, page)
                c.commit()
            db.set_state(c, "backfill_markets", cursor=None, meta={"complete": True})
            c.commit()

        if not with_candles:
            return
        cutoff = k.cutoff()
        cutoff_ts = db._ts(cutoff.get("market_settled_ts") or cutoff.get("settled_ts"))
        st = db.get_state(c, "backfill_candles")
        done_through = st["meta"].get("done_through", "")
        rows = _market_rows(c, "result <> '' AND ticker > %s ORDER BY ticker", (done_through,))
        log.info("backfilling daily candles for %d settled markets", len(rows))
        for i, m in enumerate(rows, 1):
            historical = bool(cutoff_ts and m["settled_time"] and m["settled_time"] < cutoff_ts)
            try:
                stats["rows"] += load_candles(k, c, m, DAY, historical)
            except Exception as e:  # noqa: BLE001
                log.warning("candles failed for %s: %s", m["ticker"], e)
            if i % 50 == 0:
                db.set_state(c, "backfill_candles", meta={"done_through": m["ticker"]})
                c.commit()
                log.info("candles %d/%d", i, len(rows))
        db.set_state(c, "backfill_candles", meta={"done_through": "~", "complete": True})
        c.commit()


# ------------------------------------------------------------------------------- sync
def sync(k: KalshiClient, c) -> None:
    """Hourly: refresh open markets, newly created/closed ones, hourly candles for open
    markets, and trades for watchlisted series."""
    with db.run_log(c, "sync") as stats:
        sync_reference(k, c)
        for status in ("open", "unopened", "closed"):
            for page, _ in k.markets(status=status):
                stats["rows"] += db.upsert_markets(c, page)
                c.commit()
        # markets settled since last sync
        st = db.get_state(c, "sync_settled")
        since = st["watermark"] or (_now() - timedelta(days=2))
        for page, _ in k.markets(status="settled", min_settled_ts=_epoch(since)):
            stats["rows"] += db.upsert_markets(c, page)
            c.commit()
        db.set_state(c, "sync_settled", watermark=_now() - timedelta(hours=1))
        c.commit()

        # hourly candles for everything open, last 3 hours (overlap is fine: upsert)
        for m in _market_rows(c, "status IN ('active','initialized','inactive')"):
            try:
                stats["rows"] += load_candles(k, c, m, HOUR, False, since=_now() - timedelta(hours=3))
            except Exception as e:  # noqa: BLE001
                log.warning("hourly candles failed for %s: %s", m["ticker"], e)
        c.commit()

        # watchlist extras
        for w in db.watchlist(c):
            if w["minute_candles"]:
                for m in _market_rows(c, "series_ticker=%s AND status IN ('active','initialized','inactive')", (w["series_ticker"],)):
                    stats["rows"] += load_candles(k, c, m, MINUTE, False, since=_now() - timedelta(hours=2))
            if w["trades"]:
                for m in _market_rows(c, "series_ticker=%s AND status IN ('active','initialized','inactive','closed')", (w["series_ticker"],)):
                    job = f"trades:{m['ticker']}"
                    st = db.get_state(c, job)
                    min_ts = _epoch(st["watermark"]) if st["watermark"] else None
                    for page, nxt in k.trades(ticker=m["ticker"], min_ts=min_ts):
                        stats["rows"] += db.insert_trades(c, page)
                    db.set_state(c, job, watermark=_now() - timedelta(minutes=10))
            c.commit()


# --------------------------------------------------------------------------- snapshot
def snapshot(k: KalshiClient, c, interval_min: int = 5) -> None:
    """Order book depth for open markets in watchlisted series. Forward-only data."""
    ts = _now().replace(second=0, microsecond=0)
    ts -= timedelta(minutes=ts.minute % interval_min)
    with db.run_log(c, "snapshot") as stats:
        series = [w["series_ticker"] for w in db.watchlist(c) if w["book_snapshots"]]
        if not series:
            return
        rows = _market_rows(c, "series_ticker = ANY(%s) AND status IN ('active','initialized','inactive')", (series,))
        for m in rows:
            try:
                stats["rows"] += db.insert_book(c, m["ticker"], ts, k.orderbook(m["ticker"]))
            except Exception as e:  # noqa: BLE001
                log.warning("book failed for %s: %s", m["ticker"], e)
        c.commit()


# --------------------------------------------------------------------------- reconcile
def reconcile(k: KalshiClient, c) -> None:
    """Nightly: lock in results for anything settled in the last 3 days and finish its
    daily + hourly candles through settlement."""
    with db.run_log(c, "reconcile") as stats:
        since = _now() - timedelta(days=3)
        for page, _ in k.markets(status="settled", min_settled_ts=_epoch(since)):
            stats["rows"] += db.upsert_markets(c, page)
        c.commit()
        for m in _market_rows(c, "settled_time >= %s", (since,)):
            for period in (DAY, HOUR):
                try:
                    stats["rows"] += load_candles(k, c, m, period, False)
                except Exception as e:  # noqa: BLE001
                    log.warning("reconcile candles failed for %s/%d: %s", m["ticker"], period, e)
        c.commit()
