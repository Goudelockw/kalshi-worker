"""Ingestion jobs. Each is idempotent and resumable via sync_state."""
from __future__ import annotations

import logging
import time
from contextlib import contextmanager
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


@contextmanager
def _stage(job: str, name: str):
    t0 = time.monotonic()
    yield
    log.info("%s: %s done in %.1fs", job, name, time.monotonic() - t0)


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
        rows = _market_rows(c, "result <> '' AND ticker > %s AND ticker NOT LIKE %s ORDER BY ticker",
                            (done_through, "KXMVE%"))
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
OPEN = "status IN ('active','initialized','inactive')"


def _sync_events(k: KalshiClient, c, **params) -> int:
    """Page /events with nested markets; upsert each event row, then its markets with
    series_ticker/event_ticker taken from the parent. Returns rows written."""
    n = 0
    for page, _ in k.events(with_nested_markets="true", **params):
        events, markets = [], []
        for ev in page:
            nested = ev.pop("markets", None) or []
            for m in nested:
                m["event_ticker"] = ev["event_ticker"]
                m["series_ticker"] = ev.get("series_ticker")
            events.append(ev)
            markets.extend(nested)
        n += db.upsert_events(c, events)
        n += db.upsert_markets(c, markets)  # drops KXMVE* tickers
        c.commit()
    return n


def _open_markets(c, series: list[str]) -> list[dict]:
    return _market_rows(c, f"series_ticker = ANY(%s) AND {OPEN}", (series,))


def _batch_candles(k: KalshiClient, c, tickers: list[str], period: int, start_ts: int, end_ts: int) -> int:
    """Candles for tickers sharing one window via the batch endpoint. Calls are sized so none
    asks for more than BATCH_CANDLES candles: fewer tickers per call for long windows, and
    time-sliced when a single ticker overflows. Failed calls are logged and skipped."""
    if not tickers or start_ts >= end_ts:
        return 0
    per_ticker = (end_ts - start_ts) // (period * 60) + 1
    size = max(1, min(k.BATCH_CANDLE_TICKERS, k.BATCH_CANDLES // per_ticker))
    step = period * 60 * (k.BATCH_CANDLES // size - 1)  # inclusive range: N periods -> N+1 candles
    n = 0
    for i in range(0, len(tickers), size):
        chunk = tickers[i:i + size]
        for s in range(start_ts, end_ts, step):
            try:
                by_ticker = k.candlesticks_batch(chunk, s, min(s + step, end_ts), period)
            except Exception as e:  # noqa: BLE001
                log.warning("candles batch failed (%s..%s, period %d): %s", chunk[0], chunk[-1], period, e)
                continue
            for t, candles in by_ticker.items():
                if candles:
                    n += db.insert_candles(c, t, period, candles)
            c.commit()
    return n


def sync(k: KalshiClient, c) -> None:
    """Hourly: series; open events + markets and ones closed/settled in the last 3 days;
    then, for open markets in watchlisted series only, recent hourly candles, 1-minute
    candles (minute_candles) and new trades (trades)."""
    with db.run_log(c, "sync") as stats:
        with _stage("sync", "series"):
            for page, _ in k.series():
                stats["rows"] += db.upsert_series(c, page)
            c.commit()

        recent = _epoch(_now() - timedelta(days=3))
        with _stage("sync", "open events"):
            stats["rows"] += _sync_events(k, c, status="open")
        with _stage("sync", "closed events (3d)"):
            stats["rows"] += _sync_events(k, c, status="closed", min_close_ts=recent)
        with _stage("sync", "settled events (3d)"):
            stats["rows"] += _sync_events(k, c, status="settled", min_settled_ts=recent)

        watch = db.watchlist(c)
        now = _now()
        with _stage("sync", "hourly candles (3h)"):
            tickers = [m["ticker"] for m in _open_markets(c, [w["series_ticker"] for w in watch])]
            stats["rows"] += _batch_candles(k, c, tickers, HOUR, _epoch(now - timedelta(hours=3)), _epoch(now))
        with _stage("sync", "minute candles (2h)"):
            tickers = [m["ticker"] for m in _open_markets(c, [w["series_ticker"] for w in watch if w["minute_candles"]])]
            stats["rows"] += _batch_candles(k, c, tickers, MINUTE, _epoch(now - timedelta(hours=2)), _epoch(now))
        with _stage("sync", "trades"):
            for m in _open_markets(c, [w["series_ticker"] for w in watch if w["trades"]]):
                job = f"trades:{m['ticker']}"
                st = db.get_state(c, job)
                min_ts = _epoch(st["watermark"]) if st["watermark"] else None
                for page, _ in k.trades(ticker=m["ticker"], min_ts=min_ts):
                    stats["rows"] += db.insert_trades(c, page)
                db.set_state(c, job, watermark=now - timedelta(minutes=10))
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
def _life_windows(rows: list[dict], period: int) -> dict[tuple[int, int], list[str]]:
    """Group markets by their (open_time, settled_time) window, snapped outward to period
    boundaries so markets in the same event share one batch call."""
    span = period * 60
    groups: dict[tuple[int, int], list[str]] = {}
    for m in rows:
        start = m["open_time"] or (m["close_time"] - timedelta(days=90))
        end = min(m["settled_time"], _now())
        key = (_epoch(start) // span * span, -(-_epoch(end) // span) * span)
        groups.setdefault(key, []).append(m["ticker"])
    return groups


def reconcile(k: KalshiClient, c) -> None:
    """Nightly: lock in results for anything settled in the last 3 days, then daily candles
    over each such market's full life; hourly too for markets in watchlisted series."""
    with db.run_log(c, "reconcile") as stats:
        since = _now() - timedelta(days=3)
        with _stage("reconcile", "settled events (3d)"):
            stats["rows"] += _sync_events(k, c, status="settled", min_settled_ts=_epoch(since))

        rows = _market_rows(c, "settled_time >= %s AND ticker NOT LIKE %s", (since, "KXMVE%"))
        watched = {w["series_ticker"] for w in db.watchlist(c)}
        hourly = [m for m in rows if m["series_ticker"] in watched]
        for name, period, subset in (("daily", DAY, rows), ("hourly", HOUR, hourly)):
            with _stage("reconcile", f"{name} candles ({len(subset)} markets)"):
                for (s, e), tickers in _life_windows(subset, period).items():
                    stats["rows"] += _batch_candles(k, c, tickers, period, s, e)
