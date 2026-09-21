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


def _batch_candles(k: KalshiClient, c, tickers: list[str], period: int, start_ts: int, end_ts: int,
                   on_done=None) -> int:
    """Candles for tickers sharing one window via the batch endpoint. Calls are sized so none
    asks for more than BATCH_CANDLES candles: fewer tickers per call for long windows, and
    time-sliced when a single ticker overflows. Failed calls are logged and skipped; `on_done`
    (if given) is called with each chunk of tickers whose every call succeeded, before commit."""
    if not tickers or start_ts >= end_ts:
        return 0
    per_ticker = (end_ts - start_ts) // (period * 60) + 1
    size = max(1, min(k.BATCH_CANDLE_TICKERS, k.BATCH_CANDLES // per_ticker))
    step = period * 60 * (k.BATCH_CANDLES // size - 1)  # inclusive range: N periods -> N+1 candles
    n = 0
    for i in range(0, len(tickers), size):
        chunk = tickers[i:i + size]
        ok = True
        for s in range(start_ts, end_ts, step):
            try:
                by_ticker = k.candlesticks_batch(chunk, s, min(s + step, end_ts), period)
            except Exception as e:  # noqa: BLE001
                log.warning("candles batch failed (%s..%s, period %d): %s", chunk[0], chunk[-1], period, e)
                ok = False
                continue
            for t, candles in by_ticker.items():
                if candles:
                    n += db.insert_candles(c, t, period, candles)
        if ok and on_done:
            on_done(chunk)
        c.commit()
    return n


def _life_windows(rows: list[dict], period: int) -> dict[tuple[int, int], list[str]]:
    """Group markets by their (open_time, settled_time) window, snapped outward to period
    boundaries so markets in the same event share one batch call."""
    span = period * 60
    groups: dict[tuple[int, int], list[str]] = {}
    for m in rows:
        start = m["open_time"] or (m["close_time"] - timedelta(days=90))
        end = min(m["settled_time"] or m["close_time"], _now())
        key = (_epoch(start) // span * span, -(-_epoch(end) // span) * span)
        groups.setdefault(key, []).append(m["ticker"])
    return groups


# --------------------------------------------------------------------------- backfill
SETTLED = "result <> '' AND ticker NOT LIKE 'KXMVE%%'"


def _mark_checked(c, tickers: list[str]) -> None:
    with c.cursor() as cur:
        cur.execute("UPDATE markets SET daily_checked_at = now() WHERE ticker = ANY(%s)", (list(tickers),))


def _daily_progress(c) -> tuple[int, int]:
    """(settled markets with daily_checked_at set, settled markets) excluding KXMVE."""
    with c.cursor() as cur:
        cur.execute(f"SELECT count(*) FILTER (WHERE daily_checked_at IS NOT NULL), count(*) FROM markets WHERE {SETTLED}")
        return cur.fetchone()


def backfill(k: KalshiClient, c) -> None:
    """One-time: daily candles for every settled market not yet checked (markets.daily_checked_at
    is null), newest first. Markets settled after the /historical/cutoff go through the batch
    endpoint (grouped by life window, up to BATCH_CANDLE_TICKERS per call); older ones go one
    at a time through /historical/markets/{ticker}/candlesticks, last. daily_checked_at is set
    after each market once its fetch succeeded, whether or not candles came back, so a rerun
    only revisits markets whose fetch failed. Progress is checked/settled market counts."""
    with db.run_log(c, "backfill") as stats:
        rows = _market_rows(c, f"{SETTLED} AND daily_checked_at IS NULL ORDER BY settled_time DESC NULLS LAST")
        cutoff = k.cutoff()
        cutoff_ts = db._ts(cutoff.get("market_settled_ts") or cutoff.get("settled_ts"))

        def old(m: dict) -> bool:
            end = m["settled_time"] or m["close_time"]
            return bool(cutoff_ts and end and end < cutoff_ts)

        recent = [m for m in rows if not old(m)]
        historical = [m for m in rows if old(m)]
        done = 0
        checked, settled = _daily_progress(c)
        log.info("backfill: %d/%d settled markets checked for daily candles; %d to do (%d batch, %d historical)",
                 checked, settled, len(rows), len(recent), len(historical))

        def checkpoint(final: bool = False) -> None:
            checked, settled = _daily_progress(c)
            db.set_state(c, "backfill_candles", meta={"checked": checked, "settled": settled, "complete": final})
            c.commit()
            log.info("backfill: %d/%d settled markets checked (%.1f%%), %d candles this run",
                     checked, settled, 100 * checked / settled if settled else 0, stats["rows"])

        with _stage("backfill", f"batch daily candles ({len(recent)} markets)"):
            for (s, e), tickers in _life_windows(recent, DAY).items():
                stats["rows"] += _batch_candles(k, c, tickers, DAY, s, e, on_done=lambda chunk: _mark_checked(c, chunk))
                prev, done = done, done + len(tickers)
                if prev // 50 != done // 50:
                    checkpoint()

        with _stage("backfill", f"historical daily candles ({len(historical)} markets)"):
            for m in historical:
                try:
                    stats["rows"] += load_candles(k, c, m, DAY, historical=True)
                    _mark_checked(c, [m["ticker"]])
                except Exception as e:  # noqa: BLE001
                    log.warning("historical candles failed for %s: %s", m["ticker"], e)
                done += 1
                if done % 50 == 0:
                    checkpoint()
        checkpoint(final=True)


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


def sync(k: KalshiClient, c) -> None:
    """Hourly: series; open events + markets and ones closed/settled in the last 3 days;
    then, for open markets in watchlisted series only, recent hourly candles, 1-minute
    candles (minute_candles) and new trades (trades)."""
    with db.run_log(c, "sync") as stats:
        with _stage("sync", "series"):
            for page, _ in k.series():
                stats["rows"] += db.upsert_series(c, page)
            c.commit()

        # closed/settled events are polled by update time: from the last run's watermark
        # (10 min overlap) or 3 days back on the first run; the watermark advances afterwards.
        started = _now()
        st = db.get_state(c, "events_updated")
        since = (st["watermark"] - timedelta(minutes=10)) if st["watermark"] else started - timedelta(days=3)
        with _stage("sync", "open events"):
            stats["rows"] += _sync_events(k, c, status="open")
        with _stage("sync", "closed events (updated)"):
            stats["rows"] += _sync_events(k, c, status="closed", min_updated_ts=_epoch(since))
        with _stage("sync", "settled events (updated)"):
            stats["rows"] += _sync_events(k, c, status="settled", min_updated_ts=_epoch(since))
        db.set_state(c, "events_updated", watermark=started)
        c.commit()

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
def reconcile(k: KalshiClient, c) -> None:
    """Nightly: lock in results for anything settled in the last 3 days, then daily candles
    over each such market's full life; hourly too for markets in watchlisted series."""
    with db.run_log(c, "reconcile") as stats:
        since = _now() - timedelta(days=3)
        with _stage("reconcile", "settled events (updated 3d)"):
            stats["rows"] += _sync_events(k, c, status="settled", min_updated_ts=_epoch(since))

        rows = _market_rows(c, "settled_time >= %s AND ticker NOT LIKE %s", (since, "KXMVE%"))
        watched = {w["series_ticker"] for w in db.watchlist(c)}
        hourly = [m for m in rows if m["series_ticker"] in watched]
        for name, period, subset in (("daily", DAY, rows), ("hourly", HOUR, hourly)):
            with _stage("reconcile", f"{name} candles ({len(subset)} markets)"):
                for (s, e), tickers in _life_windows(subset, period).items():
                    stats["rows"] += _batch_candles(k, c, tickers, period, s, e)
