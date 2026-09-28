"""One-off backfill of every earnings-mention (KXEARNINGSMENTION*) series.

1. Series: page GET /series and keep the KXEARNINGSMENTION tickers (upserted).
2. Per series: its events (GET /events?series_ticker=), its archived markets
   (GET /historical/markets?series_ticker=) and its settled live-tier markets
   (GET /markets?series_ticker=&status=settled), all upserted. A finished series gets
   sync_state 'mentions_backfill:<series>' and is skipped on a rerun.
3. precall.fill for every settled KXEARNINGSMENTION market not yet done (archived markets go
   through /historical/markets/{ticker}/candlesticks), then db.refresh_word_counts.
"""
from __future__ import annotations

import logging

from . import db, precall
from .client import KalshiClient
from .jobs import _stage

log = logging.getLogger(__name__)

PREFIX = "KXEARNINGSMENTION"
STATE_PREFIX = "mentions_backfill:"


def series(k: KalshiClient, c) -> list[str]:
    """Every KXEARNINGSMENTION series ticker from GET /series (all pages), upserted."""
    found: list[dict] = []
    for page, _ in k.series():
        found.extend(s for s in page if (s.get("ticker") or "").startswith(PREFIX))
    db.upsert_series(c, found)
    c.commit()
    return sorted({s["ticker"] for s in found})


def backfill_series(k: KalshiClient, c, s: str) -> tuple[int, int]:
    """Events and settled markets (archive, then live tier) for one series; commits per page.
    Returns (events, markets) upserted."""
    n_events = n_markets = 0
    for page, _ in k.events(series_ticker=s):
        for ev in page:
            ev.pop("markets", None)
        n_events += db.upsert_events(c, page)
        c.commit()
    for pages in (k.historical_markets(series_ticker=s), k.markets(series_ticker=s, status="settled")):
        for page, _ in pages:
            n_markets += db.upsert_markets(c, page)
            c.commit()
    return n_events, n_markets


def run(k: KalshiClient, c) -> None:
    with db.run_log(c, "mentions_backfill") as stats:
        with _stage("mentions-backfill", "series"):
            all_series = series(k, c)
        done = db.states_with_prefix(c, STATE_PREFIX)
        todo = [s for s in all_series if s not in done]
        log.info("mentions-backfill: %d KXEARNINGSMENTION series, %d already done, %d to do",
                 len(all_series), len(all_series) - len(todo), len(todo))
        failed = 0
        with _stage("mentions-backfill", f"events + markets ({len(todo)} series)"):
            for i, s in enumerate(todo, 1):
                try:
                    n_events, n_markets = backfill_series(k, c, s)
                except Exception as e:  # noqa: BLE001
                    c.rollback()
                    failed += 1
                    log.warning("mentions-backfill: %s failed (will retry on rerun): %s", s, e)
                    continue
                stats["rows"] += n_events + n_markets
                db.set_state(c, STATE_PREFIX + s, meta={"events": n_events, "markets": n_markets})
                c.commit()
                log.info("mentions-backfill: [%d/%d] %s: %d events, %d markets upserted", i, len(todo), s,
                         n_events, n_markets)
        log.info("mentions-backfill: series done: %d ok, %d failed", len(todo) - failed, failed)
        stats["rows"] += precall.fill(k, c)["candles"]
    db.refresh_word_counts(c)
