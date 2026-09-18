"""Thin Kalshi Trade API client: rate-limited, retrying, cursor-aware.

Public market data needs no auth. All prices come back as fixed-point dollar
strings ("0.5600") and counts as fixed-point strings ("10.00"); we pass them
through untouched and let Postgres numeric columns hold them exactly.
"""
from __future__ import annotations

import logging
import os
import time
from typing import Any, Iterator

import httpx

log = logging.getLogger(__name__)

BASE_URL = os.getenv("KALSHI_BASE_URL", "https://external-api.kalshi.com/trade-api/v2")
RPS = float(os.getenv("KALSHI_RPS", "8"))


class _TokenBucket:
    def __init__(self, rps: float):
        self.rps = rps
        self.tokens = rps
        self.last = time.monotonic()

    def take(self) -> None:
        now = time.monotonic()
        self.tokens = min(self.rps, self.tokens + (now - self.last) * self.rps)
        self.last = now
        if self.tokens < 1:
            time.sleep((1 - self.tokens) / self.rps)
            self.tokens = 0
        else:
            self.tokens -= 1


class KalshiClient:
    def __init__(self, base_url: str = BASE_URL, rps: float = RPS):
        self.http = httpx.Client(base_url=base_url, timeout=30.0, headers={"Accept": "application/json"})
        self.bucket = _TokenBucket(rps)
        self._cutoff: dict[str, Any] | None = None

    # ------------------------------------------------------------------ core
    def get(self, path: str, params: dict[str, Any] | None = None, retries: int = 5) -> dict[str, Any]:
        params = {k: v for k, v in (params or {}).items() if v is not None and v != ""}
        for attempt in range(retries):
            self.bucket.take()
            try:
                r = self.http.get(path, params=params)
            except httpx.TransportError as e:
                log.warning("transport error %s on %s (attempt %d)", e, path, attempt + 1)
                time.sleep(2**attempt)
                continue
            if r.status_code == 429 or r.status_code >= 500:
                wait = float(r.headers.get("Retry-After", 2**attempt))
                log.warning("HTTP %s on %s; sleeping %.1fs", r.status_code, path, wait)
                time.sleep(wait)
                continue
            r.raise_for_status()
            return r.json()
        raise RuntimeError(f"gave up on {path} after {retries} attempts")

    def paginate(self, path: str, key: str, params: dict[str, Any] | None = None,
                 cursor: str | None = None, limit: int = 1000) -> Iterator[tuple[list[dict], str | None]]:
        """Yield (page_items, next_cursor). Caller persists next_cursor for resume."""
        params = dict(params or {})
        params["limit"] = limit
        while True:
            params["cursor"] = cursor
            data = self.get(path, params)
            items = data.get(key) or []
            cursor = data.get("cursor") or None
            yield items, cursor
            if not cursor or not items:
                return

    # -------------------------------------------------------------- reference
    def series(self, **params) -> Iterator[tuple[list[dict], str | None]]:
        return self.paginate("/series", "series", params, limit=200)

    def events(self, **params) -> Iterator[tuple[list[dict], str | None]]:
        return self.paginate("/events", "events", params, limit=200)

    def markets(self, cursor: str | None = None, **params) -> Iterator[tuple[list[dict], str | None]]:
        return self.paginate("/markets", "markets", params, cursor=cursor)

    def historical_markets(self, cursor: str | None = None, **params) -> Iterator[tuple[list[dict], str | None]]:
        return self.paginate("/historical/markets", "markets", params, cursor=cursor)

    def cutoff(self) -> dict[str, Any]:
        if self._cutoff is None:
            self._cutoff = self.get("/historical/cutoff")
        return self._cutoff

    # ------------------------------------------------------------- time series
    def candlesticks(self, ticker: str, start_ts: int, end_ts: int, period: int,
                     historical: bool = False, series_ticker: str | None = None) -> list[dict]:
        if historical:
            path = f"/historical/markets/{ticker}/candlesticks"
        else:
            if not series_ticker:
                raise ValueError("series_ticker is required for live candlesticks")
            path = f"/series/{series_ticker}/markets/{ticker}/candlesticks"
        data = self.get(path, {"start_ts": start_ts, "end_ts": end_ts, "period_interval": period})
        return data.get("candlesticks") or []

    BATCH_CANDLE_TICKERS = 100  # GET /markets/candlesticks cap; also 10k candles per response

    def candlesticks_batch(self, tickers: list[str], start_ts: int, end_ts: int, period: int) -> dict[str, list[dict]]:
        """Live-tier batch candles for many markets at once. Chunks at 100 tickers per call
        and returns {ticker: [candles]} (tickers with no candles map to [])."""
        out: dict[str, list[dict]] = {t: [] for t in tickers}
        for i in range(0, len(tickers), self.BATCH_CANDLE_TICKERS):
            chunk = tickers[i:i + self.BATCH_CANDLE_TICKERS]
            data = self.get("/markets/candlesticks", {
                "market_tickers": ",".join(chunk), "start_ts": start_ts, "end_ts": end_ts,
                "period_interval": period})
            for mk in data.get("markets") or []:
                t = mk.get("market_ticker") or mk.get("ticker")
                if t:
                    out.setdefault(t, []).extend(mk.get("candlesticks") or [])
        return out

    def trades(self, cursor: str | None = None, historical: bool = False, **params):
        path = "/historical/trades" if historical else "/markets/trades"
        return self.paginate(path, "trades", params, cursor=cursor)

    def orderbook(self, ticker: str, depth: int = 50) -> dict[str, Any]:
        data = self.get(f"/markets/{ticker}/orderbook", {"depth": depth})
        return data.get("orderbook") or data
