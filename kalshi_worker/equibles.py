"""Equibles earnings-call transcripts: a third source, only for calls nobody else has.

Works kalshi.v_transcript_gaps (settled Kalshi calls the label audit found no transcript for
first, then recent earnings releases with no transcript within 3 days, newest first), skipping
gaps tried in the last 7 days (kalshi.equibles_attempts). For each gap:

1. GET /v1/stocks/{T}/investor-events (once per symbol per run) and take the EarningsCall
   event with hasTranscript whose callDate is nearest the target date, within 5 days.
2. GET /v1/stocks/{T}/earnings-calls/{fiscalYear}/{fiscalQuarter}/speakers?offset=N, 50 turns
   a page, while hasMore.
3. Store it like the Fortune path: transcript_sources + transcripts (source 'equibles',
   raw_html = all pages' turns as JSON) + one transcript_segments row per turn.

The free tier allows 100 requests a day: every request (listings and pages) counts against a
per-run budget of MAX_REQUESTS, and the run stops cleanly on the budget or HTTP 429 without
storing a partial transcript or recording an attempt for that gap. A response without the
expected "data" list raises ShapeError, which stops the run the same way. Needs
EQUIBLES_API_KEY.
"""
from __future__ import annotations

import json
import logging
import os
import re
import time
from datetime import date

import httpx

from . import db, tickers
from .transcripts import ANALYST_WORDS, _write_segments

log = logging.getLogger(__name__)

BASE_URL = "https://api.equibles.com/v1"
EVENTS_PATH = "/stocks/{ticker}/investor-events"
SPEAKERS_PATH = "/stocks/{ticker}/earnings-calls/{fy}/{fq}/speakers"
SOURCE_URL = "https://equibles.com/stocks/{ticker}/calls/{fy}-q{fq}"
MAX_REQUESTS = 90                # free tier: 100 requests/day
RETRY_DAYS = 7
MATCH_DAYS = 5                   # the event's callDate must be this close to the gap's target date
MAX_PAGES = 40                   # safety stop for hasMore (40 x 50 turns)
ANALYST_ROLE_RE = re.compile(r"analyst|research|equity", re.I)
EXEC_ROLE_RE = re.compile(r"\b(?:chief|officer|president|ceo|cfo|coo|cto|vp|head|director|chair|chairman|chairwoman|"
                          r"founder|treasurer|controller|secretary|counsel|investor relations|executive)\b", re.I)


class QuotaExhausted(Exception):
    """Per-run request budget spent, or the API answered 429."""


class ShapeError(Exception):
    """A response without the expected "data" list."""


# ------------------------------------------------------------------------------ http
class Equibles:
    def __init__(self, api_key: str, http: httpx.Client | None = None, budget: int = MAX_REQUESTS):
        self.http = http or httpx.Client(base_url=BASE_URL, timeout=30.0, headers={
            "Authorization": f"Bearer {api_key}", "Accept": "application/json"})
        self.budget, self.used = budget, 0

    def get(self, path: str, params: dict | None = None) -> dict | None:
        """GET one API path; None on 404. Every attempt counts against the budget; one retry on
        5xx / transport errors; 429 raises QuotaExhausted."""
        for attempt in range(2):
            if self.used >= self.budget:
                raise QuotaExhausted(f"request budget of {self.budget} used")
            self.used += 1
            try:
                r = self.http.get(path, params=params)
            except httpx.TransportError as e:
                if attempt:
                    raise
                log.warning("equibles: transport error %s on %s; retrying", e, path)
                time.sleep(2)
                continue
            if r.status_code == 429:
                raise QuotaExhausted(f"HTTP 429 on {path}")
            if r.status_code == 404:
                return None
            if r.status_code >= 500 and not attempt:
                log.warning("equibles: HTTP %s on %s; retrying", r.status_code, path)
                time.sleep(2)
                continue
            r.raise_for_status()
            return r.json()
        raise RuntimeError(f"gave up on {path}")

    def close(self) -> None:
        self.http.close()


def _data(payload, what: str) -> list:
    items = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(items, list):
        keys = sorted(payload)[:15] if isinstance(payload, dict) else type(payload).__name__
        raise ShapeError(f"{what}: no 'data' list (keys: {keys})")
    return items


def _date(value) -> date | None:
    try:
        return date.fromisoformat(str(value)[:10]) if value else None
    except ValueError:
        return None


# ---------------------------------------------------------------------------- parse
def nearest_call(events: list[dict], target: date) -> dict | None:
    """The EarningsCall event with a transcript whose callDate is nearest `target`, within
    MATCH_DAYS -> {"fy", "fq", "date"}; None when there is none."""
    best = None
    for e in events:
        if not isinstance(e, dict) or e.get("eventType") != "EarningsCall" or e.get("hasTranscript") is not True:
            continue
        d, fy, fq = _date(e.get("callDate")), e.get("fiscalYear"), e.get("fiscalQuarter")
        if not d or fy is None or fq is None or abs((d - target).days) > MATCH_DAYS:
            continue
        if best is None or abs((d - target).days) < abs((best["date"] - target).days):
            best = {"fy": int(fy), "fq": int(fq), "date": d}
    return best


def _names_firm(role: str) -> bool:
    """A speakerRole that is a firm (a bank / broker name) rather than an executive title."""
    low = role.lower()
    return any(w in low for w in ANALYST_WORDS) and not EXEC_ROLE_RE.search(role)


def parse_turns(turns: list[dict]) -> dict:
    """/speakers turns (all pages) -> {"turns", "raw_text"}, one turn per entry: speaker =
    speakerName or "Speaker {speakerIndex}", speaker_title = speakerRole; role operator (role or
    name says operator) / analyst (role says analyst, research or equity, or names a firm) /
    unknown (no speakerName) / exec; section prepared until the first analyst turn, then qa."""
    out: list[dict] = []
    in_qa = False
    for t in turns:
        text = (t.get("text") or "").strip()
        if not text:
            continue
        name = (t.get("speakerName") or "").strip()
        role_raw = (t.get("speakerRole") or "").strip()
        if "operator" in role_raw.lower() or "operator" in name.lower():
            role = "operator"
        elif ANALYST_ROLE_RE.search(role_raw) or (role_raw and _names_firm(role_raw)):
            role = "analyst"
        elif not name:
            role = "unknown"
        else:
            role = "exec"
        in_qa = in_qa or role == "analyst"
        out.append({"speaker": name or f"Speaker {t.get('speakerIndex')}", "speaker_title": role_raw or None,
                    "role": role, "section": "qa" if in_qa else "prepared", "text": text})
    return {"turns": out, "raw_text": "\n\n".join(t["text"] for t in out)}


def fetch_turns(eq: Equibles, ticker: str, fy: int, fq: int) -> tuple[dict | None, list[dict]]:
    """All pages of a call's /speakers -> (first page's envelope, turns). (None, []) on 404.
    QuotaExhausted propagates, so a call is either fetched whole or not at all."""
    path = SPEAKERS_PATH.format(ticker=ticker, fy=fy, fq=fq)
    head, turns, offset = None, [], 0
    for _ in range(MAX_PAGES):
        page = eq.get(path, {"offset": offset})
        if page is None:
            return None, []
        items = _data(page, f"{ticker} FY{fy} Q{fq} speakers")
        head = head or page
        turns.extend(items)
        if not page.get("hasMore") or not items:
            break
        offset += len(items)
    else:
        log.warning("equibles: %s FY%d Q%d still hasMore after %d pages; keeping %d turns", ticker, fy, fq,
                    MAX_PAGES, len(turns))
    return head, turns


# --------------------------------------------------------------------------------- db
def _gaps(c) -> tuple[int, list[tuple[str, date]]]:
    """(all gaps, gaps not tried in the last RETRY_DAYS): settled Kalshi calls (label audit)
    first, then newest first."""
    with c.cursor() as cur:
        cur.execute("""SELECT g.symbol, g.target_date,
                              EXISTS (SELECT 1 FROM equibles_attempts a
                                      WHERE a.symbol = g.symbol AND a.target_date = g.target_date
                                        AND a.tried_at > now() - %s * interval '1 day') AS recent
                       FROM v_transcript_gaps g
                       ORDER BY (g.reasons LIKE '%%label_audit%%') DESC, g.target_date DESC, g.symbol""",
                    (RETRY_DAYS,))
        rows = cur.fetchall()
    return len(rows), [(s, d) for s, d, recent in rows if not recent]


def _have(c, symbol: str, fy: int, fq: int) -> bool:
    with c.cursor() as cur:
        cur.execute("""SELECT 1 FROM transcripts WHERE symbol = %s AND fiscal_year = %s AND fiscal_quarter = %s
                       AND source = 'equibles'""", (symbol, fy, fq))
        return cur.fetchone() is not None


def _attempt(c, symbol: str, target: date, ok: bool, note: str) -> None:
    with c.cursor() as cur:
        cur.execute("INSERT INTO equibles_attempts (symbol, target_date, ok, note) VALUES (%s, %s, %s, %s)",
                    (symbol, target, ok, note[:2000]))
    c.commit()


def _write(c, symbol: str, ticker: str, fy: int, fq: int, call_date: date | None, parsed: dict,
           turns: list[dict]) -> int:
    url = SOURCE_URL.format(ticker=ticker.lower(), fy=fy, fq=fq)
    raw_text = parsed["raw_text"]
    with c.cursor() as cur:
        cur.execute("""INSERT INTO transcript_sources (symbol, source, url, fiscal_year, fiscal_quarter, call_date,
                                                       published_date, status, error, fetched_at)
                       VALUES (%s, 'equibles', %s, %s, %s, %s, %s, 'parsed', NULL, now())
                       ON CONFLICT (url) DO UPDATE SET fiscal_year = EXCLUDED.fiscal_year,
                         fiscal_quarter = EXCLUDED.fiscal_quarter, call_date = EXCLUDED.call_date,
                         published_date = EXCLUDED.published_date, status = 'parsed', error = NULL, fetched_at = now()""",
                    (symbol, url, fy, fq, call_date, call_date))
        cur.execute("""
            INSERT INTO transcripts (symbol, fiscal_year, fiscal_quarter, call_date, published_date, source,
                                     source_url, raw_html, raw_text, word_count, parsed_at)
            VALUES (%s,%s,%s,%s,%s,'equibles',%s,%s,%s,%s,now())
            ON CONFLICT (symbol, fiscal_year, fiscal_quarter, source) DO UPDATE SET
              call_date=EXCLUDED.call_date, published_date=EXCLUDED.published_date,
              source_url=EXCLUDED.source_url, raw_html=EXCLUDED.raw_html,
              raw_text=EXCLUDED.raw_text, word_count=EXCLUDED.word_count, parsed_at=now()
            RETURNING id""",
            (symbol, fy, fq, call_date, call_date, url, json.dumps(turns), raw_text, len(raw_text.split())))
        tid = cur.fetchone()[0]
        _write_segments(cur, tid, parsed["turns"])
    c.commit()
    return 1 + len(parsed["turns"])


# -------------------------------------------------------------------------------- job
def run(c, limit: int | None = None, refresh: bool = True, client: Equibles | None = None) -> None:
    """Fill transcript gaps from Equibles; `limit` caps the gaps looked at. Ends with
    db.refresh_word_counts (kalshi.resolve_speaker_roles(), then the mv_word_counts refresh)
    unless `refresh` is False (the transcripts job refreshes once itself)."""
    api_key = os.getenv("EQUIBLES_API_KEY")
    if client is None and not api_key:
        log.warning("equibles: EQUIBLES_API_KEY is not set; skipping")
        return
    eq = client or Equibles(api_key)
    try:
        with db.run_log(c, "equibles") as stats:
            total, eligible = _gaps(c)
            todo = eligible[:limit] if limit is not None else eligible
            log.info("equibles: %d transcript gaps, %d not tried in the last %d days%s", total, len(eligible), RETRY_DAYS,
                     f"; looking at {len(todo)} (limit {limit})" if limit is not None else "")
            sym_tickers = tickers.mention_tickers(c)
            events: dict[str, list | None] = {}     # investor-events listing per ticker
            stored = not_covered = no_match = other = 0
            stop = None
            for symbol, target in todo:
                try:
                    ticker = sym_tickers.get(symbol) or tickers.alias(symbol)   # API paths use the ticker
                    if ticker not in events:
                        listing = eq.get(EVENTS_PATH.format(ticker=ticker))
                        events[ticker] = None if listing is None else _data(listing, f"{ticker} investor-events")
                    if events[ticker] is None:
                        not_covered += 1
                        _attempt(c, symbol, target, False, "not covered (404 on investor-events)")
                        continue
                    ref = nearest_call(events[ticker], target)
                    if not ref:
                        no_match += 1
                        _attempt(c, symbol, target, False, f"no matching call (EarningsCall with transcript within "
                                                           f"{MATCH_DAYS} days)")
                        continue
                    fy, fq = ref["fy"], ref["fq"]
                    if _have(c, symbol, fy, fq):
                        other += 1
                        _attempt(c, symbol, target, False, f"FY{fy} Q{fq} ({ref['date']}) already stored from equibles")
                        continue
                    head, turns = fetch_turns(eq, ticker, fy, fq)
                    parsed = parse_turns(turns)
                    if not parsed["turns"]:
                        other += 1
                        _attempt(c, symbol, target, False, f"FY{fy} Q{fq} ({ref['date']}): no transcript turns"
                                                           f"{' (404)' if head is None else ''}")
                        continue
                    call_date = _date(head.get("callDate")) or ref["date"]
                    stats["rows"] += _write(c, symbol, ticker, fy, fq, call_date, parsed, turns)
                    stored += 1
                    _attempt(c, symbol, target, True, f"stored FY{fy} Q{fq}, call {call_date}, {len(parsed['turns'])} turns")
                    log.info("equibles: %s gap %s filled: FY%d Q%d, call %s, %d turns, %d words", symbol, target, fy, fq,
                             call_date, len(parsed["turns"]), len(parsed["raw_text"].split()))
                except (QuotaExhausted, ShapeError) as e:
                    c.rollback()
                    stop = f"stopped at {symbol} {target} (no attempt recorded): {e}"
                    break
                except Exception as e:  # noqa: BLE001
                    c.rollback()
                    other += 1
                    log.warning("equibles: %s gap %s failed: %s", symbol, target, e)
                    _attempt(c, symbol, target, False, f"{type(e).__name__}: {e}")
            if stop:
                log.warning("equibles: %s", stop)
            log.info("equibles: done: %d/%d requests used, %d symbols listed, %d calls stored, %d not covered, "
                     "%d no matching call, %d other misses; %d of %d gaps still open",
                     eq.used, eq.budget, len(events), stored, not_covered, no_match, other, total - stored, total)
    finally:
        if client is None:
            eq.close()
    if refresh:
        db.refresh_word_counts(c)
