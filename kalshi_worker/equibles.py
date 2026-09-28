"""Equibles earnings-call transcripts: a third source, only for calls nobody else has.

Works kalshi.v_transcript_gaps (mention events the label audit found no transcript for, and
recent earnings releases with no transcript within 3 days), newest first, skipping gaps tried
in the last 7 days (kalshi.equibles_attempts). For each gap: list the ticker's earnings calls,
take the one nearest the target date, fetch
/v1/stocks/{ticker}/earnings-calls/{fiscalYear}/{fiscalQuarter} (the company's own fiscal
labels) and store it like the Fortune path: transcript_sources + transcripts (source
'equibles', raw_html = the JSON response) + one transcript_segments row per speaker turn.

The free tier allows 100 requests a day: each run makes at most MAX_REQUESTS and stops
cleanly on HTTP 429. Needs EQUIBLES_API_KEY.

The response parsing below is deliberately tolerant about field names (camelCase or
snake_case, list wrapped in data/items/results or bare); a response it can't read at all
raises ShapeError, which stops the run without recording an attempt, so fixing the parser
doesn't cost a 7-day wait.
"""
from __future__ import annotations

import json
import logging
import os
import re
import time
from datetime import date, datetime

import httpx

from . import db
from .transcripts import _write_segments

log = logging.getLogger(__name__)

BASE_URL = "https://api.equibles.com/v1"
LIST_PATH = "/stocks/{ticker}/earnings-calls"
CALL_PATH = "/stocks/{ticker}/earnings-calls/{fy}/{fq}"
MAX_REQUESTS = 80                # free tier: 100 requests/day
RETRY_DAYS = 7
MATCH_DAYS = 10                  # nearest listed call must be this close to the gap's target date
CORPORATE_WORDS = {"the", "inc", "corp", "corporation", "co", "company", "ltd", "plc", "nv", "sa", "holdings",
                   "group", "technologies", "llc"}


class QuotaExhausted(Exception):
    """Per-run request budget spent, or the API answered 429."""


class ShapeError(Exception):
    """The response doesn't look like anything the parser knows."""


# ------------------------------------------------------------------------------ http
class Equibles:
    def __init__(self, api_key: str, http: httpx.Client | None = None, budget: int = MAX_REQUESTS):
        self.http = http or httpx.Client(base_url=BASE_URL, timeout=30.0, headers={
            "Authorization": f"Bearer {api_key}", "Accept": "application/json"})
        self.budget, self.used = budget, 0

    def get(self, path: str) -> dict | list | None:
        """GET one API path; None on 404. Every attempt counts against the budget; one retry on
        5xx / transport errors; 429 raises QuotaExhausted."""
        for attempt in range(2):
            if self.used >= self.budget:
                raise QuotaExhausted(f"request budget of {self.budget} used")
            self.used += 1
            try:
                r = self.http.get(path)
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


# ---------------------------------------------------------------------------- parse
def _pick(d: dict, *keys: str):
    for k in keys:
        if k in d and d[k] not in (None, ""):
            return d[k]
    return None


def _as_list(payload, *keys: str) -> list | None:
    """The list inside a response: the payload itself, or under data/items/results/`keys`
    (one level of nesting, e.g. {"data": {"items": [...]}})."""
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for k in (*keys, "data", "items", "results"):
            v = payload.get(k)
            if isinstance(v, list):
                return v
            if isinstance(v, dict):
                inner = _as_list(v, *keys)
                if inner is not None:
                    return inner
    return None


def _date(value) -> date | None:
    if not value:
        return None
    s = str(value)
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).date() if "T" in s else date.fromisoformat(s[:10])
    except ValueError:
        return None


def call_ref(item: dict) -> dict | None:
    """One listed earnings call -> {"fy", "fq", "date"}; None when a field is missing."""
    fy = _pick(item, "fiscalYear", "fiscal_year", "year")
    fq = _pick(item, "fiscalQuarter", "fiscal_quarter", "quarter")
    d = _date(_pick(item, "eventDate", "event_date", "date", "callDate", "call_date", "startDate", "start_date",
                    "dateTime", "datetime", "reportDate", "report_date"))
    if fq is not None:
        m = re.search(r"\d", str(fq))
        fq = int(m.group(0)) if m else None
    try:
        fy = int(fy) if fy is not None else None
    except (TypeError, ValueError):
        fy = None
    return {"fy": fy, "fq": fq, "date": d} if fy and fq and d else None


def nearest_call(listing, target: date) -> dict | None:
    """The listed call nearest `target` within MATCH_DAYS. ShapeError when nothing in the
    listing can be read as a call."""
    items = _as_list(listing, "earningsCalls", "earnings_calls", "events", "calls")
    if items is None:
        raise ShapeError(f"no list in earnings-call listing (keys: {sorted(listing)[:12] if isinstance(listing, dict) else type(listing).__name__})")
    refs = [r for r in (call_ref(i) for i in items if isinstance(i, dict)) if r]
    if items and not refs:
        raise ShapeError(f"no fiscal year/quarter/date in listed calls (first item keys: {sorted(items[0])[:15]})")
    near = [r for r in refs if abs((r["date"] - target).days) <= MATCH_DAYS]
    return min(near, key=lambda r: abs((r["date"] - target).days)) if near else None


def _core(name: str | None) -> set[str]:
    """Significant lower-case words of a company name ('The Walt Disney Company' -> {walt, disney})."""
    return {w for w in re.sub(r"[^a-z0-9\s]", " ", (name or "").lower()).split() if w not in CORPORATE_WORDS}


def parse_call(payload: dict, issuer: str | None) -> dict:
    """Transcript response -> {"call_date", "turns", "raw_text"}. Turns: speaker = name (or
    "Speaker N" when unverified), speaker_title = role - company; role operator / analyst (role
    or company says analyst, or a company other than the issuer) / unknown (unverified) / exec;
    section prepared until the first analyst turn, then qa. Consecutive entries from one speaker
    merge."""
    body = payload.get("data") if isinstance(payload.get("data"), dict) else payload
    entries = _as_list(body, "transcript", "segments", "paragraphs", "turns", "speakers", "content")
    if entries is None and isinstance(body.get("transcript"), dict):
        entries = _as_list(body["transcript"], "segments", "paragraphs", "turns", "speakers", "content")
    if not entries:
        raise ShapeError(f"no speaker turns in transcript response (keys: {sorted(body)[:15]})")
    issuer_core = _core(issuer)
    turns: list[dict] = []
    in_qa = False
    for e in entries:
        if not isinstance(e, dict):
            continue
        text = _pick(e, "text", "content", "speech", "paragraph", "body")
        if isinstance(text, list):
            text = " ".join(str(x.get("text", x)) if isinstance(x, dict) else str(x) for x in text)
        text = (text or "").strip()
        if not text:
            continue
        sp = e.get("speaker") if isinstance(e.get("speaker"), dict) else {}
        src = {**e, **sp}
        num = _pick(sp, "number", "id", "speakerNumber", "speakerId") if sp else None
        if num is None:
            num = _pick(e, "speakerNumber", "speaker_number", "speakerId", "speaker_id")
        if num is None and isinstance(e.get("speaker"), int):
            num = e["speaker"]
        name = _pick(src, "speakerName", "speaker_name", "name")
        if name is None and isinstance(e.get("speaker"), str):
            name = e["speaker"]
        verified = _pick(src, "verified", "isVerified", "is_verified", "speakerVerified")
        unverified = verified is False or not name
        role_raw = (_pick(src, "speakerRole", "speaker_role", "role", "title") or "").strip()
        company = (_pick(src, "speakerCompany", "speaker_company", "company", "organization", "firm") or "").strip()
        name = str(name).strip() if name and not unverified else f"Speaker {num if num is not None else '?'}"
        low = f"{role_raw} {company}".lower()
        if "operator" in name.lower() or "operator" in role_raw.lower():
            role = "operator"
        elif "analyst" in low or (company and issuer_core and not (_core(company) & issuer_core)):
            role = "analyst"
        elif unverified:
            role = "unknown"
        else:
            role = "exec"
        in_qa = in_qa or role == "analyst"
        section = "qa" if in_qa else "prepared"
        title = " - ".join(x for x in (role_raw, company) if x) or None
        prev = turns[-1] if turns else None
        if prev and prev["speaker"] == name and prev["section"] == section:
            prev["text"] = f"{prev['text']} {text}"
            continue
        turns.append({"speaker": name, "speaker_title": title, "role": role, "section": section, "text": text})
    if not turns:
        raise ShapeError(f"no text in transcript entries (first entry keys: {sorted(entries[0])[:15] if isinstance(entries[0], dict) else '?'})")
    call_date = _date(_pick(body, "eventDate", "event_date", "date", "callDate", "call_date", "startDate", "dateTime"))
    return {"call_date": call_date, "turns": turns, "raw_text": "\n\n".join(t["text"] for t in turns)}


# --------------------------------------------------------------------------------- db
def _gaps(c) -> tuple[int, list[tuple[str, date]]]:
    """(all gaps, gaps not tried in the last RETRY_DAYS, newest first)."""
    with c.cursor() as cur:
        cur.execute("""SELECT g.symbol, g.target_date,
                              EXISTS (SELECT 1 FROM equibles_attempts a
                                      WHERE a.symbol = g.symbol AND a.target_date = g.target_date
                                        AND a.tried_at > now() - %s * interval '1 day') AS recent
                       FROM v_transcript_gaps g ORDER BY g.target_date DESC, g.symbol""", (RETRY_DAYS,))
        rows = cur.fetchall()
    return len(rows), [(s, d) for s, d, recent in rows if not recent]


def _issuer(c, symbol: str) -> str | None:
    with c.cursor() as cur:
        cur.execute("SELECT name FROM company_map WHERE symbol = %s", (symbol,))
        row = cur.fetchone()
    return row[0] if row else None


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


def _write(c, symbol: str, url: str, fy: int, fq: int, call_date: date | None, parsed: dict, payload) -> int:
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
            (symbol, fy, fq, call_date, call_date, url, json.dumps(payload), raw_text, len(raw_text.split())))
        tid = cur.fetchone()[0]
        _write_segments(cur, tid, parsed["turns"])
    c.commit()
    return 1 + len(parsed["turns"])


# -------------------------------------------------------------------------------- job
def run(c, limit: int | None = None, refresh: bool = True, client: Equibles | None = None) -> None:
    """Fill transcript gaps from Equibles, newest first; `limit` caps the gaps looked at."""
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
            listings: dict[str, object] = {}
            filled = missed = 0
            stop = None
            for symbol, target in todo:
                try:
                    if symbol not in listings:
                        listings[symbol] = eq.get(LIST_PATH.format(ticker=symbol))
                    listing = listings[symbol]
                    if listing is None:
                        missed += 1
                        _attempt(c, symbol, target, False, "ticker not found (404 on earnings-call list)")
                        continue
                    ref = nearest_call(listing, target)
                    if not ref:
                        missed += 1
                        _attempt(c, symbol, target, False, f"no listed call within {MATCH_DAYS} days")
                        continue
                    fy, fq = ref["fy"], ref["fq"]
                    if _have(c, symbol, fy, fq):
                        missed += 1
                        _attempt(c, symbol, target, False, f"FY{fy} Q{fq} ({ref['date']}) already stored from equibles")
                        continue
                    path = CALL_PATH.format(ticker=symbol, fy=fy, fq=fq)
                    payload = eq.get(path)
                    if payload is None:
                        missed += 1
                        _attempt(c, symbol, target, False, f"FY{fy} Q{fq} ({ref['date']}): no transcript (404)")
                        continue
                    parsed = parse_call(payload, _issuer(c, symbol))
                    call_date = ref["date"] or parsed["call_date"]
                    stats["rows"] += _write(c, symbol, BASE_URL + path, fy, fq, call_date, parsed, payload)
                    filled += 1
                    _attempt(c, symbol, target, True, f"stored FY{fy} Q{fq}, call {call_date}, {len(parsed['turns'])} turns")
                    log.info("equibles: %s gap %s filled: FY%d Q%d, call %s, %d turns, %d words", symbol, target, fy, fq,
                             call_date, len(parsed["turns"]), len(parsed["raw_text"].split()))
                except QuotaExhausted as e:
                    c.rollback()
                    stop = f"stopped: {e}"
                    break
                except ShapeError as e:
                    c.rollback()
                    stop = f"stopped on an unreadable response for {symbol} {target} (no attempt recorded): {e}"
                    break
                except Exception as e:  # noqa: BLE001
                    c.rollback()
                    missed += 1
                    log.warning("equibles: %s gap %s failed: %s", symbol, target, e)
                    _attempt(c, symbol, target, False, f"{type(e).__name__}: {e}")
            if stop:
                log.warning("equibles: %s", stop)
            log.info("equibles: done: %d gaps, %d filled, %d still missing, %d not reached; %d/%d requests used",
                     total, filled, total - filled, len(todo) - filled - missed, eq.used, eq.budget)
    finally:
        if client is None:
            eq.close()
    if refresh:
        db.refresh_word_counts(c)
