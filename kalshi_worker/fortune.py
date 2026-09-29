"""Fortune.com (Quartr) earnings-call transcripts: a second transcript source next to the Fool.

1. Slug discovery: for tracked KXEARNINGSMENTION symbols in company_map without a fortune_slug
   (unchecked, or last checked over 30 days ago), try slugs built from the company name against
   https://fortune.com/company/<slug>/ and keep the first whose page lists the symbol as its
   ticker. fortune_checked_at is set either way.
2. Calls: for every company with a slug, read the earnings reports listed on its page and fetch
   each one not yet in transcript_sources (plus ones still pending). A report whose transcript
   has no paragraphs yet is queued as pending and retried next run; otherwise the paragraphs
   become a transcripts row (source='fortune') and speaker-merged transcript_segments.

Pages are Next.js: the data sits in <script id="__NEXT_DATA__">. One request per second, the
same browser User-Agent as the Fool fetcher, 3 attempts with backoff on 429/5xx.
"""
from __future__ import annotations

import json
import logging
import re
import time
from datetime import date
from urllib.parse import urljoin

import httpx

from . import db, tickers
from .transcripts import USER_AGENT, _mention_symbols, _write_segments, fetch

log = logging.getLogger(__name__)

BASE_URL = "https://fortune.com"
COMPANY_URL = BASE_URL + "/company/{slug}/"
REQUEST_DELAY_S = 1.0
RECHECK_DAYS = 30
NEXT_DATA_RE = re.compile(r'<script[^>]*id="__NEXT_DATA__"[^>]*>(.*?)</script>', re.S | re.I)
REPORT_SLUG_RE = re.compile(r"\bq([1-4])-(\d{4})\b", re.I)
CORPORATE_WORDS = {"inc", "corp", "co", "company", "ltd", "plc", "nv", "sa", "the"}
GROUP_WORDS = {"holdings", "group", "technologies"}
TICKER_ALIASES = {"GOOGL": {"GOOG"}}     # Kalshi symbol -> other tickers Fortune may list


# ------------------------------------------------------------------------------ http
class Fortune:
    """Browser-UA client paced to one request per second (retries come from transcripts.fetch)."""

    def __init__(self, http: httpx.Client | None = None, delay: float = REQUEST_DELAY_S):
        self.http = http or httpx.Client(timeout=30.0, follow_redirects=True, headers={
            "User-Agent": USER_AGENT, "Accept": "text/html,application/xhtml+xml", "Accept-Language": "en-US,en;q=0.9"})
        self.delay, self.last = delay, 0.0

    def page_data(self, url: str) -> dict:
        wait = self.last + self.delay - time.monotonic()
        if wait > 0:
            time.sleep(wait)
        try:
            return next_data(fetch(self.http, url))
        finally:
            self.last = time.monotonic()

    def close(self) -> None:
        self.http.close()


def next_data(page: str) -> dict:
    m = NEXT_DATA_RE.search(page)
    if not m:
        raise ValueError("no __NEXT_DATA__ script in page")
    return json.loads(m.group(1))


def _page_props(data: dict) -> dict:
    return (data.get("props") or {}).get("pageProps") or {}


# ---------------------------------------------------------------------------- slugs
def slug_candidates(name: str | None) -> list[str]:
    """Company name -> candidate slugs, in order: all words; minus corporate words (inc, corp,
    co, ...); also minus holdings/group/technologies; first two words; first word. '&' and '-'
    split words, '/XX/' state suffixes and other punctuation are dropped."""
    n = re.sub(r"/[a-z]+/?", " ", (name or "").lower())
    n = re.sub(r"[&\-]", " ", n)
    words = re.sub(r"[^a-z0-9\s]", "", n).split()
    core = [w for w in words if w not in CORPORATE_WORDS]
    short = [w for w in core if w not in GROUP_WORDS]
    out: list[str] = []
    for ws in (words, core, short, core[:2], core[:1]):
        slug = "-".join(ws)
        if slug and slug not in out:
            out.append(slug)
    return out


def ticker_matches(ticker: str | None, expected: str) -> bool:
    """Fortune's companyInfo.Ticker against the company's exchange ticker (company_map.ticker,
    not the Kalshi symbol: ADBE for ADOBE), ignoring '.'; GOOGL also takes GOOG."""
    t = (ticker or "").replace(".", "").upper()
    s = expected.replace(".", "").upper()
    return bool(t) and (t == s or t in TICKER_ALIASES.get(s, set()))


def _slug_todo(c, symbols: list[str], force: bool) -> list[tuple[str, str, str | None]]:
    with c.cursor() as cur:
        cur.execute(f"""SELECT symbol, name, ticker FROM company_map
                        WHERE symbol = ANY(%s) AND fortune_slug IS NULL AND coalesce(name, '') <> ''
                          {"" if force else "AND (fortune_checked_at IS NULL OR fortune_checked_at < now() - %s * interval '1 day')"}
                        ORDER BY symbol""", (symbols,) if force else (symbols, RECHECK_DAYS))
        return cur.fetchall()


def discover_slugs(f: Fortune, c, symbols: list[str], force: bool = False) -> tuple[int, int]:
    """Find fortune_slug for companies without one. Returns (checked, found)."""
    todo = _slug_todo(c, symbols, force)
    found = 0
    for symbol, name, ticker in todo:
        ticker = ticker or tickers.alias(symbol)
        hit = None
        for cand in slug_candidates(name):
            try:
                info = (_page_props(f.page_data(COMPANY_URL.format(slug=cand))).get("company") or {}).get("companyInfo") or {}
            except httpx.HTTPStatusError as e:
                if e.response.status_code != 404:
                    log.warning("fortune: %s slug %r: %s", symbol, cand, e)
                continue
            except Exception as e:  # noqa: BLE001
                log.warning("fortune: %s slug %r: %s", symbol, cand, e)
                continue
            if ticker_matches(info.get("Ticker") or info.get("ticker"), ticker):
                hit = cand
                break
        with c.cursor() as cur:
            cur.execute("""UPDATE company_map SET fortune_slug = coalesce(%s, fortune_slug), fortune_checked_at = now()
                           WHERE symbol = %s""", (hit, symbol))
        c.commit()
        found += hit is not None
        log.info("fortune: %s (%s, ticker %s): %s", symbol, name, ticker, f"slug {hit!r}" if hit else "no slug matched")
    return len(todo), found


# ---------------------------------------------------------------------------- parse
def fiscal_from_slug(slug: str | None) -> tuple[int, int] | None:
    """'q4-2026' -> (2026, 4). Fortune uses fiscal labels (STZ's July 2026 call is q1-2027)."""
    m = REPORT_SLUG_RE.search(slug or "")
    return (int(m.group(2)), int(m.group(1))) if m else None


def parse_report(report: dict) -> dict:
    """props.pageProps.earningsReport -> {"call_date", "paragraphs", "turns", "raw_text"}.
    Roles: speaker_role 'Analyst' -> analyst; 'operator' in name or role -> operator; else exec.
    Section: qa from audio.qna seconds on when it's given, otherwise from the first analyst
    paragraph on. Consecutive paragraphs from one speaker in one section merge into one turn."""
    tr = report.get("transcript") or {}
    speakers = {m.get("speaker_id"): m for m in tr.get("speaker_mapping") or []}
    qna = (report.get("audio") or {}).get("qna")
    call_date = None
    if report.get("date"):
        try:
            call_date = date.fromisoformat(str(report["date"])[:10])
        except ValueError:
            call_date = None
    paragraphs = [p for p in tr.get("paragraphs") or [] if (p.get("text") or "").strip()]
    turns: list[dict] = []
    in_qa = False
    for p in paragraphs:
        sid = p.get("speaker")
        sp = speakers.get(sid) or {}
        name = (sp.get("speaker_name") or "").strip() or f"Speaker {sid}"
        role_raw = (sp.get("speaker_role") or "").strip()
        if role_raw.lower() == "analyst":
            role = "analyst"
        elif "operator" in name.lower() or "operator" in role_raw.lower():
            role = "operator"
        else:
            role = "exec"
        if qna is not None:
            section = "qa" if p.get("start") is not None and float(p["start"]) >= float(qna) else "prepared"
        else:
            in_qa = in_qa or role == "analyst"
            section = "qa" if in_qa else "prepared"
        text = p["text"].strip()
        prev = turns[-1] if turns else None
        if prev and prev["speaker_id"] == sid and prev["section"] == section:
            prev["text"] = f"{prev['text']} {text}"
            continue
        title = " - ".join(x for x in (role_raw, (sp.get("speaker_company") or "").strip()) if x) or None
        turns.append({"speaker_id": sid, "speaker": name, "speaker_title": title, "role": role,
                      "section": section, "text": text})
    raw_text = "\n\n".join(p["text"].strip() for p in paragraphs)
    return {"call_date": call_date, "paragraphs": paragraphs, "turns": turns, "raw_text": raw_text}


# --------------------------------------------------------------------------------- db
def _companies(c, symbols: list[str]) -> list[tuple[str, str]]:
    with c.cursor() as cur:
        cur.execute("""SELECT symbol, fortune_slug FROM company_map
                       WHERE symbol = ANY(%s) AND fortune_slug IS NOT NULL ORDER BY symbol""", (symbols,))
        return cur.fetchall()


def _known(c, symbol: str) -> dict[str, str]:
    """{url: status} of this symbol's Fortune source rows."""
    with c.cursor() as cur:
        cur.execute("SELECT url, status FROM transcript_sources WHERE source = 'fortune' AND symbol = %s", (symbol,))
        return dict(cur.fetchall())


def _upsert_source(cur, symbol: str, url: str, fiscal: tuple[int, int] | None, call_date: date | None,
                   status: str, error: str | None = None) -> None:
    fy, fq = fiscal or (None, None)
    cur.execute("""INSERT INTO transcript_sources (symbol, source, url, fiscal_year, fiscal_quarter, call_date,
                                                   published_date, status, error, fetched_at)
                   VALUES (%s, 'fortune', %s, %s, %s, %s, %s, %s, %s, now())
                   ON CONFLICT (url) DO UPDATE SET
                     fiscal_year = coalesce(EXCLUDED.fiscal_year, transcript_sources.fiscal_year),
                     fiscal_quarter = coalesce(EXCLUDED.fiscal_quarter, transcript_sources.fiscal_quarter),
                     call_date = coalesce(EXCLUDED.call_date, transcript_sources.call_date),
                     published_date = coalesce(EXCLUDED.published_date, transcript_sources.published_date),
                     status = EXCLUDED.status, error = EXCLUDED.error, fetched_at = now()""",
                (symbol, url, fy, fq, call_date, call_date, status, error))


def _write(c, symbol: str, url: str, fiscal: tuple[int, int], parsed: dict, transcript: dict) -> int:
    raw_text = parsed["raw_text"]
    with c.cursor() as cur:
        _upsert_source(cur, symbol, url, fiscal, parsed["call_date"], "parsed")
        cur.execute("""
            INSERT INTO transcripts (symbol, fiscal_year, fiscal_quarter, call_date, published_date, source,
                                     source_url, raw_html, raw_text, word_count, parsed_at)
            VALUES (%s,%s,%s,%s,%s,'fortune',%s,%s,%s,%s,now())
            ON CONFLICT (symbol, fiscal_year, fiscal_quarter, source) DO UPDATE SET
              call_date=EXCLUDED.call_date, published_date=EXCLUDED.published_date,
              source_url=EXCLUDED.source_url, raw_html=EXCLUDED.raw_html,
              raw_text=EXCLUDED.raw_text, word_count=EXCLUDED.word_count, parsed_at=now()
            RETURNING id""",
            (symbol, fiscal[0], fiscal[1], parsed["call_date"], parsed["call_date"], url, json.dumps(transcript),
             raw_text, len(raw_text.split())))
        tid = cur.fetchone()[0]
        _write_segments(cur, tid, parsed["turns"])
    c.commit()
    return 1 + len(parsed["turns"])


def _fail(c, symbol: str, url: str, fiscal: tuple[int, int] | None, err: str) -> None:
    c.rollback()
    with c.cursor() as cur:
        _upsert_source(cur, symbol, url, fiscal, None, "failed", err[:2000])
    c.commit()


# -------------------------------------------------------------------------------- job
def run(c, symbol: str | None = None, limit: int | None = None, refresh: bool = True) -> None:
    """Slug discovery, then new / pending transcripts for every company with a slug. `symbol`
    limits both steps to one company (and re-checks its slug regardless of the 30-day wait);
    `limit` caps the number of report pages fetched."""
    f = Fortune()
    try:
        with db.run_log(c, "fortune") as stats:
            symbols = [symbol] if symbol else sorted(set(_mention_symbols(c).values()))
            checked, found = discover_slugs(f, c, symbols, force=bool(symbol))
            companies = _companies(c, symbols)
            log.info("fortune: slugs: %d checked, %d found; %d companies with a slug", checked, found, len(companies))
            seen = stored = pending = failed = fetched = 0
            for sym, slug in companies:
                if limit is not None and fetched >= limit:
                    break
                try:
                    reports = (_page_props(f.page_data(COMPANY_URL.format(slug=slug))).get("company") or {}) \
                        .get("earningReports") or []
                except Exception as e:  # noqa: BLE001
                    log.warning("fortune: %s company page (%s) failed: %s", sym, slug, e)
                    continue
                seen += len(reports)
                known = _known(c, sym)
                for rep in reports:
                    if not rep.get("permalink"):
                        continue
                    url = urljoin(BASE_URL + "/", rep["permalink"])
                    if known.get(url, "pending") != "pending":
                        continue
                    if limit is not None and fetched >= limit:
                        break
                    fetched += 1
                    fiscal = fiscal_from_slug(rep.get("slug")) or fiscal_from_slug(url)
                    try:
                        if not fiscal:
                            raise ValueError(f"no fiscal quarter in report slug {rep.get('slug')!r}")
                        report = _page_props(f.page_data(url)).get("earningsReport") or {}
                        parsed = parse_report(report)
                        if not parsed["paragraphs"]:
                            with c.cursor() as cur:
                                _upsert_source(cur, sym, url, fiscal, parsed["call_date"], "pending")
                            c.commit()
                            pending += 1
                            log.info("fortune: %s Q%d %d: transcript not posted yet (%s)", sym, fiscal[1], fiscal[0], url)
                            continue
                        stats["rows"] += _write(c, sym, url, fiscal, parsed, report.get("transcript") or {})
                        stored += 1
                        log.info("fortune: %s Q%d %d stored: %d paragraphs, %d turns, call date %s (%s)", sym,
                                 fiscal[1], fiscal[0], len(parsed["paragraphs"]), len(parsed["turns"]),
                                 parsed["call_date"], url)
                    except Exception as e:  # noqa: BLE001
                        failed += 1
                        log.warning("fortune: %s %s failed: %s", sym, url, e)
                        _fail(c, sym, url, fiscal, f"{type(e).__name__}: {e}")
            log.info("fortune: done: %d slugs found (%d checked), %d reports seen across %d companies, "
                     "%d transcripts stored, %d pending, %d failed%s", found, checked, seen, len(companies),
                     stored, pending, failed, f" (limit {limit})" if limit is not None else "")
    finally:
        f.close()
    if refresh:
        db.refresh_word_counts(c)
