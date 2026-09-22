"""SEC EDGAR 8-K earnings press releases (Item 2.02, Exhibit 99.1).

For every company with an earnings-mention market: resolve its CIK from the SEC ticker file,
list recent submissions, keep 8-Ks carrying Item 2.02 filed in the last N days, and store the
Exhibit 99.1 text in kalshi.filings. Every request carries the descriptive User-Agent the SEC
requires (KalshiWorker/1.0 with SEC_CONTACT_EMAIL) and is throttled to 8 requests/second.
"""
from __future__ import annotations

import html
import logging
import os
import re
import time
from datetime import date, datetime, timedelta, timezone

import httpx

from . import db

log = logging.getLogger(__name__)

TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
SUBMISSIONS_URL = "https://data.sec.gov/submissions/{name}"
ARCHIVE_URL = "https://www.sec.gov/Archives/edgar/data/{cik}/{acc}/"
DEFAULT_DAYS = 3
RPS = 8.0
EARNINGS_ITEM = "2.02"

HREF_RE = re.compile(r'href="([^"]+)"', re.I)
EXHIBIT_RE = re.compile(r"(?:ex(?:hibit)?[-_]?99|99[.\-_]1)", re.I)
EXHIBIT_991_RE = re.compile(r"(?:ex(?:hibit)?[-_]?99[-_.]?1\b|99\.1)", re.I)
DOC_EXT_RE = re.compile(r"\.(?:htm|html|txt)$", re.I)
DROP_BLOCKS_RE = re.compile(r"<(script|style|head|title|ix:header)\b.*?</\1>", re.S | re.I)
BLOCK_TAG_RE = re.compile(r"</?(?:p|div|br|li|ul|ol|h\d|table|blockquote|section|article)\b[^>]*>", re.I)
ROW_OPEN_RE = re.compile(r"<tr\b[^>]*>", re.I)              # a row ends with a newline, starts silently
ROW_CLOSE_RE = re.compile(r"</tr\s*>", re.I)
CELL_TAG_RE = re.compile(r"</?(?:td|th)\b[^>]*>", re.I)   # table cells stay on one line


def user_agent() -> str:
    email = os.getenv("SEC_CONTACT_EMAIL")
    if not email:
        raise RuntimeError("SEC_CONTACT_EMAIL is not set; the SEC requires a contact address in the User-Agent")
    return f"KalshiWorker/1.0 (contact: {email})"


class _Throttle:
    """Simple token bucket: at most `rps` requests per second."""

    def __init__(self, rps: float):
        self.rps, self.tokens, self.last = rps, rps, time.monotonic()

    def take(self) -> None:
        now = time.monotonic()
        self.tokens = min(self.rps, self.tokens + (now - self.last) * self.rps)
        self.last = now
        if self.tokens < 1:
            time.sleep((1 - self.tokens) / self.rps)
            self.tokens, self.last = 0.0, time.monotonic()   # the sleep paid for this request
        else:
            self.tokens -= 1


class Edgar:
    def __init__(self, http: httpx.Client | None = None, rps: float = RPS):
        self.http = http or httpx.Client(timeout=30.0, follow_redirects=True,
                                         headers={"User-Agent": user_agent(), "Accept-Encoding": "gzip, deflate"})
        self.throttle = _Throttle(rps)

    def get(self, url: str, retries: int = 3) -> httpx.Response:
        for attempt in range(retries):
            self.throttle.take()
            try:
                r = self.http.get(url)
            except httpx.TransportError as e:
                if attempt == retries - 1:
                    raise
                log.warning("transport error %s on %s (attempt %d)", e, url, attempt + 1)
                time.sleep(2 ** attempt)
                continue
            if r.status_code in (429, 500, 502, 503, 504) and attempt < retries - 1:
                wait = float(r.headers.get("Retry-After", 2 ** attempt))
                log.warning("HTTP %s on %s; sleeping %.1fs", r.status_code, url, wait)
                time.sleep(wait)
                continue
            r.raise_for_status()
            return r
        raise RuntimeError(f"gave up on {url}")

    def json(self, url: str) -> dict:
        return self.get(url).json()

    def text(self, url: str) -> str:
        return self.get(url).text


# ---------------------------------------------------------------------------- helpers
def _norm(symbol: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", (symbol or "").upper())


def cik_map(tickers_json: dict) -> dict[str, tuple[str, str]]:
    """company_tickers.json -> {normalized ticker: (10-digit CIK, company name)}."""
    out: dict[str, tuple[str, str]] = {}
    rows = tickers_json.values() if isinstance(tickers_json, dict) else tickers_json
    for row in rows:
        t = _norm(row.get("ticker", ""))
        if t and t not in out:
            out[t] = (str(row["cik_str"]).zfill(10), row.get("title") or "")
    return out


def recent_filings(submissions: dict) -> list[dict]:
    """Flatten filings.recent (parallel arrays) into one dict per filing."""
    rec = (submissions.get("filings") or {}).get("recent") or {}
    keys = ("accessionNumber", "filingDate", "reportDate", "acceptanceDateTime", "form", "items",
            "primaryDocument", "primaryDocDescription")
    n = len(rec.get("accessionNumber") or [])
    return [{k: (rec.get(k) or [None] * n)[i] for k in keys} for i in range(n)]


def earnings_8ks(filings: list[dict], since: date) -> list[dict]:
    """8-Ks with Item 2.02 filed on or after `since`."""
    out = []
    for f in filings:
        items = [x.strip() for x in (f.get("items") or "").split(",") if x.strip()]
        if f.get("form") != "8-K" or EARNINGS_ITEM not in items or not f.get("filingDate"):
            continue
        if date.fromisoformat(f["filingDate"]) < since:
            continue
        out.append({**f, "item_list": items})
    return out


def pick_exhibit(index_html: str, primary_document: str | None) -> str | None:
    """Filename of the Exhibit 99.1 document in an archive directory listing: prefer names
    containing 'ex99'/'99.1' (a 99.1 over other 99.x), else the filing's primaryDocument."""
    names = []
    for href in HREF_RE.findall(index_html):
        name = html.unescape(href).rsplit("/", 1)[-1]
        if DOC_EXT_RE.search(name) and name not in names:
            names.append(name)
    for pattern in (EXHIBIT_991_RE, EXHIBIT_RE):
        hits = [n for n in names if pattern.search(n)]
        if hits:
            return hits[0]
    return primary_document or None


def html_to_text(doc: str) -> str:
    """Filing HTML (or plain text) -> readable text: scripts, styles and inline-XBRL headers
    dropped, block tags become line breaks (table cells stay on their row), inline tags vanish,
    entities decoded."""
    doc = DROP_BLOCKS_RE.sub(" ", doc)
    doc = CELL_TAG_RE.sub(" ", doc)
    doc = ROW_OPEN_RE.sub("", doc)
    doc = ROW_CLOSE_RE.sub("\n", doc)
    doc = BLOCK_TAG_RE.sub("\n", doc)
    doc = re.sub(r"<[^>]+>", "", doc)
    doc = html.unescape(doc).replace("\xa0", " ")
    doc = re.sub(r"[ \t\r\f\v]+", " ", doc)
    doc = re.sub(r" *\n *", "\n", doc)
    return re.sub(r"\n{3,}", "\n\n", doc).strip()


def _parse_ts(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)
    except ValueError:
        return None


def _parse_date(value: str | None) -> date | None:
    try:
        return date.fromisoformat(value) if value else None
    except ValueError:
        return None


# --------------------------------------------------------------------------------- db
def _symbols(c) -> list[str]:
    with c.cursor() as cur:
        cur.execute("""SELECT DISTINCT regexp_replace(series_ticker, '^KXEARNINGSMENTION', '')
                       FROM markets WHERE series_ticker LIKE 'KXEARNINGSMENTION%' ORDER BY 1""")
        return [r[0] for r in cur.fetchall() if r[0]]


def _upsert_company(c, symbol: str, cik: str, name: str) -> None:
    with c.cursor() as cur:
        cur.execute("""INSERT INTO company_map (symbol, cik, name, updated_at) VALUES (%s, %s, %s, now())
                       ON CONFLICT (symbol) DO UPDATE SET cik = EXCLUDED.cik, name = EXCLUDED.name, updated_at = now()""",
                    (symbol, cik, name))


def _known_accessions(c, symbol: str) -> set[str]:
    with c.cursor() as cur:
        cur.execute("SELECT accession FROM filings WHERE symbol = %s", (symbol,))
        return {r[0] for r in cur.fetchall()}


def _insert_filing(c, row: dict) -> int:
    with c.cursor() as cur:
        cur.execute("""INSERT INTO filings (accession, cik, symbol, form, items, filed_at, period, exhibit_url,
                                            raw_text, word_count)
                       VALUES (%(accession)s, %(cik)s, %(symbol)s, %(form)s, %(items)s, %(filed_at)s, %(period)s,
                               %(exhibit_url)s, %(raw_text)s, %(word_count)s)
                       ON CONFLICT (accession) DO NOTHING""", row)
        return cur.rowcount


# -------------------------------------------------------------------------------- job
def run(c, days: int = DEFAULT_DAYS, edgar: Edgar | None = None) -> None:
    edgar = edgar or Edgar()
    since = date.today() - timedelta(days=days)
    with db.run_log(c, "filings") as stats:
        # 1. symbols
        symbols = _symbols(c)
        log.info("filings: %d symbols with earnings-mention markets", len(symbols))

        # 2. CIKs
        ciks = cik_map(edgar.json(TICKERS_URL))
        resolved: dict[str, tuple[str, str]] = {}
        for s in symbols:
            hit = ciks.get(_norm(s))
            if hit:
                resolved[s] = hit
                _upsert_company(c, s, hit[0], hit[1])
        c.commit()
        unresolved = sorted(set(symbols) - set(resolved))
        log.info("filings: %d/%d symbols resolved to a CIK%s", len(resolved), len(symbols),
                 f"; unresolved: {', '.join(unresolved)}" if unresolved else "")

        # 3 + 4. submissions -> new 8-K 2.02 filings -> Exhibit 99.1 text
        candidates = new = failed = 0
        for i, (symbol, (cik, _name)) in enumerate(sorted(resolved.items()), 1):
            try:
                subs = edgar.json(SUBMISSIONS_URL.format(name=f"CIK{cik}.json"))
            except Exception as e:  # noqa: BLE001
                failed += 1
                log.warning("filings: %s submissions failed: %s", symbol, e)
                continue
            known = _known_accessions(c, symbol)
            todo = [f for f in earnings_8ks(recent_filings(subs), since) if f["accessionNumber"] not in known]
            candidates += len(todo)
            for f in todo:
                acc = f["accessionNumber"]
                try:
                    folder = ARCHIVE_URL.format(cik=int(cik), acc=acc.replace("-", ""))
                    doc = pick_exhibit(edgar.text(folder), f.get("primaryDocument"))
                    if not doc:
                        raise ValueError("no exhibit or primary document in archive index")
                    exhibit_url = folder + doc
                    text = html_to_text(edgar.text(exhibit_url))
                    filed_at = _parse_ts(f.get("acceptanceDateTime")) or _parse_ts(f.get("filingDate"))
                    if not filed_at:
                        raise ValueError(f"no usable acceptanceDateTime/filingDate ({f.get('filingDate')!r})")
                    new += _insert_filing(c, {
                        "accession": acc, "cik": cik, "symbol": symbol, "form": f["form"], "items": f["item_list"],
                        "filed_at": filed_at, "period": _parse_date(f.get("reportDate")), "exhibit_url": exhibit_url,
                        "raw_text": text, "word_count": len(text.split())})
                    c.commit()
                    stats["rows"] += 1
                    log.info("filings: %s %s filed %s -> %s (%d words)", symbol, acc, f["filingDate"], doc, len(text.split()))
                except Exception as e:  # noqa: BLE001
                    c.rollback()
                    failed += 1
                    log.warning("filings: %s %s failed: %s", symbol, acc, e)
            if i % 25 == 0 or i == len(resolved):
                log.info("filings: %d/%d symbols done; %d candidate filings, %d stored, %d failed",
                         i, len(resolved), candidates, new, failed)
        log.info("filings: done (last %d days): %d symbols, %d resolved, %d new 8-K 2.02 candidates, %d stored, %d failed",
                 days, len(symbols), len(resolved), candidates, new, failed)
