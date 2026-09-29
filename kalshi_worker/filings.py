"""SEC EDGAR 8-K earnings press releases (Item 2.02, Exhibit 99.1).

For every company with an earnings-mention market: resolve its CIK from the SEC ticker file,
list its submissions, keep 8-Ks carrying Item 2.02 filed in the last N days, and store the
Exhibit 99.1 text in kalshi.filings. Foreign filers report earnings on Form 6-K, which has no
item numbers: a 6-K is kept when its Exhibit 99.1 (or primary document) announces results in
its first 1,500 characters, and stored with form '6-K' and no items. A symbol without a
filings_backfill:<SYMBOL> row in sync_state is looked at over the last three years once (older
submissions pages included), then marked. Every request carries the descriptive User-Agent the
SEC requires (KalshiWorker/1.0 with SEC_CONTACT_EMAIL) and is throttled to 8 requests/second.
"""
from __future__ import annotations

import html
import logging
import os
import re
import time
from collections import Counter
from datetime import date, datetime, timedelta, timezone

import httpx

from . import db, tickers

log = logging.getLogger(__name__)

TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
SUBMISSIONS_URL = "https://data.sec.gov/submissions/{name}"
ARCHIVE_URL = "https://www.sec.gov/Archives/edgar/data/{cik}/{acc}/"
DEFAULT_DAYS = 3
RPS = 8.0
EARNINGS_ITEM = "2.02"
FOREIGN_FORM = "6-K"
RESULTS_HEAD = 1500             # a 6-K counts as an earnings release when this much of it announces results
RESULTS_RE = re.compile(r"(?:first|second|third|fourth|q[1-4]).{0,40}(?:quarter|results)"
                        r"|results for the (?:quarter|three months|fiscal)", re.I | re.S)
BACKFILL_DAYS = 3 * 365
BACKFILL_STATE = "filings_backfill:"          # sync_state job per symbol whose 3-year backfill ran
EVENT_FORMS = ("8-K", "8-K/A")
EVENT_ITEMS = ("1.01", "1.02", "2.01", "2.05", "2.06", "5.02")   # agreements, M&A, restructuring, impairment, exec change

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


def _flatten(rec: dict) -> list[dict]:
    """Parallel arrays (filings.recent, or an older submissions page) -> one dict per filing."""
    keys = ("accessionNumber", "filingDate", "reportDate", "acceptanceDateTime", "form", "items",
            "primaryDocument", "primaryDocDescription")
    n = len(rec.get("accessionNumber") or [])
    return [{k: (rec.get(k) or [None] * n)[i] for k in keys} for i in range(n)]


def recent_filings(submissions: dict) -> list[dict]:
    """Flatten filings.recent (parallel arrays) into one dict per filing."""
    return _flatten((submissions.get("filings") or {}).get("recent") or {})


def all_filings(edgar: "Edgar", submissions: dict, since: date) -> list[dict]:
    """filings.recent plus, when it doesn't reach back to `since`, the older submissions pages
    listed in filings.files (CIK##########-submissions-001.json, ...), newest first, until a
    page starts before `since`."""
    out = recent_filings(submissions)
    dates = [f["filingDate"] for f in out if f.get("filingDate")]
    if dates and min(dates) < since.isoformat():
        return out
    pages = sorted((submissions.get("filings") or {}).get("files") or [],
                   key=lambda p: p.get("filingTo") or "", reverse=True)
    for page in pages:
        if (page.get("filingTo") or "9999") < since.isoformat():
            break
        data = edgar.json(SUBMISSIONS_URL.format(name=page["name"]))
        out.extend(_flatten((data.get("filings") or {}).get("recent") or data))   # pages are bare arrays
        if (page.get("filingFrom") or "") < since.isoformat():
            break
    return out


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


def foreign_6ks(filings: list[dict], since: date) -> list[dict]:
    """6-Ks filed on or after `since` (no item numbers; whether one is an earnings release is
    decided from its text, see is_results)."""
    return [{**f, "item_list": []} for f in filings
            if f.get("form") == FOREIGN_FORM and f.get("filingDate") and date.fromisoformat(f["filingDate"]) >= since]


def is_results(text: str) -> bool:
    """True when the first RESULTS_HEAD characters announce quarterly / fiscal results."""
    return bool(RESULTS_RE.search((text or "")[:RESULTS_HEAD]))


def event_8ks(filings: list[dict], since: date) -> list[dict]:
    """8-Ks and 8-K/As carrying any corporate-event item (EVENT_ITEMS) filed on or after `since`."""
    out = []
    for f in filings:
        items = [x.strip() for x in (f.get("items") or "").split(",") if x.strip()]
        if f.get("form") not in EVENT_FORMS or not set(items) & set(EVENT_ITEMS) or not f.get("filingDate"):
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


PARA_SPLIT_RE = re.compile(r"\n\s*\n")
BOILERPLATE_PARA_RE = re.compile(
    r"forward[- ]looking|safe harbor|risks and uncertainties|undertakes? no obligation|private securities litigation", re.I)
NON_GAAP_HEADING_RE = re.compile(r"^\W*(?:use of |reconciliation of )?non-gaap financial measures\W*$", re.I)
CONTACT_HEADING_RE = re.compile(
    r"^\W*(?:contacts?|investor relations|investor contacts?|media contacts?|media relations|press contacts?)\W*$", re.I)
CORPORATE_WORDS = {"the", "inc", "inc.", "corp", "corp.", "corporation", "co", "co.", "company", "plc", "ltd", "ltd.",
                   "llc", "holdings", "group", "&"}


def _about_pattern(company: str | None) -> re.Pattern:
    """'About <Company>' paragraph start: the company's first significant name word (SEC names
    look like 'ADOBE INC.' or 'THE KROGER CO'), or any capitalized word when no name is known."""
    words = [w for w in re.split(r"\s+", (company or "").strip()) if w and w.lower() not in CORPORATE_WORDS]
    if words:
        return re.compile(r"^About\s+" + re.escape(words[0].rstrip(",.")), re.I)
    return re.compile(r"^About\s+[A-Z]")


def _is_heading(para: str) -> bool:
    return "\n" not in para and len(para.split()) <= 8 and not para.rstrip().endswith(".")


def split_boilerplate(text: str, company: str | None = None) -> tuple[str, str]:
    """Paragraph-level filter -> (body, boilerplate). Paragraphs are separated by blank lines.
    A paragraph is boilerplate when it mentions forward-looking statements, safe harbor, risks
    and uncertainties, undertake no obligation or the Private Securities Litigation Reform Act,
    is a "Non-GAAP Financial Measures" heading, or starts with "About <company>" (a bare About
    heading also takes the description paragraph that follows it); a paragraph that is exactly
    a Contact / Investor Relations / Media Contact heading takes everything after it into the
    boilerplate too. body is the kept paragraphs joined, boilerplate the removed ones."""
    about = _about_pattern(company)
    body, boilerplate, after_contact, absorb_next = [], [], False, False
    for para in PARA_SPLIT_RE.split(text or ""):
        para = para.strip()
        if not para:
            continue
        if after_contact or CONTACT_HEADING_RE.match(para):
            after_contact = True
            boilerplate.append(para)
        elif absorb_next:
            boilerplate.append(para)
            absorb_next = False
        elif BOILERPLATE_PARA_RE.search(para) or NON_GAAP_HEADING_RE.match(para) or about.match(para):
            boilerplate.append(para)
            absorb_next = bool(about.match(para) and _is_heading(para))
        else:
            body.append(para)
    return "\n\n".join(body), "\n\n".join(boilerplate)


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


def _upsert_company(c, symbol: str, ticker: str, cik: str | None, name: str | None) -> None:
    """company_map row for a Kalshi symbol. An existing ticker is kept (company_map is the source
    of truth once set); cik/name are only overwritten with non-null values."""
    with c.cursor() as cur:
        cur.execute("""INSERT INTO company_map (symbol, ticker, cik, name, updated_at) VALUES (%s, %s, %s, %s, now())
                       ON CONFLICT (symbol) DO UPDATE SET ticker = coalesce(company_map.ticker, EXCLUDED.ticker),
                         cik = coalesce(EXCLUDED.cik, company_map.cik), name = coalesce(EXCLUDED.name, company_map.name),
                         updated_at = now()""",
                    (symbol, ticker, cik, name))


def _update_sic(c, symbol: str, sic: str | None, sic_description: str | None) -> None:
    with c.cursor() as cur:
        cur.execute("UPDATE company_map SET sic = %s, sic_description = %s, updated_at = now() WHERE symbol = %s",
                    (sic or None, sic_description or None, symbol))


def _known_events(c, symbol: str) -> set[str]:
    with c.cursor() as cur:
        cur.execute("SELECT accession FROM corporate_events WHERE symbol = %s", (symbol,))
        return {r[0] for r in cur.fetchall()}


def _upsert_event(c, row: dict) -> None:
    with c.cursor() as cur:
        cur.execute("""INSERT INTO corporate_events (accession, symbol, cik, form, items, filed_at, doc_url, raw_text)
                       VALUES (%(accession)s, %(symbol)s, %(cik)s, %(form)s, %(items)s, %(filed_at)s, %(doc_url)s,
                               %(raw_text)s)
                       ON CONFLICT (accession) DO UPDATE SET
                         symbol = EXCLUDED.symbol, cik = EXCLUDED.cik, form = EXCLUDED.form, items = EXCLUDED.items,
                         filed_at = EXCLUDED.filed_at, doc_url = EXCLUDED.doc_url, raw_text = EXCLUDED.raw_text,
                         fetched_at = now()""", row)


def _store_events(edgar: "Edgar", c, symbol: str, cik: str, filings: list[dict], since: date,
                  counts: "Counter[str]") -> tuple[int, int, int]:
    """Corporate-event 8-Ks for one company: returns (candidates, stored, failed); `counts` gets
    one tick per stored filing for each EVENT_ITEMS code it carries."""
    known = _known_events(c, symbol)
    todo = [f for f in event_8ks(filings, since) if f["accessionNumber"] not in known]
    stored = failed = 0
    for f in todo:
        acc = f["accessionNumber"]
        try:
            if not f.get("primaryDocument"):
                raise ValueError("no primaryDocument")
            doc_url = ARCHIVE_URL.format(cik=int(cik), acc=acc.replace("-", "")) + f["primaryDocument"]
            text = html_to_text(edgar.text(doc_url))
            filed_at = _parse_ts(f.get("acceptanceDateTime")) or _parse_ts(f.get("filingDate"))
            if not filed_at:
                raise ValueError(f"no usable acceptanceDateTime/filingDate ({f.get('filingDate')!r})")
            _upsert_event(c, {"accession": acc, "symbol": symbol, "cik": cik, "form": f["form"],
                              "items": f["item_list"], "filed_at": filed_at, "doc_url": doc_url, "raw_text": text})
            c.commit()
            stored += 1
            hit = [i for i in f["item_list"] if i in EVENT_ITEMS]
            counts.update(hit)
            log.info("filings: %s %s %s items %s filed %s (%d words)", symbol, f["form"], acc, ",".join(hit),
                     f["filingDate"], len(text.split()))
        except Exception as e:  # noqa: BLE001
            c.rollback()
            failed += 1
            log.warning("filings: %s event %s failed: %s", symbol, acc, e)
    return len(todo), stored, failed


def _known_accessions(c, symbol: str) -> set[str]:
    with c.cursor() as cur:
        cur.execute("SELECT accession FROM filings WHERE symbol = %s", (symbol,))
        return {r[0] for r in cur.fetchall()}


def _uncovered(c, symbols: list[str]) -> list[str]:
    """Tracked symbols with no stored filings."""
    with c.cursor() as cur:
        cur.execute("""SELECT s.sym FROM unnest(%s::text[]) s(sym)
                       WHERE NOT EXISTS (SELECT 1 FROM filings f WHERE f.symbol = s.sym) ORDER BY 1""", (symbols,))
        return [r[0] for r in cur.fetchall()]


def _insert_filing(c, row: dict) -> int:
    with c.cursor() as cur:
        cur.execute("""INSERT INTO filings (accession, cik, symbol, form, items, filed_at, period, exhibit_url,
                                            raw_text, word_count, body_text, boilerplate_text)
                       VALUES (%(accession)s, %(cik)s, %(symbol)s, %(form)s, %(items)s, %(filed_at)s, %(period)s,
                               %(exhibit_url)s, %(raw_text)s, %(word_count)s, %(body_text)s, %(boilerplate_text)s)
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

        # 2. company_map rows (ticker = company_map.ticker or the alias) and CIKs looked up by ticker;
        #    TSX-only listings keep a null CIK. Filings are fetched once per ticker, under its
        #    canonical Kalshi symbol (ADBE, not the older ADOBE series), so accession-keyed rows
        #    don't flip between symbols.
        ciks = cik_map(edgar.json(TICKERS_URL))
        sym_tickers = {s: t for s, t in tickers.mention_tickers(c).items() if s in symbols}
        found: dict[str, tuple[str, str]] = {}
        for s, t in sorted(sym_tickers.items()):
            if s in tickers.TSX_ONLY:
                _upsert_company(c, s, t, None, tickers.TSX_ONLY[s])
                continue
            hit = ciks.get(_norm(t))
            _upsert_company(c, s, t, hit[0] if hit else None, hit[1] if hit else None)
            if hit:
                found[s] = hit
        c.commit()
        canonical = tickers.canonical(sym_tickers)
        resolved = {s: hit for s, hit in found.items() if s in canonical}
        shared = sorted(f"{s}->{sym_tickers[s]}" for s in found if s not in canonical)
        unresolved = sorted(set(symbols) - set(found))
        log.info("filings: %d/%d symbols resolved to a CIK%s%s", len(found), len(symbols),
                 f"; unresolved: {', '.join(unresolved)}" if unresolved else "",
                 f"; fetched under another symbol with the same ticker: {', '.join(shared)}" if shared else "")

        # 3 + 4. submissions -> new 8-K 2.02 / 6-K results filings -> Exhibit 99.1 text
        done = db.states_with_prefix(c, BACKFILL_STATE)
        backfill_since = date.today() - timedelta(days=BACKFILL_DAYS)
        to_backfill = sorted(s for s in resolved if s not in done)
        if to_backfill:
            log.info("filings: backfilling %d symbols since %s: %s", len(to_backfill), backfill_since,
                     ", ".join(to_backfill))
        candidates = new = failed = not_results = 0
        ev_candidates = ev_stored = ev_failed = 0
        ev_counts: Counter[str] = Counter()
        for i, (symbol, (cik, _name)) in enumerate(sorted(resolved.items()), 1):  # noqa: F841
            backfill = symbol not in done
            sym_since = backfill_since if backfill else since
            try:
                subs = edgar.json(SUBMISSIONS_URL.format(name=f"CIK{cik}.json"))
                _update_sic(c, symbol, subs.get("sic"), subs.get("sicDescription"))
                c.commit()
                filings = all_filings(edgar, subs, sym_since)
            except Exception as e:  # noqa: BLE001
                c.rollback()
                failed += 1
                log.warning("filings: %s submissions failed: %s", symbol, e)
                continue
            known = _known_accessions(c, symbol)
            todo = [f for f in earnings_8ks(filings, sym_since) + foreign_6ks(filings, sym_since)
                    if f["accessionNumber"] not in known]
            n_new = n_failed = n_skipped = 0
            for f in todo:
                acc = f["accessionNumber"]
                try:
                    folder = ARCHIVE_URL.format(cik=int(cik), acc=acc.replace("-", ""))
                    doc = pick_exhibit(edgar.text(folder), f.get("primaryDocument"))
                    if not doc:
                        raise ValueError("no exhibit or primary document in archive index")
                    exhibit_url = folder + doc
                    text = html_to_text(edgar.text(exhibit_url))
                    if f["form"] == FOREIGN_FORM and not is_results(text):
                        n_skipped += 1
                        continue
                    body, boilerplate = split_boilerplate(text, _name)
                    filed_at = _parse_ts(f.get("acceptanceDateTime")) or _parse_ts(f.get("filingDate"))
                    if not filed_at:
                        raise ValueError(f"no usable acceptanceDateTime/filingDate ({f.get('filingDate')!r})")
                    n_new += _insert_filing(c, {
                        "accession": acc, "cik": cik, "symbol": symbol, "form": f["form"], "items": f["item_list"],
                        "filed_at": filed_at, "period": _parse_date(f.get("reportDate")), "exhibit_url": exhibit_url,
                        "raw_text": text, "word_count": len(text.split()),
                        "body_text": body, "boilerplate_text": boilerplate})
                    c.commit()
                    stats["rows"] += 1
                    log.info("filings: %s %s %s filed %s -> %s (%d words)", symbol, f["form"], acc, f["filingDate"],
                             doc, len(text.split()))
                except Exception as e:  # noqa: BLE001
                    c.rollback()
                    n_failed += 1
                    log.warning("filings: %s %s failed: %s", symbol, acc, e)
            candidates += len(todo) - n_skipped
            new, failed, not_results = new + n_new, failed + n_failed, not_results + n_skipped
            n_cand, n_stored, n_ev_failed = _store_events(edgar, c, symbol, cik, filings, sym_since, ev_counts)
            ev_candidates, ev_stored, ev_failed = ev_candidates + n_cand, ev_stored + n_stored, ev_failed + n_ev_failed
            stats["rows"] += n_stored
            if backfill:
                db.set_state(c, BACKFILL_STATE + symbol, meta={
                    "since": sym_since.isoformat(), "filings_scanned": len(filings), "earnings_stored": n_new,
                    "earnings_failed": n_failed, "sixk_not_results": n_skipped, "events_stored": n_stored,
                    "events_failed": n_ev_failed})
                c.commit()
                log.info("filings: %s backfilled since %s: %d submissions scanned, %d earnings releases stored "
                         "(%d failed, %d 6-Ks not results), %d corporate events stored (%d failed)", symbol,
                         sym_since, len(filings), n_new, n_failed, n_skipped, n_stored, n_ev_failed)
            if i % 25 == 0 or i == len(resolved):
                log.info("filings: %d/%d symbols done; %d candidate filings, %d stored, %d failed; "
                         "%d corporate events stored, %d failed", i, len(resolved), candidates, new, failed,
                         ev_stored, ev_failed)
        log.info("filings: done (last %d days; %d symbols backfilled over %d days): %d symbols, %d resolved, "
                 "%d new 8-K 2.02 / 6-K results candidates, %d stored, %d failed, %d 6-Ks skipped as not results",
                 days, len(to_backfill), BACKFILL_DAYS, len(symbols), len(resolved), candidates, new, failed,
                 not_results)
        log.info("filings: corporate events: %d new candidates, %d stored, %d failed; by item: %s",
                 ev_candidates, ev_stored, ev_failed,
                 ", ".join(f"{item}={ev_counts.get(item, 0)}" for item in EVENT_ITEMS))
        uncovered = _uncovered(c, symbols)
        log.info("filings: coverage: %d tracked symbols with 0 filings%s", len(uncovered),
                 f": {', '.join(uncovered)}" if uncovered else "")
    db.refresh_word_counts(c)


def reset_backfill(c, symbol: str) -> None:
    """Delete the symbol's filings_backfill row so the next run looks back three years for it."""
    n = db.delete_state(c, BACKFILL_STATE + symbol)
    c.commit()
    log.info("filings: %s backfill marker %s", symbol, "cleared" if n else "was not set")


def reparse(c) -> None:
    """Fill body_text / boilerplate_text for every stored filing that lacks them, from raw_text."""
    with db.run_log(c, "filings_reparse") as stats:
        with c.cursor() as cur:
            cur.execute("""SELECT f.accession, f.raw_text, cm.name FROM filings f
                           LEFT JOIN company_map cm ON cm.symbol = f.symbol
                           WHERE f.body_text IS NULL ORDER BY f.filed_at DESC""")
            rows = cur.fetchall()
        log.info("filings: reparse %d filings without body_text", len(rows))
        split = 0
        for accession, raw, company in rows:
            body, boilerplate = split_boilerplate(raw or "", company)
            with c.cursor() as cur:
                cur.execute("UPDATE filings SET body_text = %s, boilerplate_text = %s WHERE accession = %s",
                            (body, boilerplate, accession))
            stats["rows"] += 1
            split += bool(boilerplate)
        c.commit()
        log.info("filings: reparse done: %d updated, %d with boilerplate split off", len(rows), split)
