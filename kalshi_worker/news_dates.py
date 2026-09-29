"""Exact publish times for kalshi.news_articles (`news-dates` job).

Serper's news results mostly carry relative dates ("3 weeks ago", "10 months ago") measured from the
fetch, which for a call months back pins an article down to about a month, and Google re-dates old
pages ("... for 2020-21" shown as "4 days ago"). So no article counts until this job has read its
publish time from the page itself:

1. GET the url (10s timeout, redirects followed, browser User-Agent), at most RPS requests a second
   overall and one a second per domain.
2. The first of these that parses to a timezone-aware time (a date alone = 23:59 New York):
   JSON-LD datePublished (any ld+json script, @graph included) -> 'json_ld',
   <meta property="article:published_time"> -> 'article_published_time',
   <meta name="pubdate|publishdate|date|DC.date.issued|parsely-pub-date|sailthru.date"> -> 'meta_name',
   <meta itemprop="datePublished"> -> 'itemprop',
   the first <time datetime> inside <article> -> 'article_time';
   none of them -> 'none'. dateModified / article:modified_time are never read.
3. Stored as exact_published_at, date_source, http_status, date_checked_at; then counted /
   reject_reason are recomputed: counted only when the exact time is before the cutoff (call_start
   - 2h), not before window_start (news_counts), not a re-dated stale page and not a recap title;
   otherwise 'no_exact_date' | 'after_cutoff_exact' | 'stale_redate' | 'before_window_exact' |
   'recap_title'. 'stale_redate': the exact time is more than 30 days before the earliest time
   Serper's own date allows ("4 days ago" -> fetched_at - 5 days).
4. When every article of a market and of its event's company baseline ('__company__' ||
   event_ticker) is checked, news_counts.n_word_articles / n_company_articles / n_rejected are
   recomputed from `counted` and dates_verified is set.

Rows a `news` re-fetch reset to 'unverified' after they were checked are re-classified from the
stored exact time without another request. Ends with the same leak self-check as `news`.
"""
from __future__ import annotations

import html
import json
import logging
import re
import time
from collections import Counter, deque
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import date, datetime, time as dtime, timedelta
from email.utils import parsedate_to_datetime
from html.parser import HTMLParser
from urllib.parse import urlsplit

import httpx

from . import db
from .news import COMPANY, CUTOFF_BEFORE, NY, RECAP_RE, RELATIVE_RE, WINDOW_DAYS, parse_date
from .transcripts import USER_AGENT

log = logging.getLogger(__name__)

RPS = 5
DOMAIN_GAP = 1.0                 # seconds between requests to one domain
TIMEOUT = 10.0
WORKERS = 12
MAX_BYTES = 3_000_000
STALE_DAYS = 30
META_NAMES = ("pubdate", "publishdate", "date", "dc.date.issued", "parsely-pub-date", "sailthru.date")
SOURCES = ("json_ld", "article_published_time", "meta_name", "itemprop", "article_time")
# the longest length of each unit: "N units ago" means less than N + 1 of them have passed
LONGEST = {"sec": timedelta(seconds=1), "second": timedelta(seconds=1), "min": timedelta(minutes=1),
           "minute": timedelta(minutes=1), "hr": timedelta(hours=1), "hour": timedelta(hours=1),
           "day": timedelta(days=1), "week": timedelta(weeks=1), "month": timedelta(days=31),
           "year": timedelta(days=366)}


# ------------------------------------------------------------------------------ html
class _Collector(HTMLParser):
    """Collects the raw candidate values of each source, in document order."""

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.found: dict[str, list[str]] = {s: [] for s in SOURCES}
        self.ld: list[str] = []
        self._in_ld = False
        self._article = 0

    def handle_starttag(self, tag, attrs):
        a = {k.lower(): (v or "") for k, v in attrs}
        if tag == "script" and a.get("type", "").strip().lower() == "application/ld+json":
            self._in_ld = True
            self.ld.append("")
        elif tag == "meta":
            content = a.get("content", "")
            if a.get("property", "").strip().lower() == "article:published_time":
                self.found["article_published_time"].append(content)
            if a.get("name", "").strip().lower() in META_NAMES:
                self.found["meta_name"].append(content)
            if a.get("itemprop", "").strip() == "datePublished":
                self.found["itemprop"].append(content)
        elif tag == "article":
            self._article += 1
        elif tag == "time" and self._article and a.get("datetime"):
            self.found["article_time"].append(a["datetime"])

    def handle_endtag(self, tag):
        if tag == "script":
            self._in_ld = False
        elif tag == "article" and self._article:
            self._article -= 1

    def handle_data(self, data):
        if self._in_ld:
            self.ld[-1] += data


def _ld_dates(text: str) -> list[str]:
    """datePublished values of one ld+json block: top-level objects, lists and @graph members."""
    try:
        doc = json.loads(text.strip(), strict=False)
    except ValueError:
        return []
    out: list[str] = []
    todo = deque([doc])
    while todo:
        x = todo.popleft()
        if isinstance(x, list):
            todo.extend(x)
        elif isinstance(x, dict):
            v = x.get("datePublished")
            if isinstance(v, str):
                out.append(v)
            elif isinstance(v, list):
                out.extend(i for i in v if isinstance(i, str))
            if isinstance(x.get("@graph"), list):
                todo.extend(x["@graph"])
    return out


def parse_time(raw: str | None) -> datetime | None:
    """A timezone-aware datetime from an HTML date value, or None. A date alone is 23:59 New York;
    a time without a zone doesn't count."""
    s = html.unescape(raw or "").strip()
    if not s:
        return None
    if m := re.fullmatch(r"(\d{4})-?(\d{2})-?(\d{2})", s):
        try:
            d = date(int(m[1]), int(m[2]), int(m[3]))
        except ValueError:
            return None
        return datetime.combine(d, dtime(23, 59), NY)
    for cand in (s, s.replace(" ", "T", 1), re.sub(r"\s+([+-]\d{2}:?\d{2}|Z)$", r"\1", s.replace(" ", "T", 1))):
        try:
            dt = datetime.fromisoformat(cand)
        except ValueError:
            continue
        return dt if dt.tzinfo else None
    try:
        dt = parsedate_to_datetime(s)
    except (TypeError, ValueError, IndexError):
        return None
    return dt if dt and dt.tzinfo else None


def extract(page: str) -> tuple[datetime | None, str]:
    """HTML -> (exact publish time, source name) using the first source that parses; (None, 'none')."""
    p = _Collector()
    try:
        p.feed(page)
        p.close()
    except Exception:  # noqa: BLE001  (malformed markup: use whatever was collected)
        pass
    p.found["json_ld"] = [v for block in p.ld for v in _ld_dates(block)]
    for source in SOURCES:
        for raw in p.found[source]:
            if (dt := parse_time(raw)) is not None:
                return dt, source
    return None, "none"


# ------------------------------------------------------------------------------ classify
def implied_earliest(date_raw: str | None, fetched_at: datetime) -> datetime | None:
    """The earliest publish time Serper's date string allows: 'N units ago' -> fetched_at - (N + 1)
    of the unit's longest length; an absolute date -> 00:00 New York that day."""
    s = " ".join((date_raw or "").split()).strip().rstrip(".")
    if not s:
        return None
    low = s.lower()
    if low in ("just now", "now"):
        return fetched_at - timedelta(hours=1)
    if low == "yesterday":
        return fetched_at - timedelta(days=2)
    if m := RELATIVE_RE.match(s):
        n = 1 if m[1].lower() in ("a", "an", "one") else int(m[1])
        return fetched_at - (n + 1) * LONGEST[m[2].lower()]
    latest = parse_date(s, fetched_at)      # an absolute date: 23:59 New York that day
    return datetime.combine(latest.astimezone(NY).date(), dtime(0, 0), NY) if latest else None


def classify(exact: datetime | None, cutoff: datetime, window_start: date, title: str | None,
             date_raw: str | None, fetched_at: datetime) -> tuple[bool, str | None]:
    """-> (counted, reject_reason) for a checked article."""
    if exact is None:
        return False, "no_exact_date"
    if exact >= cutoff:
        return False, "after_cutoff_exact"
    earliest = implied_earliest(date_raw, fetched_at)
    if earliest is not None and exact < earliest - timedelta(days=STALE_DAYS):
        return False, "stale_redate"
    if exact < datetime.combine(window_start, dtime(0, 0), NY):
        return False, "before_window_exact"
    if RECAP_RE.search(title or ""):
        return False, "recap_title"
    assert exact < cutoff
    return True, None


# ------------------------------------------------------------------------------ http
def domain(url: str) -> str:
    host = (urlsplit(url).hostname or "").lower()
    return host[4:] if host.startswith("www.") else host


def fetch_page(http: httpx.Client, url: str) -> tuple[int | None, str | None]:
    """GET one article -> (status, html or None). Transport errors -> (None, None)."""
    try:
        with http.stream("GET", url) as r:
            ctype = r.headers.get("content-type", "").lower()
            if r.status_code != 200 or ("html" not in ctype and "xml" not in ctype and ctype):
                return r.status_code, None
            body = bytearray()
            for chunk in r.iter_bytes():
                body += chunk
                if len(body) >= MAX_BYTES:
                    break
            return r.status_code, body.decode(r.encoding or "utf-8", errors="replace")
    except (httpx.HTTPError, UnicodeError, LookupError) as e:
        log.debug("news-dates: %s: %s", url, e)
        return None, None


def check(http: httpx.Client, url: str) -> tuple[int | None, datetime | None, str]:
    status, page = fetch_page(http, url)
    if page is None:
        return status, None, "none"
    exact, source = extract(page)
    return status, exact, source


class Scheduler:
    """Hands out rows so that requests start at most `rps` a second overall and one per
    `domain_gap` seconds per domain, newest call first among the domains that are ready."""

    def __init__(self, rows: list[dict], rps: float = RPS, domain_gap: float = DOMAIN_GAP,
                 clock=time.monotonic, sleep=time.sleep):
        self.queues: dict[str, deque] = {}
        for i, r in enumerate(rows):
            self.queues.setdefault(domain(r["url"]), deque()).append((i, r))
        self.ready: dict[str, float] = {d: 0.0 for d in self.queues}
        self.gap, self.domain_gap, self.clock, self.sleep = 1.0 / rps, domain_gap, clock, sleep
        self.last = None

    def __bool__(self) -> bool:
        return bool(self.queues)

    def next(self) -> dict:
        """The next row, after sleeping until both limits allow its request."""
        now = self.clock()
        start = max(now, min(self.ready[d] for d in self.queues),
                    self.last + self.gap if self.last is not None else now)
        if start > now:
            self.sleep(start - now)
        _, d = min((q[0][0], d) for d, q in self.queues.items() if self.ready[d] <= start)
        _, row = self.queues[d].popleft()
        if not self.queues[d]:
            del self.queues[d]
        self.ready[d], self.last = start + self.domain_gap, start
        return row


# ------------------------------------------------------------------------------ db
TODO_SQL = """
SELECT a.ticker, a.event_ticker, a.url, a.title, a.date_raw, a.fetched_at, a.cutoff, a.call_start,
       a.exact_published_at, a.date_checked_at,
       COALESCE(nc.window_start, ev.window_start) AS window_start
FROM kalshi.news_articles a
LEFT JOIN kalshi.news_counts nc ON nc.ticker = a.ticker
LEFT JOIN LATERAL (SELECT min(n.window_start) AS window_start FROM kalshi.news_counts n
                   WHERE n.event_ticker = a.event_ticker AND n.call_start = a.call_start) ev ON true
WHERE ((a.exact_published_at IS NULL AND a.date_checked_at IS NULL)
       OR (a.date_checked_at IS NOT NULL AND a.reject_reason = 'unverified'))
  {extra}
ORDER BY a.call_start DESC, a.ticker, a.url
{limit}
"""
COLS = ("ticker", "event_ticker", "url", "title", "date_raw", "fetched_at", "cutoff", "call_start",
        "exact_published_at", "date_checked_at", "window_start")

RECOUNT_SQL = """
WITH a AS (
    SELECT ticker,
           count(*) FILTER (WHERE date_checked_at IS NULL OR reject_reason = 'unverified') AS unchecked,
           count(*) FILTER (WHERE counted) AS counted,
           count(*) FILTER (WHERE NOT counted) AS rejected
    FROM kalshi.news_articles WHERE event_ticker = ANY(%(events)s) GROUP BY ticker)
UPDATE kalshi.news_counts nc
SET n_word_articles = COALESCE(w.counted, 0), n_rejected = COALESCE(w.rejected, 0),
    n_company_articles = COALESCE(b.counted, 0), dates_verified = true
FROM kalshi.news_counts x
LEFT JOIN a w ON w.ticker = x.ticker
LEFT JOIN a b ON b.ticker = %(company)s || x.event_ticker
WHERE nc.ticker = x.ticker AND x.event_ticker = ANY(%(events)s) AND x.error IS NULL
  AND COALESCE(w.unchecked, 0) = 0 AND COALESCE(b.unchecked, 0) = 0
"""


def todo(c, limit: int | None = None, keys: list[tuple[str, str]] | None = None) -> list[dict]:
    """Pending rows, newest call first; `keys` narrows them to these (ticker, url) pairs."""
    extra, params = "", {"limit": limit}
    if keys is not None:
        extra = "AND (a.ticker, a.url) IN (SELECT * FROM unnest(%(tickers)s::text[], %(urls)s::text[]))"
        params.update(tickers=[k[0] for k in keys], urls=[k[1] for k in keys])
    sql = TODO_SQL.format(extra=extra, limit="LIMIT %(limit)s" if limit else "")
    with c.cursor() as cur:
        cur.execute(sql, params)
        return [dict(zip(COLS, r)) for r in cur.fetchall()]


def _window_start(row: dict) -> date:
    """news_counts.window_start; the same value computed from call_start when no row has it."""
    return row["window_start"] or (row["call_start"].astimezone(NY).date() - timedelta(days=WINDOW_DAYS))


def save(c, row: dict, status: int | None, exact: datetime | None, source: str | None,
         checked: bool) -> tuple[bool, str | None]:
    """Store one row's check (or only its reclassification when `checked` is False) and its new
    counted / reject_reason."""
    cutoff = row["cutoff"] or row["call_start"] - CUTOFF_BEFORE
    counted, reason = classify(exact, cutoff, _window_start(row), row["title"], row["date_raw"],
                               row["fetched_at"])
    with c.cursor() as cur:
        if checked:
            cur.execute("""UPDATE kalshi.news_articles SET exact_published_at = %s, date_source = %s,
                             http_status = %s, date_checked_at = now(), counted = %s, reject_reason = %s
                           WHERE ticker = %s AND url = %s""",
                        (exact, source, status, counted, reason, row["ticker"], row["url"]))
        else:
            cur.execute("UPDATE kalshi.news_articles SET counted = %s, reject_reason = %s "
                        "WHERE ticker = %s AND url = %s", (counted, reason, row["ticker"], row["url"]))
    return counted, reason


def recount(c, events: set[str]) -> int:
    """Recompute news_counts for these events' fully checked markets -> rows updated."""
    if not events:
        return 0
    with c.cursor() as cur:
        cur.execute(RECOUNT_SQL, {"events": sorted(events), "company": COMPANY})
        n = cur.rowcount
    c.commit()
    return n


def self_check(c) -> int:
    with c.cursor() as cur:
        cur.execute("select count(*) from kalshi.news_articles "
                    "where counted and (exact_published_at is null or exact_published_at >= cutoff)")
        n = cur.fetchone()[0]
    c.commit()
    (log.error if n else log.info)("news-dates: self-check: %d counted articles without an exact time "
                                   "before the cutoff", n)
    return n


# ------------------------------------------------------------------------------ run
def verify(c, limit: int | None = None, keys: list[tuple[str, str]] | None = None,
           http: httpx.Client | None = None, workers: int = WORKERS, scheduler=Scheduler) -> dict:
    """Check the pending rows (all of them, the first `limit`, or only `keys`) -> summary counters."""
    rows = todo(c, limit, keys)
    sources, reasons = Counter(), Counter()
    stats = {"checked": 0, "reclassified": 0, "counted": 0, "recounted": 0}
    touched: set[str] = set()
    log.info("news-dates: %d articles to check", len(rows))

    def done(row, status, exact, source, checked):
        counted, reason = save(c, row, status, exact, source, checked)
        c.commit()
        touched.add(row["event_ticker"])
        stats["checked" if checked else "reclassified"] += 1
        if checked:
            sources[source] += 1
        stats["counted"] += counted
        if not counted:
            reasons[reason] += 1
        if len(touched) >= 50:
            stats["recounted"] += recount(c, touched)
            touched.clear()

    fetch_rows = []
    for r in rows:                      # already checked: reclassify from the stored time
        if r["date_checked_at"] is not None:
            done(r, None, r["exact_published_at"], None, False)
        else:
            fetch_rows.append(r)

    own = http is None
    http = http or httpx.Client(timeout=TIMEOUT, follow_redirects=True, headers={
        "User-Agent": USER_AGENT, "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "en-US,en;q=0.9"})
    sched = scheduler(fetch_rows)
    try:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            running = {}
            while sched or running:
                while sched and len(running) < workers:
                    row = sched.next()
                    running[pool.submit(check, http, row["url"])] = row
                finished, _ = wait(running, timeout=1.0, return_when=FIRST_COMPLETED)
                for f in finished:
                    row = running.pop(f)
                    status, exact, source = f.result()
                    done(row, status, exact, source, True)
                    if stats["checked"] % 500 == 0:
                        log.info("news-dates: %d / %d checked", stats["checked"], len(fetch_rows))
    finally:
        if own:
            http.close()
        stats["recounted"] += recount(c, touched)
    stats["sources"], stats["reasons"] = sources, reasons
    return stats


def summary(stats: dict) -> str:
    found = ", ".join(f"{s} {stats['sources'][s]}" for s in (*SOURCES, "none") if stats["sources"][s]) or "none"
    rejected = ", ".join(f"{k} {v}" for k, v in sorted(stats["reasons"].items())) or "none"
    return (f"news-dates: {stats['checked']} articles checked ({stats['reclassified']} more reclassified "
            f"from stored times), exact dates by source: {found}; {stats['counted']} counted, rejected: "
            f"{rejected}; {stats['recounted']} news_counts rows recomputed")


def run(c, limit: int | None = None, http: httpx.Client | None = None) -> None:
    with db.run_log(c, "news_dates") as st:
        failure = None
        stats = None
        try:
            stats = verify(c, limit=limit, http=http)
            st["rows"] = stats["checked"] + stats["reclassified"]
        except Exception as e:  # noqa: BLE001  (the self-check still runs)
            c.rollback()
            failure = e
        leaked = self_check(c)
        if stats:
            log.info(summary(stats))
        if leaked:
            raise RuntimeError(f"news-dates self-check failed: {leaked} counted articles without an exact "
                               "time before the cutoff")
        if failure:
            raise failure
