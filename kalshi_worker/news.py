"""Pre-call news volume from the Serper Google News API (`news` job).

For each earnings-mention market: how many Google News articles mentioned the company and the
market's word in the two weeks before the call (n_word_articles), next to how many mentioned the
company at all (n_company_articles, one query per event). Nothing published at or after the call
start may be counted:

1. Query window: Serper's tbs=cdr:1,cd_min,cd_max (day-granular) over [call day - 14, call day - 1]
   in New York, the call day never included. The anchor is kalshi.mv_call_times.call_start_est;
   events without one are skipped (no close_time fallback).
2. Every returned article is stored in kalshi.news_articles (the company baseline under ticker
   '__company__' || event_ticker) with counted = false, reject_reason = 'unverified'. Serper's own
   `date` is mostly relative ("10 months ago") and sometimes a re-dated stale page, so it is kept
   only for reference (date_raw, published_at: "Mar 5, 2025" = 23:59 New York that day, "3 days
   ago" = fetched_at minus the offset).
3. Before the run ends, news_dates.verify reads each new article's publish time from the page
   itself and decides counted (exact time before call_start - 2h, inside the window, not a stale
   re-date, not a recap title) and recomputes kalshi.news_counts once a market is fully checked
   (dates_verified). Default run:
markets whose call starts between now + 3h and now + 3 days. --backfill: settled markets with no
news_counts row (or an errored one), newest calls first. A Serper error is stored in
news_counts.error and retried next run; 401/403/429 or an out-of-credits answer stops the run.
5 Serper requests/s, at most MAX_REQUESTS per run. Every run ends with the leak self-check (counted
without an exact time before the cutoff must be 0; the run fails otherwise). Needs SERPER_API_KEY.
"""
from __future__ import annotations

import logging
import os
import re
import time
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import httpx

from . import db

log = logging.getLogger(__name__)

URL = "https://google.serper.dev/news"
NUM = 100
MAX_REQUESTS = 2000
RPS = 5
WINDOW_DAYS = 14
CUTOFF_BEFORE = timedelta(hours=2)
UPCOMING_FROM, UPCOMING_TO = timedelta(hours=3), timedelta(days=3)
NY = ZoneInfo("America/New_York")
COMPANY = "__company__"

RECAP_RE = re.compile(r"earnings call|conference call|transcript|reported|reports|beats|misses|results|"
                      r"q[1-4] (?:earnings|results)|said on|on the call|guidance|"
                      r"shares (?:jump|fall|rise|drop|slide|surge)", re.I)
TIMES_RE = re.compile(r"\s*\(\s*\d+\s*\+?\s*times?\s*\)\s*$", re.I)
RELATIVE_RE = re.compile(r"^(\d+|an?|one)\s+(sec|second|min|minute|hr|hour|day|week|month|year)s?\s+ago$", re.I)
# the shortest length of each unit, so "N units ago" never lands earlier than the real time
UNITS = {"sec": timedelta(seconds=1), "second": timedelta(seconds=1), "min": timedelta(minutes=1),
         "minute": timedelta(minutes=1), "hr": timedelta(hours=1), "hour": timedelta(hours=1),
         "day": timedelta(days=1), "week": timedelta(weeks=1), "month": timedelta(days=28),
         "year": timedelta(days=365)}
DATE_FORMATS = ("%b %d, %Y", "%B %d, %Y", "%d %b %Y", "%d %B %Y", "%Y-%m-%d", "%m/%d/%Y")
DATE_NO_YEAR = ("%b %d", "%B %d")

SUFFIXES = {"INC", "INCORPORATED", "CORP", "CORPORATION", "CO", "COMPANY", "HOLDINGS", "HOLDING", "LTD",
            "LIMITED", "PLC", "GROUP", "NV", "SA"}
# company_map.name (or its stripped form, or the Kalshi symbol when the name is null), normalized
# to upper case without punctuation -> the name the press uses
NAME_OVERRIDES = {
    "ALPHABET INC": "Google", "ALPHABET": "Google", "META PLATFORMS": "Meta", "AMAZON COM": "Amazon",
    "ADVANCED MICRO DEVICES": "AMD", "AMC ENTERTAINMENT": "AMC", "ALBERTSONS COMPANIES": "Albertsons",
    "CEREBRAS SYSTEMS": "Cerebras", "CHIPOTLE MEXICAN GRILL": "Chipotle", "CIRCLE INTERNET": "Circle",
    "COINBASE GLOBAL": "Coinbase", "COSTCO WHOLESALE": "Costco", "CRACKER BARREL OLD COUNTRY STORE": "Cracker Barrel",
    "DELL TECHNOLOGIES": "Dell", "DOMINOS PIZZA": "Domino's", "FORD MOTOR": "Ford", "HILTON WORLDWIDE": "Hilton",
    "HIMS HERS HEALTH": "Hims & Hers", "JPMORGAN CHASE": "JPMorgan", "KKR": "KKR", "ROBINHOOD MARKETS": "Robinhood", "KRATOS DEFENSE SECURITY SOLUTIONS": "Kratos", "LOWES COMPANIES": "Lowe's",
    "LULULEMON ATHLETICA": "Lululemon", "MARVELL TECHNOLOGY": "Marvell", "MCDONALDS": "McDonald's",
    "MCCORMICK": "McCormick", "MICRON TECHNOLOGY": "Micron", "MOODYS": "Moody's", "PALANTIR TECHNOLOGIES": "Palantir",
    "RIVIAN AUTOMOTIVE": "Rivian", "SPACE EXPLORATION TECHNOLOGIES": "SpaceX", "SPOTIFY TECHNOLOGY": "Spotify",
    "STRATEGY": "MicroStrategy", "TAIWAN SEMICONDUCTOR MANUFACTURING": "TSMC",
    "TAKE TWO INTERACTIVE SOFTWARE": "Take-Two", "TRUIST FINANCIAL": "Truist", "UBER TECHNOLOGIES": "Uber",
    "VERIZON COMMUNICATIONS": "Verizon", "WALT DISNEY": "Disney", "ZOOM COMMUNICATIONS": "Zoom",
    "EA": "Electronic Arts", "VSCO": "Victoria's Secret",
}


class Fatal(Exception):
    """Serper refused the key or is out of credits / rate-limited: stop the run."""


class BudgetSpent(Exception):
    """MAX_REQUESTS used this run."""


# ------------------------------------------------------------------------------ query
def _norm(s: str) -> str:
    return " ".join(re.sub(r"[^\w ]+", " ", s.upper()).split())


def short_name(name: str | None, symbol: str = "") -> str | None:
    """company_map.name with Inc / Corp / Co / Holdings / Ltd / plc / Group (and SEC state tags like
    /DE/) and punctuation stripped, or its NAME_OVERRIDES entry; None when there is no name."""
    if not name:
        return NAME_OVERRIDES.get(_norm(symbol))
    s = re.sub(r"\s*/\s*[A-Z]{2,3}/?\s*$", "", name.strip())      # /DE/, /NEW, / DE
    s = re.sub(r"\b([A-Z])\.([A-Z])\.", r"\1\2", s)                 # N.V. -> NV, S.A. -> SA
    words = re.sub(r"[^\w&'\- ]+", " ", s).split()
    while words and (words[-1].upper() in SUFFIXES or words[-1] in {"&", "-"}):
        words.pop()
    short = " ".join(words)
    for key in (_norm(name), _norm(short)):
        if key in NAME_OVERRIDES:
            return NAME_OVERRIDES[key]
    if short.isupper():
        short = " ".join(w.capitalize() for w in short.split())
    return short or None


def word_terms(yes_sub_title: str) -> list[str]:
    """'AI / Artificial Intelligence (3+ times)' -> ['AI', 'Artificial Intelligence']."""
    parts = [p.replace('"', "").strip() for p in TIMES_RE.sub("", yes_sub_title or "").split("/")]
    out: list[str] = []
    for p in parts:
        if p and p.lower() not in {o.lower() for o in out}:
            out.append(p)
    return out


def build_query(company: str, terms: list[str] | None = None) -> str:
    q = f'"{company}"'
    if terms:
        q += " " + (f'"{terms[0]}"' if len(terms) == 1 else "(" + " OR ".join(f'"{t}"' for t in terms) + ")")
    return q


def window(call_start: datetime) -> tuple[date, date, datetime]:
    """-> (window_start, window_end, cutoff): call day (New York) - 14 and - 1, call_start - 2h."""
    day = call_start.astimezone(NY).date()
    return day - timedelta(days=WINDOW_DAYS), day - timedelta(days=1), call_start - CUTOFF_BEFORE


# ------------------------------------------------------------------------------ dates
def parse_date(raw: str | None, fetched_at: datetime) -> datetime | None:
    """Serper's `date` -> an absolute time; None when unparseable. Date-only values are 23:59 New
    York that day; relative ones are fetched_at minus the offset."""
    s = " ".join((raw or "").split()).strip().rstrip(".")
    if not s:
        return None
    low = s.lower()
    if low in ("just now", "now"):
        return fetched_at
    if low == "yesterday":
        return fetched_at - timedelta(days=1)
    if m := RELATIVE_RE.match(s):
        n = 1 if m[1].lower() in ("a", "an", "one") else int(m[1])
        return fetched_at - n * UNITS[m[2].lower()]
    for fmt in DATE_FORMATS:
        try:
            d = datetime.strptime(s, fmt).date()
            break
        except ValueError:
            continue
    else:
        today = fetched_at.astimezone(NY).date()
        for fmt in DATE_NO_YEAR:
            try:
                d = datetime.strptime(f"{s} {today.year}", f"{fmt} %Y").date()
            except ValueError:
                continue
            if d > today:
                d = d.replace(year=d.year - 1)
            break
        else:
            return None
    return datetime(d.year, d.month, d.day, 23, 59, tzinfo=NY)


# ------------------------------------------------------------------------------ http
class Serper:
    def __init__(self, api_key: str, http: httpx.Client | None = None, budget: int = MAX_REQUESTS,
                 rps: float = RPS, sleep=time.sleep, clock=time.monotonic):
        self.http = http or httpx.Client(timeout=30.0, headers={"X-API-KEY": api_key,
                                                                "Content-Type": "application/json"})
        self.budget, self.used = budget, 0
        self.gap, self.sleep, self.clock, self.last = 1.0 / rps, sleep, clock, None

    def news(self, query: str, start: date, end: date) -> tuple[list[dict], datetime]:
        """One /news search over [start, end] -> (articles, fetched_at). Raises BudgetSpent before
        the request when the budget is used, Fatal on 401/403/429 or no credits, RuntimeError on
        any other failure."""
        if self.used >= self.budget:
            raise BudgetSpent(f"request cap of {self.budget} reached")
        if self.last is not None and (wait := self.gap - (self.clock() - self.last)) > 0:
            self.sleep(wait)
        self.last = self.clock()
        self.used += 1
        payload = {"q": query, "num": NUM,
                   "tbs": f"cdr:1,cd_min:{start.month}/{start.day}/{start.year},cd_max:{end.month}/{end.day}/{end.year}"}
        try:
            r = self.http.post(URL, json=payload)
        except httpx.HTTPError as e:
            raise RuntimeError(f"transport error: {e}") from e
        fetched_at = datetime.now(timezone.utc)
        body = r.text[:300]
        if r.status_code in (401, 403, 429) or (r.status_code >= 400 and "credit" in body.lower()):
            raise Fatal(f"HTTP {r.status_code}: {body}")
        if r.status_code != 200:
            raise RuntimeError(f"HTTP {r.status_code}: {body}")
        try:
            items = r.json().get("news")
        except ValueError as e:
            raise RuntimeError(f"bad JSON: {body}") from e
        if not isinstance(items, list):
            raise RuntimeError(f"no 'news' list: {body}")
        return [i for i in items if isinstance(i, dict)], fetched_at

    def close(self) -> None:
        self.http.close()


# ------------------------------------------------------------------------------ db
TARGETS_SQL = """
SELECT m.ticker, m.event_ticker, t.symbol, m.yes_sub_title, t.call_start_est, cm.name
FROM kalshi.markets m
JOIN kalshi.mv_call_times t ON t.event_ticker = m.event_ticker
LEFT JOIN kalshi.company_map cm ON cm.symbol = t.symbol
WHERE m.series_ticker LIKE 'KXEARNINGSMENTION%%' AND t.call_start_est IS NOT NULL AND {where}
ORDER BY t.call_start_est {order}, m.event_ticker, m.ticker
"""
UPCOMING = "t.call_start_est BETWEEN now() + %s AND now() + %s"
BACKFILL = ("m.result IN ('yes', 'no') AND NOT EXISTS (SELECT 1 FROM kalshi.news_counts n "
            "WHERE n.ticker = m.ticker AND n.error IS NULL)")


def targets(c, backfill: bool) -> list[dict]:
    with c.cursor() as cur:
        if backfill:
            cur.execute(TARGETS_SQL.format(where=BACKFILL, order="DESC"))
        else:
            cur.execute(TARGETS_SQL.format(where=UPCOMING, order="ASC"), (UPCOMING_FROM, UPCOMING_TO))
        cols = ("ticker", "event_ticker", "symbol", "yes_sub_title", "call_start", "name")
        return [dict(zip(cols, r)) for r in cur.fetchall()]


def store_articles(c, ticker: str, event_ticker: str, items: list[dict], fetched_at: datetime,
                   call_start: datetime, win: tuple[date, date, datetime]) -> list[tuple[str, str]]:
    """Replace `ticker`'s articles with this response's (deduplicated by URL), all unverified ->
    the (ticker, url) keys stored. A URL already checked keeps its exact time and is reclassified by
    news_dates without another request."""
    cutoff = win[2]
    rows, seen = [], set()
    for it in items:
        url = it.get("link") or it.get("url")
        if not url or url in seen:
            continue
        seen.add(url)
        rows.append((ticker, event_ticker, url, it.get("title"), it.get("source"), it.get("date"),
                     parse_date(it.get("date"), fetched_at), call_start, cutoff, False, "unverified", fetched_at))
    with c.cursor() as cur:
        cur.execute("DELETE FROM kalshi.news_articles WHERE ticker = %s AND NOT (url = ANY(%s))", (ticker, list(seen)))
        if rows:
            cur.executemany(
                """INSERT INTO kalshi.news_articles (ticker, event_ticker, url, title, source, date_raw, published_at,
                                                     call_start, cutoff, counted, reject_reason, fetched_at)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                   ON CONFLICT (ticker, url) DO UPDATE SET
                     event_ticker = EXCLUDED.event_ticker, title = EXCLUDED.title, source = EXCLUDED.source,
                     date_raw = EXCLUDED.date_raw, published_at = EXCLUDED.published_at,
                     call_start = EXCLUDED.call_start, cutoff = EXCLUDED.cutoff, counted = EXCLUDED.counted,
                     reject_reason = EXCLUDED.reject_reason, fetched_at = EXCLUDED.fetched_at""", rows)
    return [(ticker, url) for url in seen]


def stored_baseline(c, event_ticker: str, call_start: datetime) -> int | None:
    """Counted company-baseline articles already stored for this event and call start, or None."""
    with c.cursor() as cur:
        cur.execute("SELECT count(*), count(*) FILTER (WHERE counted) FROM kalshi.news_articles "
                    "WHERE ticker = %s AND call_start = %s", (COMPANY + event_ticker, call_start))
        n, counted = cur.fetchone()
    return counted if n else None


def write_count(c, m: dict, query: str, win: tuple[date, date, datetime], n_word: int | None,
                n_company: int | None, n_rejected: int | None, error: str | None) -> None:
    with c.cursor() as cur:
        cur.execute(
            """INSERT INTO kalshi.news_counts (ticker, event_ticker, symbol, query, call_start, window_start,
                                               window_end, n_word_articles, n_company_articles, n_rejected,
                                               fetched_at, error, dates_verified)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), %s, false)
               ON CONFLICT (ticker) DO UPDATE SET
                 event_ticker = EXCLUDED.event_ticker, symbol = EXCLUDED.symbol, query = EXCLUDED.query,
                 call_start = EXCLUDED.call_start, window_start = EXCLUDED.window_start,
                 window_end = EXCLUDED.window_end, n_word_articles = EXCLUDED.n_word_articles,
                 n_company_articles = EXCLUDED.n_company_articles, n_rejected = EXCLUDED.n_rejected,
                 fetched_at = EXCLUDED.fetched_at, error = EXCLUDED.error, dates_verified = false""",
            (m["ticker"], m["event_ticker"], m["symbol"], query, m["call_start"], win[0], win[1],
             n_word, n_company, n_rejected, error))


# ------------------------------------------------------------------------------ run
def _events(rows: list[dict]) -> list[list[dict]]:
    out: list[list[dict]] = []
    for r in rows:
        if out and out[-1][0]["event_ticker"] == r["event_ticker"]:
            out[-1].append(r)
        else:
            out.append([r])
    return out


def fetch(c, api: Serper, backfill: bool, stored: list[tuple[str, str]], stats: dict) -> None:
    """Query Serper for the run's markets; the (ticker, url) keys stored are appended to `stored`."""
    rows = targets(c, backfill)
    events = _events(rows)
    written: set[str] = set()                 # markets whose news_counts row this run wrote
    log.info("news: %d markets in %d events to do (%s)", len(rows), len(events),
             "backfill: settled, no news_counts row yet or an errored one" if backfill else "calls starting in 3h to 3 days")
    for i, markets in enumerate(events):
        first = markets[0]
        ev, call_start = first["event_ticker"], first["call_start"]
        win = window(call_start)
        company = short_name(first["name"], first["symbol"])
        try:
            if not company:
                for m in markets:
                    write_count(c, m, "", win, None, None, None, "no company name in company_map")
                c.commit()
                stats["errors"] += len(markets)
                written.update(m["ticker"] for m in markets)
                continue
            # the company-only baseline, once per event; a backfill reuses one stored by an earlier run
            n_company = stored_baseline(c, ev, call_start) if backfill else None
            company_error = None
            if n_company is None:
                try:
                    items, fetched_at = api.news(build_query(company), win[0], win[1])
                    keys = store_articles(c, COMPANY + ev, ev, items, fetched_at, call_start, win)
                    c.commit()
                    stored.extend(keys)
                    n_company = 0                       # nothing counts until news_dates verifies it
                except (RuntimeError, Fatal) as e:
                    c.rollback()
                    company_error = f"company baseline: {e}"[:1000]
                    if isinstance(e, Fatal):
                        for m in markets:
                            write_count(c, m, build_query(company, word_terms(m["yes_sub_title"])), win,
                                        None, None, None, company_error)
                        c.commit()
                        written.update(m["ticker"] for m in markets)
                        raise
            for m in markets:
                query = build_query(company, word_terms(m["yes_sub_title"]))
                try:
                    items, fetched_at = api.news(query, win[0], win[1])
                except (RuntimeError, Fatal) as e:
                    write_count(c, m, query, win, None, n_company, None, str(e)[:1000])
                    c.commit()
                    written.add(m["ticker"])
                    stats["errors"] += 1
                    if isinstance(e, Fatal):
                        raise
                    continue
                keys = store_articles(c, m["ticker"], ev, items, fetched_at, call_start, win)
                write_count(c, m, query, win, 0, n_company, len(keys), company_error)
                c.commit()
                written.add(m["ticker"])
                stored.extend(keys)
                stats["rows"] += 1
                if company_error:
                    stats["errors"] += 1
                else:
                    stats["done"] += 1
        except BudgetSpent:
            c.rollback()
            left = [m for m in markets if m["ticker"] not in written] + [m for e in events[i + 1:] for m in e]
            log.warning("news: request cap of %d reached; %d markets in %d events left for the next run",
                        api.budget, len(left), len({m["event_ticker"] for m in left}))
            return


def run(c, backfill: bool = False, api: Serper | None = None, http: httpx.Client | None = None) -> None:
    """`http` is the page client handed to news_dates (tests)."""
    from . import news_dates           # imports this module

    key = os.getenv("SERPER_API_KEY")
    if api is None and not key:
        raise SystemExit("news: SERPER_API_KEY is not set")
    api = api or Serper(key)
    stored: list[tuple[str, str]] = []
    try:
        with db.run_log(c, "news_backfill" if backfill else "news") as stats:
            stats.update(done=0, errors=0)
            failure, dates = None, None
            try:
                fetch(c, api, backfill, stored, stats)
            except Exception as e:  # noqa: BLE001  (verification, the self-check and summary still run)
                c.rollback()
                failure = e
            log.info("news: %d markets done, %d with errors, %d requests used, %d articles stored; "
                     "verifying their dates", stats["done"], stats["errors"], api.used, len(stored))
            try:
                if stored:
                    dates = news_dates.verify(c, keys=stored, http=http)
            except Exception as e:  # noqa: BLE001
                c.rollback()
                failure = failure or e
            leaked = news_dates.self_check(c)
            if dates:
                log.info(news_dates.summary(dates))
            if leaked:
                raise RuntimeError(f"news self-check failed: {leaked} counted articles without an exact time "
                                   "before the cutoff")
            if failure:
                raise failure
    finally:
        api.close()
