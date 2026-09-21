"""Motley Fool earnings-call transcript ingestion.

Works the kalshi.transcript_sources queue (status='pending', newest call first): fetch the
page with a browser User-Agent, keep the HTML, split it into speaking turns, tag each turn
with a section (prepared / qa) and a speaker role (operator / exec / analyst / unknown), and
write one transcripts row plus its transcript_segments. One second between requests.
"""
from __future__ import annotations

import logging
import re
import time

import httpx
from bs4 import BeautifulSoup, Tag

from . import db

log = logging.getLogger(__name__)

USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36")
REQUEST_DELAY_S = 1.0

TITLE_RE = re.compile(r"\bQ([1-4])\s+(?:FY\s*)?(\d{4})\b", re.I)
SEPARATOR_RE = re.compile(r"\s+(?:--|—|–)\s+")
NOT_SPEAKERS = {"duration", "image source", "contents"}
BOILERPLATE = (
    "this article is a transcript",
    "the motley fool has a disclosure policy",
    "the motley fool has positions in",
    "the motley fool recommends",
    "the motley fool has no position",
    "image source: the motley fool",
)
# Words that mark a participant title as sell-side: "Analyst" itself or a bank/firm name.
ANALYST_WORDS = (
    "analyst", "securities", "capital", "bank", "partners", "research", "markets", "investment",
    "asset management", "advisors", "wealth", "llc", "& co", "morgan", "goldman", "jpmorgan",
    "barclays", "citi", "ubs", "jefferies", "wells fargo", "bernstein", "evercore", "piper",
    "raymond james", "stifel", "baird", "wolfe", "cowen", "bmo", "rbc", "hsbc", "deutsche",
    "mizuho", "oppenheimer", "needham", "truist", "kbw", "macquarie", "guggenheim", "bofa",
    "credit suisse", "nomura", "scotiabank", "canaccord", "william blair", "rosenblatt",
    "susquehanna", "cantor", "btig", "leerink", "melius", "redburn", "daiwa", "exane", "argus",
)


# ------------------------------------------------------------------------------ fetch
def fetch(http: httpx.Client, url: str, retries: int = 3) -> str:
    for attempt in range(retries):
        try:
            r = http.get(url)
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
        return r.text
    raise RuntimeError(f"gave up on {url}")


# ------------------------------------------------------------------------------ parse
def _text(el: Tag | str) -> str:
    s = el if isinstance(el, str) else el.get_text(" ", strip=True)
    return re.sub(r"\s+", " ", s).strip()


def parse_title(title: str | None) -> tuple[int, int] | None:
    """'Adobe (ADBE) Q3 2026 Earnings Call Transcript' -> (2026, 3)."""
    m = TITLE_RE.search(title or "")
    return (int(m.group(2)), int(m.group(1))) if m else None


def _article_body(soup: BeautifulSoup) -> Tag:
    for sel in ("div.article-body", "div.tailwind-article-body", "div[class*=article-body]", "article", "body"):
        el = soup.select_one(sel)
        if el is not None:
            return el
    return soup


def _split_name_title(p: Tag) -> tuple[str, str | None] | None:
    """A speaker header or participant line: '<strong>Name</strong> -- <em>Title</em>' or plain
    'Name -- Title'. Returns (name, title) or None when the paragraph doesn't look like one."""
    full = _text(p)
    if not full or len(full) > 160:
        return None
    parts = SEPARATOR_RE.split(full, 1)
    name = parts[0].strip().rstrip(":")
    title = parts[1].strip() or None if len(parts) > 1 else None
    strong = p.find("strong")
    if strong is not None:
        return (name, title) if _text(strong).rstrip(":") == name else None
    return (name, title) if title and len(name.split()) <= 5 else None


def _inline_turn(p: Tag) -> tuple[str, str] | None:
    """'<p><strong>Name:</strong> text</p>' -> (name, text)."""
    first = next((ch for ch in p.children if not (isinstance(ch, str) and not ch.strip())), None)
    if not isinstance(first, Tag) or first.name != "strong":
        return None
    label = _text(first)
    if not label.endswith(":"):
        return None
    name = label[:-1].strip()
    text = _text(p)[len(label):].strip()
    return (name, text) if name else None


def _is_boilerplate(text: str) -> bool:
    low = text.lower()
    return any(b in low for b in BOILERPLATE)


def parse_transcript(html: str) -> dict:
    """Return {"title", "fiscal": (year, quarter) | None, "raw_text", "turns": [...]}, where each
    turn is {"speaker", "speaker_title", "role", "section", "text"} in page order."""
    soup = BeautifulSoup(html, "html.parser")
    title = _text(soup.title.get_text()) if soup.title else None
    body = _article_body(soup)

    turns: list[dict] = []            # {"speaker", "speaker_title", "text"}
    paragraphs: list[str] = []        # everything kept for raw_text
    participants: dict[str, str | None] = {}
    mode = "body"                     # body | participants
    header: tuple[str, str | None] | None = None  # pending old-style speaker header

    for el in body.find_all(["p", "h2", "h3", "h4"]):
        text = _text(el)
        if not text:
            continue
        low = text.lower().rstrip(":")
        if el.name != "p":
            if low.startswith("call participants"):
                mode = "participants"
            elif mode == "participants":
                mode = "after"
            header = None
            continue
        if mode == "participants":
            nt = _split_name_title(el)
            if nt:
                participants.setdefault(nt[0], nt[1])
            continue
        if mode == "after" or _is_boilerplate(text):
            continue
        if low.startswith("call participants"):        # heading rendered as <p>
            mode = "participants"
            continue

        inline = _inline_turn(el)
        if inline and inline[0].lower() in NOT_SPEAKERS:   # e.g. 'Duration: 61 minutes'
            paragraphs.append(text)
            continue
        if inline:
            name, body_text = inline
            if body_text:
                turns.append({"speaker": name, "speaker_title": None, "text": body_text})
                paragraphs.append(f"{name}: {body_text}")
                header = None
            else:                                       # '<strong>Name:</strong>' alone = header
                header = (name, None)
            continue
        nt = _split_name_title(el)
        if nt and el.find("strong") is not None and nt[0].lower() not in NOT_SPEAKERS:
            header = nt                                # '<strong>Name</strong> -- <em>Title</em>'
            continue
        if header:
            name, ttl = header
            if turns and turns[-1]["speaker"] == name and turns[-1].get("_open"):
                turns[-1]["text"] += "\n\n" + text
            else:
                turns.append({"speaker": name, "speaker_title": ttl, "text": text, "_open": True})
            paragraphs.append(f"{name}: {text}")
            continue
        paragraphs.append(text)

    # sections: prepared until the first Operator turn mentioning "question" after remarks began
    section, seen_remarks = "prepared", False
    for t in turns:
        is_op = t["speaker"].lower() == "operator"
        if section == "prepared" and is_op and seen_remarks and "question" in t["text"].lower():
            section = "qa"
        if not is_op:
            seen_remarks = True
        t["section"] = section

    prepared_speakers = {t["speaker"] for t in turns if t["section"] == "prepared" and t["speaker"].lower() != "operator"}
    for t in turns:
        name = t["speaker"]
        ttl = t.get("speaker_title") or participants.get(name)
        t["speaker_title"] = ttl
        if name.lower() == "operator":
            t["role"] = "operator"
        elif ttl and any(w in ttl.lower() for w in ANALYST_WORDS):
            t["role"] = "analyst"
        elif name in prepared_speakers or (name in participants and ttl):
            t["role"] = "exec"
        else:
            t["role"] = "unknown"
        t.pop("_open", None)

    return {"title": title, "fiscal": parse_title(title), "raw_text": "\n\n".join(paragraphs), "turns": turns}


# --------------------------------------------------------------------------------- db
def _pending(c, limit: int | None) -> list[dict]:
    sql = """SELECT url, symbol, source, fiscal_year, fiscal_quarter, call_date
             FROM transcript_sources WHERE status = 'pending'
             ORDER BY call_date DESC NULLS LAST, url"""
    params: tuple = ()
    if limit:
        sql += " LIMIT %s"
        params = (limit,)
    with c.cursor() as cur:
        cur.execute(sql, params)
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


def _write(c, src: dict, year: int, quarter: int, html: str, parsed: dict) -> int:
    raw_text = parsed["raw_text"]
    with c.cursor() as cur:
        cur.execute("""
            INSERT INTO transcripts (symbol, fiscal_year, fiscal_quarter, call_date, source, source_url,
                                     raw_html, raw_text, word_count, parsed_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,now())
            ON CONFLICT (symbol, fiscal_year, fiscal_quarter, source) DO UPDATE SET
              call_date=EXCLUDED.call_date, source_url=EXCLUDED.source_url, raw_html=EXCLUDED.raw_html,
              raw_text=EXCLUDED.raw_text, word_count=EXCLUDED.word_count, parsed_at=now()
            RETURNING id""",
            (src["symbol"], year, quarter, src["call_date"], src["source"], src["url"],
             html, raw_text, len(raw_text.split())))
        tid = cur.fetchone()[0]
        cur.execute("DELETE FROM transcript_segments WHERE transcript_id = %s", (tid,))
        cur.executemany("""
            INSERT INTO transcript_segments (transcript_id, seq, speaker, speaker_title, role, section, text, word_count)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
            [(tid, i, t["speaker"], t["speaker_title"], t["role"], t["section"], t["text"], len(t["text"].split()))
             for i, t in enumerate(parsed["turns"], 1)])
        cur.execute("""UPDATE transcript_sources SET status='parsed', fetched_at=now(), error=NULL
                       WHERE url = %s""", (src["url"],))
    c.commit()
    return 1 + len(parsed["turns"])


def _fail(c, url: str, fetched: bool, err: str) -> None:
    c.rollback()
    with c.cursor() as cur:
        cur.execute("""UPDATE transcript_sources SET status='failed', error=%s,
                       fetched_at=CASE WHEN %s THEN now() ELSE fetched_at END WHERE url = %s""",
                    (err[:2000], fetched, url))
    c.commit()


# -------------------------------------------------------------------------------- job
def run(c, limit: int | None = None) -> None:
    http = httpx.Client(timeout=30.0, follow_redirects=True,
                        headers={"User-Agent": USER_AGENT, "Accept": "text/html,application/xhtml+xml",
                                 "Accept-Language": "en-US,en;q=0.9"})
    with db.run_log(c, "transcripts") as stats:
        queue = _pending(c, limit)
        log.info("transcripts: %d pending%s", len(queue), f" (limit {limit})" if limit else "")
        ok = failed = 0
        t0 = time.monotonic()
        for i, src in enumerate(queue, 1):
            if i > 1:
                time.sleep(REQUEST_DELAY_S)
            html = None
            try:
                html = fetch(http, src["url"])
                parsed = parse_transcript(html)
                fiscal = parsed["fiscal"] or (
                    (src["fiscal_year"], src["fiscal_quarter"]) if src["fiscal_year"] and src["fiscal_quarter"] else None)
                if not fiscal:
                    raise ValueError(f"no fiscal quarter in title {parsed['title']!r} or queue")
                if not parsed["turns"]:
                    raise ValueError("no speaker turns found")
                stats["rows"] += _write(c, src, fiscal[0], fiscal[1], html, parsed)
                ok += 1
                log.info("transcripts: %s Q%d %d parsed, %d turns (%s)", src["symbol"], fiscal[1], fiscal[0],
                         len(parsed["turns"]), src["url"])
            except Exception as e:  # noqa: BLE001
                failed += 1
                log.warning("transcripts: %s failed: %s (%s)", src["symbol"], e, src["url"])
                _fail(c, src["url"], html is not None, f"{type(e).__name__}: {e}")
            if i % 25 == 0 or i == len(queue):
                log.info("transcripts: %d/%d done (%d parsed, %d failed) in %.0fs",
                         i, len(queue), ok, failed, time.monotonic() - t0)
    http.close()

