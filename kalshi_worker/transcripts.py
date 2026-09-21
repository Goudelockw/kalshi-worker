"""Motley Fool earnings-call transcript ingestion.

Works the kalshi.transcript_sources queue (status='pending', newest call first): fetch the
page with a browser User-Agent, keep the HTML, split it into speaking turns, tag each turn
with a section (prepared / qa) and a speaker role (operator / exec / analyst / unknown), and
write one transcripts row plus its transcript_segments. One second between requests.
"""
from __future__ import annotations

import html
import logging
import re
import time

import httpx

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
H2_RE = re.compile(r"<h2[^>]*>(.*?)</h2>", re.S | re.I)
P_RE = re.compile(r"<p[^>]*>(.*?)</p>", re.S | re.I)
# a speaking turn: <p><strong>Name:</strong> text</p>, colon inside the <strong>, no title
TURN_RE = re.compile(r"<p><strong>([^<:]+):</strong>\s*(.*?)</p>", re.S)
TRANSCRIPT_H2 = "full conference call transcript"
PARTICIPANTS_H2 = "call participants"


def _text(fragment: str) -> str:
    """HTML fragment -> plain text: tags out, entities decoded, whitespace collapsed."""
    return re.sub(r"\s+", " ", html.unescape(re.sub(r"<[^>]+>", " ", fragment))).strip()


def parse_title(title: str | None) -> tuple[int, int] | None:
    """'Adobe (ADBE) Q3 2026 Earnings Call Transcript' -> (2026, 3)."""
    m = TITLE_RE.search(title or "")
    return (int(m.group(2)), int(m.group(1))) if m else None


def _sections(page: str) -> dict[str, str]:
    """{h2 text (lower-cased): html between that <h2> and the next one}."""
    heads = list(H2_RE.finditer(page))
    out: dict[str, str] = {}
    for i, h in enumerate(heads):
        end = heads[i + 1].start() if i + 1 < len(heads) else len(page)
        out.setdefault(_text(h.group(1)).lower().rstrip(":"), page[h.end():end])
    return out


def _section(sections: dict[str, str], prefix: str) -> str | None:
    return next((v for k, v in sections.items() if k.startswith(prefix)), None)


def _participants(section_html: str | None) -> dict[str, str | None]:
    """'CALL PARTICIPANTS' lines -> {name: title}. Lines look like 'Name -- Title', with the
    name possibly in <strong>; anything after the name (minus separators) is the title."""
    out: dict[str, str | None] = {}
    for m in P_RE.finditer(section_html or ""):
        strong = re.search(r"<strong>(.*?)</strong>", m.group(1), re.S)
        text = _text(m.group(1))
        if not text or len(text) > 160:
            continue
        if strong is not None and _text(strong.group(1)):
            name = _text(strong.group(1)).rstrip(":")
            title = text[len(_text(strong.group(1))):] if text.startswith(_text(strong.group(1))) else text
        else:
            parts = SEPARATOR_RE.split(text, 1)
            if len(parts) < 2 or len(parts[0].split()) > 5:
                continue
            name, title = parts[0], parts[1]
        title = re.sub(r"^[\s:–—-]+", "", title).strip() or None
        out.setdefault(name.strip(), title)
    return out


def _is_boilerplate(text: str) -> bool:
    low = text.lower()
    return any(b in low for b in BOILERPLATE)


def parse_transcript(page: str) -> dict:
    """Return {"title", "fiscal": (year, quarter) | None, "raw_text", "turns": [...]}, where each
    turn is {"speaker", "speaker_title", "role", "section", "text"} in page order. Only the
    HTML after the "Full Conference Call Transcript" <h2> is read for turns; roles come from
    the "CALL PARTICIPANTS" <h2> section."""
    tm = re.search(r"<title[^>]*>(.*?)</title>", page, re.S | re.I)
    title = _text(tm.group(1)) if tm else None
    sections = _sections(page)
    body = _section(sections, TRANSCRIPT_H2)
    if body is None:
        raise ValueError(f"no '{TRANSCRIPT_H2}' <h2> in page")
    participants = _participants(_section(sections, PARTICIPANTS_H2))

    turns: list[dict] = []
    paragraphs: list[str] = []
    for m in P_RE.finditer(body):
        raw = m.group(0)
        turn = TURN_RE.fullmatch(raw) if raw.startswith("<p>") else None
        if turn:
            name, text = html.unescape(turn.group(1)).strip(), _text(turn.group(2))
            if name.lower() in NOT_SPEAKERS:
                paragraphs.append(f"{name}: {text}")
            elif text:
                turns.append({"speaker": name, "text": text})
                paragraphs.append(f"{name}: {text}")
            continue
        text = _text(m.group(1))
        if text and not _is_boilerplate(text):
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
        ttl = participants.get(name)
        t["speaker_title"] = ttl
        if name.lower() == "operator":
            t["role"] = "operator"
        elif ttl and any(w in ttl.lower() for w in ANALYST_WORDS):
            t["role"] = "analyst"
        elif name in prepared_speakers or (name in participants and ttl):
            t["role"] = "exec"
        else:
            t["role"] = "unknown"

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

