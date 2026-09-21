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
PART_SEP_RE = re.compile(r"\s+(?:--|—|–|-)\s+")           # participants lines also use ' - '
ITEM_RE = re.compile(r"<(?:li|p)[^>]*>(.*?)</(?:li|p)>", re.S | re.I)
ROLE_WORDS_RE = re.compile(r"\b(?:officer|president|chief|vice|director|head|analyst|relations|manager|"
                           r"chair|chairman|founder|treasurer|secretary|counsel|partner|executive|"
                           r"senior|managing|general|operating|financial|technology|strategy)\b", re.I)
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
# legacy (pre-April-2025) template: sections by <h2>, speaker lines <p><strong>Name</strong> -- <em>Title</em></p>
PREPARED_H2 = "prepared remarks"
QA_H2 = "questions & answers"
LEGACY_SPEAKER_RE = re.compile(
    r"<p[^>]*>\s*<strong>([^<]+)</strong>\s*(?:(?:--|&mdash;|—|–)\s*<em>([^<]*)</em>)?\s*</p>", re.S | re.I)


BLOCK_TAG_RE = re.compile(r"</?(?:p|div|li|ul|ol|h\d|br|tr|td|th|table|blockquote)\b[^>]*>", re.I)


def _text(fragment: str) -> str:
    """HTML fragment -> plain text: block tags and <br> become a space, inline tags (<em>,
    <strong>, <a>) vanish, entities are decoded, whitespace is collapsed."""
    fragment = BLOCK_TAG_RE.sub(" ", fragment)
    return re.sub(r"\s+", " ", html.unescape(re.sub(r"<[^>]+>", "", fragment))).strip()


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
    """'CALL PARTICIPANTS' lines -> {name: title}. Current pages list '<li>Title - Name</li>';
    older ones '<p><strong>Name</strong> -- Title</p>'. Whichever side looks like a job title
    (role words, or more than four words) is the title; the other is the name."""
    out: dict[str, str | None] = {}
    for m in ITEM_RE.finditer(section_html or ""):
        text = _text(m.group(1))
        if not text or len(text) > 160:
            continue
        parts = [x.strip() for x in PART_SEP_RE.split(text) if x.strip()]
        if len(parts) < 2:
            out.setdefault(text, None)
            continue
        titleish = [bool(ROLE_WORDS_RE.search(x)) or len(x.split()) > 4 for x in parts]
        name_idx = titleish.index(False) if False in titleish else len(parts) - 1
        name = parts[name_idx].rstrip(":")
        title = " -- ".join(x for i, x in enumerate(parts) if i != name_idx) or None
        out.setdefault(name, title)
    return out


def _is_boilerplate(text: str) -> bool:
    low = text.lower()
    return any(b in low for b in BOILERPLATE)


def parse_transcript(page: str) -> dict:
    """Return {"title", "fiscal": (year, quarter) | None, "template", "raw_text", "turns": [...]},
    where each turn is {"speaker", "speaker_title", "role", "section", "text"} in page order.
    Two Fool templates: the current one (a "Full Conference Call Transcript" <h2>) and the
    legacy one (<h2>Prepared Remarks:</h2> / <h2>Questions & Answers:</h2>)."""
    tm = re.search(r"<title[^>]*>(.*?)</title>", page, re.S | re.I)
    title = _text(tm.group(1)) if tm else None
    sections = _sections(page)
    if _section(sections, TRANSCRIPT_H2) is not None:
        template, (turns, paragraphs) = "current", _parse_current(sections)
    elif _section(sections, PREPARED_H2) is not None:
        template, (turns, paragraphs) = "legacy", _parse_legacy(sections)
    else:
        raise ValueError(f"neither '{TRANSCRIPT_H2}' nor '{PREPARED_H2}' <h2> in page")
    return {"title": title, "fiscal": parse_title(title), "template": template,
            "raw_text": "\n\n".join(paragraphs), "turns": turns}


def _parse_current(sections: dict[str, str]) -> tuple[list[dict], list[str]]:
    """Current template. Only the HTML after the "Full Conference Call Transcript" <h2> is read
    for turns. A turn starts at <p><strong>Name:</strong> text</p> and continues through every
    following plain <p> until the next <p><strong> line or the end of the article. Roles: Operator by name; speakers listed under "CALL PARTICIPANTS" are exec (or
    analyst when the title says so); unlisted speakers are analyst when all their turns are in
    Q&A, else unknown."""
    body = _section(sections, TRANSCRIPT_H2)
    participants = _participants(_section(sections, PARTICIPANTS_H2))

    turns: list[dict] = []
    paragraphs: list[str] = []
    cur: dict | None = None            # the turn that following plain <p>s belong to
    for m in P_RE.finditer(body):
        raw = m.group(0)
        turn = TURN_RE.fullmatch(raw) if raw.startswith("<p>") else None
        if turn:
            name, text = html.unescape(turn.group(1)).strip(), _text(turn.group(2))
            if name.lower() in NOT_SPEAKERS:
                paragraphs.append(f"{name}: {text}")
                cur = None
            else:
                cur = {"speaker": name, "text": text}
                turns.append(cur)
                if text:
                    paragraphs.append(f"{name}: {text}")
            continue
        if re.match(r"<p[^>]*>\s*<strong>", raw, re.I):   # bold-led line that isn't a turn: ends the turn
            cur = None
        text = _text(m.group(1))
        if not text or _is_boilerplate(text):
            continue
        if cur is not None:                                # continuation paragraph of the open turn
            paragraphs.append(text if cur["text"] else f"{cur['speaker']}: {text}")
            cur["text"] = f"{cur['text']} {text}".strip()
        else:
            paragraphs.append(text)
    turns = [t for t in turns if t["text"]]

    # sections: prepared until the first Operator turn mentioning "question" after remarks began
    section, seen_remarks = "prepared", False
    for t in turns:
        is_op = t["speaker"].lower() == "operator"
        if section == "prepared" and is_op and seen_remarks and "question" in t["text"].lower():
            section = "qa"
        if not is_op:
            seen_remarks = True
        t["section"] = section

    in_prepared = {t["speaker"] for t in turns if t["section"] == "prepared"}
    for t in turns:
        name = t["speaker"]
        ttl = participants.get(name)
        t["speaker_title"] = ttl
        if name.lower() == "operator":
            t["role"] = "operator"
        elif name in participants:
            t["role"] = "analyst" if ttl and any(w in ttl.lower() for w in ANALYST_WORDS) else "exec"
        elif name not in in_prepared:          # unlisted and only ever speaks in Q&A
            t["role"] = "analyst"
        else:                                  # spoke in prepared remarks but isn't listed
            t["role"] = "unknown"

    return turns, paragraphs


def _parse_legacy(sections: dict[str, str]) -> tuple[list[dict], list[str]]:
    """Legacy template. Turns live under <h2>Prepared Remarks:</h2> and <h2>Questions &
    Answers:</h2> (the parser stops at <h2>Call participants:</h2>). A speaker line is
    <p><strong>Name</strong> -- <em>Title</em></p> (Operator has no title); every following <p>
    up to the next speaker line is that speaker's text, joined with a space. Roles: operator
    for Operator, analyst when the title contains "Analyst", exec otherwise."""
    turns: list[dict] = []
    paragraphs: list[str] = []
    for section, key in (("prepared", PREPARED_H2), ("qa", QA_H2)):
        body = _section(sections, key)
        if body is None:
            continue
        cur: dict | None = None
        for m in P_RE.finditer(body):
            sp = LEGACY_SPEAKER_RE.fullmatch(m.group(0))
            if sp:
                name = _text(sp.group(1))
                ttl = _text(sp.group(2)) if sp.group(2) else None
                if name.lower() in NOT_SPEAKERS:
                    cur = None
                    continue
                if name.lower() == "operator":
                    role = "operator"
                elif ttl and "analyst" in ttl.lower():
                    role = "analyst"
                else:
                    role = "exec"
                cur = {"speaker": name, "speaker_title": ttl, "role": role, "section": section, "text": ""}
                turns.append(cur)
                continue
            text = _text(m.group(1))
            if not text or _is_boilerplate(text):
                continue
            if re.match(r"^(?:Duration|Contents):", text):
                paragraphs.append(text)
                continue
            if cur is None:
                paragraphs.append(text)
                continue
            paragraphs.append(text if cur["text"] else f"{cur['speaker']}: {text}")
            cur["text"] = f"{cur['text']} {text}".strip()
    turns = [t for t in turns if t["text"]]
    return turns, paragraphs


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
        _write_segments(cur, tid, parsed["turns"])
        cur.execute("""UPDATE transcript_sources SET status='parsed', fetched_at=now(), error=NULL
                       WHERE url = %s""", (src["url"],))
    c.commit()
    return 1 + len(parsed["turns"])


def _write_segments(cur, tid: int, turns: list[dict]) -> None:
    cur.execute("DELETE FROM transcript_segments WHERE transcript_id = %s", (tid,))
    cur.executemany("""
        INSERT INTO transcript_segments (transcript_id, seq, speaker, speaker_title, role, section, text, word_count)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
        [(tid, i, t["speaker"], t["speaker_title"], t["role"], t["section"], t["text"], len(t["text"].split()))
         for i, t in enumerate(turns, 1)])


def _truncated(c, limit: int | None, ratio: float) -> list[dict]:
    """Stored transcripts whose segments hold less than `ratio` of the transcript's word_count."""
    sql = """SELECT t.id, t.symbol, t.fiscal_year, t.fiscal_quarter, t.raw_html,
                    t.word_count, coalesce(sum(s.word_count), 0) AS segment_words
             FROM transcripts t LEFT JOIN transcript_segments s ON s.transcript_id = t.id
             WHERE t.raw_html IS NOT NULL
             GROUP BY t.id
             HAVING coalesce(sum(s.word_count), 0) < %s * coalesce(t.word_count, 0)
             ORDER BY t.call_date DESC NULLS LAST, t.id"""
    params: tuple = (ratio,)
    if limit:
        sql += " LIMIT %s"
        params += (limit,)
    with c.cursor() as cur:
        cur.execute(sql, params)
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


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
                log.info("transcripts: %s Q%d %d parsed (%s template), %d turns (%s)", src["symbol"], fiscal[1],
                         fiscal[0], parsed["template"], len(parsed["turns"]), src["url"])
            except Exception as e:  # noqa: BLE001
                failed += 1
                log.warning("transcripts: %s failed: %s (%s)", src["symbol"], e, src["url"])
                _fail(c, src["url"], html is not None, f"{type(e).__name__}: {e}")
            if i % 25 == 0 or i == len(queue):
                log.info("transcripts: %d/%d done (%d parsed, %d failed) in %.0fs",
                         i, len(queue), ok, failed, time.monotonic() - t0)
    http.close()




REPARSE_RATIO = 0.7


def reparse(c, limit: int | None = None) -> None:
    """Re-run the parser on stored raw_html (no HTTP) for every transcript whose segments
    hold less than REPARSE_RATIO of its word_count; rewrite raw_text and its segments."""
    with db.run_log(c, "transcripts_reparse") as stats:
        rows = _truncated(c, limit, REPARSE_RATIO)
        log.info("transcripts: reparse %d transcripts with segments under %.0f%% of word_count%s",
                 len(rows), REPARSE_RATIO * 100, f" (limit {limit})" if limit else "")
        ok = failed = 0
        for i, t in enumerate(rows, 1):
            label = f"{t['symbol']} Q{t['fiscal_quarter']} {t['fiscal_year']} (id {t['id']})"
            try:
                parsed = parse_transcript(t["raw_html"])
                if not parsed["turns"]:
                    raise ValueError("no speaker turns found")
                raw_text = parsed["raw_text"]
                with c.cursor() as cur:
                    cur.execute("UPDATE transcripts SET raw_text=%s, word_count=%s, parsed_at=now() WHERE id=%s",
                                (raw_text, len(raw_text.split()), t["id"]))
                    _write_segments(cur, t["id"], parsed["turns"])
                c.commit()
                stats["rows"] += 1 + len(parsed["turns"])
                ok += 1
                seg_words = sum(len(x["text"].split()) for x in parsed["turns"])
                log.info("transcripts: reparsed %s (%s template): %d turns, segment words %s -> %d",
                         label, parsed["template"], len(parsed["turns"]), t["segment_words"], seg_words)
            except Exception as e:  # noqa: BLE001
                c.rollback()
                failed += 1
                log.warning("transcripts: reparse failed for %s: %s", label, e)
            if i % 25 == 0 or i == len(rows):
                log.info("transcripts: reparse %d/%d done (%d ok, %d failed)", i, len(rows), ok, failed)
