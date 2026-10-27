"""Run the queued pipeline fixes in ops.fix_requests.

The weekly batch in recursive-mentions-markets (`rmm weekly --save`) queues fixes it is allowed to
make on its own: backfills and re-fetches that are safe to repeat. This runs them, one at a time,
and records each outcome. Only the commands and argument shapes in ALLOWED run; anything else is
marked rejected. Requests older than MAX_AGE_DAYS are rejected too, so a stale queue never fires.
"""
from __future__ import annotations

import io
import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Callable

from . import db

log = logging.getLogger(__name__)
MAX_AGE_DAYS = 7
SYMBOL = re.compile(r"^[A-Z0-9.]{1,12}$")

# command -> accepted argument lists (each a function of the args that returns True when allowed)
ALLOWED: dict[str, Callable[[list[str]], bool]] = {
    "sweep": lambda a: a == [],
    "precall": lambda a: a == ["--refetch-missing"],
    "equibles": lambda a: a == [],
    "fortune": lambda a: len(a) == 2 and a[0] == "--symbol" and bool(SYMBOL.match(a[1])),
    "transcripts": lambda a: len(a) == 2 and a[0] == "--backfill-symbol" and bool(SYMBOL.match(a[1])),
    "filings": lambda a: len(a) == 2 and a[0] == "--backfill-symbol" and bool(SYMBOL.match(a[1])),
}


def allowed(command: str, args: list[str]) -> bool:
    check = ALLOWED.get(command)
    return bool(check and check(list(args)))


def _claim(c) -> dict | None:
    """Take the oldest queued request, marking it running (skips rows another run holds)."""
    with c.cursor() as cur:
        cur.execute("""update ops.fix_requests set status = 'running', started_at = now()
                       where id = (select id from ops.fix_requests where status = 'queued'
                                   order by created_at, id limit 1 for update skip locked)
                       returning id, created_at, command, args""")
        row = cur.fetchone()
    c.commit()
    return dict(zip(("id", "created_at", "command", "args"), row)) if row else None


def _finish(c, rid: int, status: str, output: str) -> None:
    with c.cursor() as cur:
        cur.execute("update ops.fix_requests set status = %s, finished_at = now(), output = %s where id = %s",
                    (status, output[-4000:], rid))
    c.commit()


def run(run_command: Callable[[str, list[str]], None]) -> None:
    """run_command is __main__.run, so each fix runs exactly as it would from the command line."""
    done = failed = rejected = 0
    with db.conn() as c, db.run_log(c, "ops-fixes") as stats:
        while (req := _claim(c)) is not None:
            cmd, args = req["command"], list(req["args"] or [])
            label = " ".join([cmd, *args])
            age = datetime.now(timezone.utc) - req["created_at"]
            if not allowed(cmd, args) or age > timedelta(days=MAX_AGE_DAYS):
                why = "not on the allowlist" if not allowed(cmd, args) else f"older than {MAX_AGE_DAYS} days"
                log.warning("ops-fixes: #%d %s rejected: %s", req["id"], label, why)
                _finish(c, req["id"], "rejected", why)
                rejected += 1
                continue
            buf = io.StringIO()
            handler = logging.StreamHandler(buf)
            handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
            logging.getLogger().addHandler(handler)
            log.info("ops-fixes: #%d running %s", req["id"], label)
            try:
                run_command(cmd, args)
                status = "done"
                done += 1
            except Exception as e:  # noqa: BLE001  (one failed fix must not stop the rest)
                log.exception("ops-fixes: #%d %s failed", req["id"], label)
                buf.write(f"\nFAILED: {e}\n")
                status = "failed"
                failed += 1
            finally:
                logging.getLogger().removeHandler(handler)
            _finish(c, req["id"], status, buf.getvalue())
            stats["rows"] += 1
        log.info("ops-fixes: %d done, %d failed, %d rejected", done, failed, rejected)
