"""CLI entry point.

  python -m kalshi_worker backfill     one-time daily-candle backfill (resumable)
  python -m kalshi_worker sync         hourly refresh          (Railway cron)
  python -m kalshi_worker reconcile    nightly settlement lock (Railway cron)
  python -m kalshi_worker snapshot     one order-book snapshot pass
  python -m kalshi_worker transcripts [--limit N] [--discover-months N] [--reparse]
                                       discover new Fool transcript URLs from the monthly
                                       sitemaps (default 2 months), then parse pending rows;
                                       --reparse re-parses stored HTML instead
  python -m kalshi_worker filings [--days N] [--reparse]   8-K earnings press releases
                                       (Item 2.02 / Exhibit 99.1) from SEC EDGAR, last N days
                                       (default 3; 1100 for the backfill); --reparse only
                                       splits body/boilerplate for stored rows lacking it
  python -m kalshi_worker reactions [--days N]   1-minute candles around earnings press
                                       releases for mention markets (default 3; 90 for backfill)
  python -m kalshi_worker worker       always-on: snapshot every N minutes
"""
import logging
import os
import sys

from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("kalshi_worker")

from . import db, filings, jobs, reactions, transcripts  # noqa: E402  (after load_dotenv so DATABASE_URL is present)
from .client import KalshiClient  # noqa: E402


def _opt(name: str, args: list[str]) -> int | None:
    """--name N or --name=N from the remaining argv."""
    for i, a in enumerate(args):
        if a == f"--{name}" and i + 1 < len(args):
            return int(args[i + 1])
        if a.startswith(f"--{name}="):
            return int(a.split("=", 1)[1])
    return None


def run(cmd: str, args: list[str] = ()) -> None:
    k = KalshiClient()
    with db.conn() as c:
        if cmd == "backfill":
            jobs.backfill(k, c)
        elif cmd == "sync":
            jobs.sync(k, c)
        elif cmd == "reconcile":
            jobs.reconcile(k, c)
        elif cmd == "snapshot":
            jobs.snapshot(k, c, int(os.getenv("SNAPSHOT_INTERVAL_MIN", "5")))
        elif cmd == "transcripts":
            if "--reparse" in args:
                transcripts.reparse(c, limit=_opt("limit", list(args)))
            else:
                transcripts.run(c, limit=_opt("limit", list(args)),
                                discover_months=_opt("discover-months", list(args)) or transcripts.DISCOVER_MONTHS)
        elif cmd == "filings":
            if "--reparse" in args:
                filings.reparse(c)
            else:
                filings.run(c, days=_opt("days", list(args)) or filings.DEFAULT_DAYS)
        elif cmd == "reactions":
            reactions.run(k, c, days=_opt("days", list(args)) or reactions.DEFAULT_DAYS)
        else:
            raise SystemExit(f"unknown command {cmd!r}")


def worker() -> None:
    from apscheduler.schedulers.blocking import BlockingScheduler

    interval = int(os.getenv("SNAPSHOT_INTERVAL_MIN", "5"))
    sched = BlockingScheduler(timezone="UTC")
    sched.add_job(lambda: run("snapshot"), "cron", minute=f"*/{interval}", misfire_grace_time=60,
                  coalesce=True, max_instances=1)
    log.info("worker up: snapshots every %d min", interval)
    sched.start()


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "worker"
    worker() if cmd == "worker" else run(cmd, sys.argv[2:])
