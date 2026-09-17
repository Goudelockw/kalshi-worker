"""CLI entry point.

  python -m kalshi_worker backfill     one-time historical load (resumable)
  python -m kalshi_worker sync         hourly refresh          (Railway cron)
  python -m kalshi_worker reconcile    nightly settlement lock (Railway cron)
  python -m kalshi_worker snapshot     one order-book snapshot pass
  python -m kalshi_worker worker       always-on: snapshot every N minutes
"""
import logging
import os
import sys

from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("kalshi_worker")

from . import db, jobs  # noqa: E402  (after load_dotenv so DATABASE_URL is present)
from .client import KalshiClient  # noqa: E402


def run(cmd: str) -> None:
    k = KalshiClient()
    with db.conn() as c:
        if cmd == "backfill":
            jobs.backfill(k, c, with_candles=os.getenv("BACKFILL_CANDLES", "1") == "1")
        elif cmd == "sync":
            jobs.sync(k, c)
        elif cmd == "reconcile":
            jobs.reconcile(k, c)
        elif cmd == "snapshot":
            jobs.snapshot(k, c, int(os.getenv("SNAPSHOT_INTERVAL_MIN", "5")))
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
    worker() if cmd == "worker" else run(cmd)
