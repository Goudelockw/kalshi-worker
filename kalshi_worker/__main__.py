"""CLI entry point.

  python -m kalshi_worker backfill     one-time daily-candle backfill (resumable)
  python -m kalshi_worker sync [--max N]        hourly refresh          (Railway cron)
  python -m kalshi_worker reconcile [--max N]   nightly settlement lock (Railway cron)
  python -m kalshi_worker sweep [--max N]       just the stale sweep: re-fetch non-final
                                       markets via /markets?tickers=, least recently updated
                                       first, up to N per run (default 20000; also caps the
                                       sweep at the end of sync and reconcile)
  python -m kalshi_worker snapshot     one order-book snapshot pass
  python -m kalshi_worker transcripts [--limit N] [--discover-months N] [--reparse] [--backfill-symbol SYM]
                                       discover new Fool transcript URLs from the monthly
                                       sitemaps (default 2 months; 36 for symbols never
                                       backfilled), then parse pending rows; --reparse re-parses
                                       stored HTML instead; --backfill-symbol clears SYM's
                                       backfill marker first so this run re-scans 36 months for it
  python -m kalshi_worker filings [--days N] [--reparse] [--backfill-symbol SYM]
                                       8-K earnings press releases (Item 2.02 / Exhibit 99.1)
                                       and 6-K results releases from SEC EDGAR, last N days
                                       (default 3; three years for symbols never backfilled);
                                       --reparse only splits body/boilerplate for stored rows
                                       lacking it; --backfill-symbol clears SYM's backfill marker
                                       first so this run looks back three years for it
  python -m kalshi_worker fortune [--symbol SYM] [--limit N]   Fortune.com (Quartr) transcripts:
                                       find missing company slugs, then store new / pending
                                       calls (also runs at the end of `transcripts`)
  python -m kalshi_worker equibles [--limit N]   Equibles transcripts for calls no other source has
                                       (kalshi.v_transcript_gaps), at most 90 API requests per run;
                                       needs EQUIBLES_API_KEY (also runs at the end of `transcripts`)
  python -m kalshi_worker precall [--limit N] [--refetch-missing]   hourly candles from 48h before
                                       each settled earnings-mention call's start to 3h after it
                                       (and 1h past the close); newest events first, --limit caps
                                       events; reconcile runs it for the last 3 days.
                                       --refetch-missing: one-off re-fetch for events with incomplete
                                       prices in mv_precall_prices, then the view refresh
  python -m kalshi_worker mentions-backfill   one-off: every KXEARNINGSMENTION series, its events and
                                       settled markets (archive + live tier), then precall for
                                       settled markets without hourly windows; resumable per series
  python -m kalshi_worker reactions [--days N]   1-minute candles around earnings press
                                       releases for mention markets (default 3; 90 for backfill)
  python -m kalshi_worker news [--backfill]   pre-call Google News counts (Serper) per earnings-mention
                                       market: company + word and company alone over the 14 days
                                       before the call day, nothing at or after call start - 2h;
                                       calls starting in 3h-3 days, or --backfill: settled markets
                                       without a news_counts row, newest first; at most 2,000
                                       requests per run; needs SERPER_API_KEY. New articles count
                                       only after their page's publish time is verified (below),
                                       which the run does for its own articles before it ends
  python -m kalshi_worker news-dates [--limit N]   exact publish times for unverified news articles,
                                       read from each page (JSON-LD, meta tags, <time> in <article>);
                                       recomputes counted and news_counts; newest calls first;
                                       5 pages/s overall, 1/s per domain
  python -m kalshi_worker worker       always-on: snapshot every N minutes
"""
import logging
import os
import sys

from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("kalshi_worker")

from . import db, equibles, filings, fortune, jobs, mentions, news, news_dates, precall, reactions, transcripts  # noqa: E402  (after load_dotenv so DATABASE_URL is present)
from .client import KalshiClient  # noqa: E402


def _opt(name: str, args: list[str]) -> int | None:
    """--name N or --name=N from the remaining argv."""
    for i, a in enumerate(args):
        if a == f"--{name}" and i + 1 < len(args):
            return int(args[i + 1])
        if a.startswith(f"--{name}="):
            return int(a.split("=", 1)[1])
    return None


def _sopt(name: str, args: list[str]) -> str | None:
    """--name VALUE or --name=VALUE from the remaining argv, as a string."""
    for i, a in enumerate(args):
        if a == f"--{name}" and i + 1 < len(args):
            return args[i + 1]
        if a.startswith(f"--{name}="):
            return a.split("=", 1)[1]
    return None


def run(cmd: str, args: list[str] = ()) -> None:
    k = KalshiClient()
    with db.conn() as c:
        if cmd == "backfill":
            jobs.backfill(k, c)
        elif cmd == "sync":
            jobs.sync(k, c, sweep_max=_opt("max", list(args)) or jobs.SWEEP_MAX)
        elif cmd == "reconcile":
            jobs.reconcile(k, c, sweep_max=_opt("max", list(args)) or jobs.SWEEP_MAX)
        elif cmd == "sweep":
            jobs.sweep(k, c, max_markets=_opt("max", list(args)) or jobs.SWEEP_MAX)
        elif cmd == "snapshot":
            jobs.snapshot(k, c, int(os.getenv("SNAPSHOT_INTERVAL_MIN", "5")))
        elif cmd == "transcripts":
            if "--reparse" in args:
                transcripts.reparse(c, limit=_opt("limit", list(args)))
            else:
                if sym := _sopt("backfill-symbol", list(args)):
                    transcripts.reset_backfill(c, sym.upper())
                transcripts.run(c, limit=_opt("limit", list(args)),
                                discover_months=_opt("discover-months", list(args)) or transcripts.DISCOVER_MONTHS)
        elif cmd == "filings":
            if "--reparse" in args:
                filings.reparse(c)
            else:
                if sym := _sopt("backfill-symbol", list(args)):
                    filings.reset_backfill(c, sym.upper())
                filings.run(c, days=_opt("days", list(args)) or filings.DEFAULT_DAYS)
        elif cmd == "fortune":
            sym = _sopt("symbol", list(args))
            fortune.run(c, symbol=sym.upper() if sym else None, limit=_opt("limit", list(args)))
        elif cmd == "equibles":
            equibles.run(c, limit=_opt("limit", list(args)))
        elif cmd == "mentions-backfill":
            mentions.run(k, c)
        elif cmd == "precall":
            precall.run(k, c, limit=_opt("limit", list(args)), refetch="--refetch-missing" in args)
        elif cmd == "news":
            news.run(c, backfill="--backfill" in args)
        elif cmd == "news-dates":
            news_dates.run(c, limit=_opt("limit", list(args)))
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
