# kalshi-worker

Ingests Kalshi market history into Neon Postgres (schema from `001_kalshi_schema.sql` + `002_fixed_point_types.sql`).

## Commands
| command | what | how it runs on Railway |
|---|---|---|
| `backfill` | daily candles for settled markets that have none yet; resumable | one-off (`railway run python -m kalshi_worker backfill`) |
| `sync` | open events + markets, hourly candles, watchlist trades + 1-min candles, then the stale sweep (`--max N`, default 20000) | cron service, `0 * * * *` |
| `reconcile` | complete candles for recently settled markets, then the stale sweep (`--max N`) | cron service, `15 4 * * *` |
| `sweep` | only the stale sweep: re-fetch non-finalized/settled markets not in the open-events pull via `/markets?tickers=`, least recently updated first, up to `--max N` (default 20000) | one-off catch-up |
| `transcripts` | discover new Motley Fool transcript URLs for companies with earnings-mention markets from the monthly sitemaps (`--discover-months N`, default 2; symbols never backfilled get 36 months once, marked by `transcripts_backfill:<SYMBOL>` in `sync_state`; `--backfill-symbol SYM` clears the marker so this run re-scans SYM), then fetch + parse pending ones into `transcripts` / `transcript_segments`; `--limit N` for testing; `--reparse` re-runs the parser on stored HTML for transcripts whose segments hold under 70% of their words or whose page date was never parsed; `sql/` holds the schema additions and the `v_transcript_mentions` base-rate view | on demand (`railway run python -m kalshi_worker transcripts`) |
| `fortune` | Fortune.com (Quartr) transcripts as a second source (`source='fortune'`): finds `company_map.fortune_slug` for tracked companies without one (name-based candidates checked against the page's ticker; re-checked every 30 days), then stores new calls from each company's earnings reports as `transcripts` + speaker-merged `transcript_segments`; calls whose transcript isn't posted yet stay `pending` in `transcript_sources` and are retried; `--symbol SYM`, `--limit N` (report pages); 1 request/second | runs at the end of `transcripts`, or on demand |
| `equibles` | Equibles API transcripts only for calls no other source has: works `kalshi.v_transcript_gaps` (label-audit `no_transcript` events and 2 years of earnings releases with no transcript within 3 days; `sql/009_equibles.sql`), newest first, each gap at most once per 7 days (`kalshi.equibles_attempts`); settled Kalshi calls first, then newest; per symbol one `/investor-events` listing, the `EarningsCall` with a transcript within 5 days of the target, then its `/speakers` pages (50 turns each) stored as `source='equibles'`; at most 90 requests per run (every request counts), stops cleanly on the budget or 429 without storing a partial call; `--limit N` gaps; needs `EQUIBLES_API_KEY` | runs at the end of `transcripts` after `fortune`, or on demand |
| `filings` | 8-K earnings press releases (Item 2.02, Exhibit 99.1) and foreign filers' 6-K results releases (Exhibit 99.1 announcing results in its first 1,500 characters; stored with form `6-K`, no items) from SEC EDGAR for companies with earnings-mention markets into `filings`; symbols never backfilled get three years once, older submissions pages included (`filings_backfill:<SYMBOL>` in `sync_state`; `--backfill-symbol SYM` re-runs it); plus corporate-event 8-K / 8-K/As (items 1.01, 1.02, 2.01, 2.05, 2.06, 5.02) into `corporate_events` and SIC codes into `company_map`; `--days N` (default 3, use 1100 for the backfill); `--reparse` splits stored rows into `body_text` / `boilerplate_text`; needs `SEC_CONTACT_EMAIL` | cron service, daily, or on demand |
| `reactions` | 1-minute candles from 3h before each 8-K earnings release to 5 min after close for the filer's mention markets closing within 36h; feeds `v_release_reaction`; `--days N` (default 3, 90 for the backfill) | cron service, daily, or on demand |
| `worker` | always-on; takes an order-book snapshot for watchlist series every N min | default service |

`transcripts` and `filings` both end by logging the tracked symbols with no transcripts / filings and refreshing `kalshi.mv_word_counts` after running `kalshi.resolve_speaker_roles()` (a failure in either is only logged; `sync` does the same after its stale sweep). `sql/008_source_coverage.sql` adds `v_source_coverage`: one row per earnings-mention symbol with transcript / filing counts, latest call / filing, CIK and the two backfill flags.

## Railway setup
1. Create the repo on GitHub, push this directory.
2. In the `kalshi-data` project: **+ New → GitHub Repo** → this repo. Name the service `worker`. Uses the Dockerfile CMD.
3. Add two more services from the same repo: `sync` (start command `python -m kalshi_worker sync`, cron `0 * * * *`) and `reconcile` (start command `python -m kalshi_worker reconcile`, cron `15 4 * * *`).
4. Shared variables: `DATABASE_URL` (Neon pooled string with `?sslmode=require`), optionally `KALSHI_RPS`, `SNAPSHOT_INTERVAL_MIN`.
5. Kick off the backfill once from the `worker` service's shell or `railway run`.

## Local
```
cp .env.example .env   # fill DATABASE_URL
pip install -r requirements.txt
python -m kalshi_worker sync
```

## Monitoring
`SELECT job, max(finished_at), bool_and(ok) FROM kalshi.ingest_runs WHERE started_at > now()-interval '1 day' GROUP BY 1;`
