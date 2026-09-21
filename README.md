# kalshi-worker

Ingests Kalshi market history into Neon Postgres (schema from `001_kalshi_schema.sql` + `002_fixed_point_types.sql`).

## Commands
| command | what | how it runs on Railway |
|---|---|---|
| `backfill` | daily candles for settled markets that have none yet; resumable | one-off (`railway run python -m kalshi_worker backfill`) |
| `sync` | open/new/closed markets, hourly candles, watchlist trades + 1-min candles | cron service, `0 * * * *` |
| `reconcile` | lock results and complete candles for recently settled markets | cron service, `15 4 * * *` |
| `transcripts` | fetch + parse pending Motley Fool earnings-call transcripts into `transcripts` / `transcript_segments`; `--limit N` for testing; `--reparse` re-runs the parser on stored HTML for transcripts whose segments hold under 70% of their words or whose page date was never parsed; `sql/` holds the schema additions and the `v_transcript_mentions` base-rate view | on demand (`railway run python -m kalshi_worker transcripts`) |
| `worker` | always-on; takes an order-book snapshot for watchlist series every N min | default service |

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
