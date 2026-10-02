"""Inputs for the incentive labeling pass (rubric incentive_v1): the valence excerpts (most recent prior call
that used the word + this quarter's release) plus OLDER excerpts from the latest call before the company's
previous call, so Claude can rate the topic as of that previous call too.
Usage: python incentive_inputs.py <outdir> [--only-missing] [--horizon-days N] [--per-file N] [--tickers-from DIR]"""
import sys, json, os, math, glob
from concurrent.futures import ThreadPoolExecutor
from nsql import q
from valence_inputs import build_rows, write_batches
outdir = sys.argv[1]; os.makedirs(outdir, exist_ok=True)
arg = lambda k, d=None: sys.argv[sys.argv.index(k)+1] if k in sys.argv else d
only_missing = "--only-missing" in sys.argv
horizon = int(arg("--horizon-days")) if arg("--horizon-days") else None
per_file = int(arg("--per-file", 220))

OLDER = r"""
with mk as (
  select w.ticker, w.symbol, m.yes_sub_title, (kalshi.mention_patterns(m.yes_sub_title))[1] pat, w.call_date_est
  from kalshi.mv_word_counts w join kalshi.markets m on m.ticker = w.ticker
  where w.ticker = any($1::text[])
), prevcall as (   -- the company's previous call
  select mk.ticker, max(c.call_date) prev_call_date
  from mk join (select distinct symbol, call_date from kalshi.mv_word_call_counts) c
    on c.symbol = mk.symbol and c.call_date < mk.call_date_est - 2
  group by mk.ticker
), older as (      -- latest call before that one where company speakers used the word
  select distinct on (mk.ticker) mk.ticker, c.transcript_id, c.call_date
  from mk join prevcall p using (ticker)
  join kalshi.mv_word_call_counts c on c.symbol = mk.symbol and c.yes_sub_title = mk.yes_sub_title
   and c.call_date < p.prev_call_date - 2 and c.n > 0
  order by mk.ticker, c.call_date desc
), ex as (
  select o.ticker, substr(s.text, greatest(x.pos - 210, 1), 420) snip,
         row_number() over (partition by o.ticker order by s.seq) r
  from older o join mk using (ticker) join kalshi.transcript_segments s on s.transcript_id = o.transcript_id
  cross join lateral (select regexp_instr(s.text, mk.pat, 1, 1, 0, 'i') pos) x
  where x.pos > 0 and coalesce(s.role_resolved, s.role) <> 'analyst'
)
select mk.ticker, p.prev_call_date, o.call_date older_call_date,
  coalesce((select json_agg(snip) from ex where ex.ticker = mk.ticker and r <= 2), '[]'::json) older_excerpts
from mk left join prevcall p using (ticker) left join older o using (ticker)
"""

if __name__ == "__main__":
    tk = None
    if arg("--tickers-from"):   # reuse the ticker set of an existing input folder
        tk = [r["ticker"] for f in sorted(glob.glob(arg("--tickers-from") + "/in_*.json")) for r in json.load(open(f))]
    rows = build_rows(only_missing, tk, horizon, missing_table="kalshi.word_incentive")
    chunks = [[r["ticker"] for r in rows[i:i+80]] for i in range(0, len(rows), 80)]
    older = {}
    with ThreadPoolExecutor(4) as ex:
        for res in ex.map(lambda c: q(OLDER, [c]), chunks):
            for o in res: older[o["ticker"]] = o
    for r in rows:
        o = older.get(r["ticker"], {})
        r["prev_call_date"] = o.get("prev_call_date")
        r["older_call_date"] = o.get("older_call_date")
        r["older_excerpts"] = [" ".join(t.split())[40:380] for t in (o.get("older_excerpts") or [])]
        # keep the files compact
        r["prior_call_excerpts"] = [{"who": e["who"], "text": e["text"][50:470]} for e in r["prior_call_excerpts"][:2]]
        r["release_excerpts"] = [t[50:470] for t in r["release_excerpts"][:1]]
        r.pop("has_release", None)
    print(len(rows), "rows in", write_batches(rows, outdir, per_file), "files")
