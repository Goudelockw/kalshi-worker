"""Build point-in-time inputs for valence labeling: excerpts from the most recent prior call that used the word,
and from this quarter's press release (only if filed before the call). Writes JSON batches.
Usage: python valence_inputs.py <outdir> [--only-missing] [--horizon-days N] [--sample N] [--per-file N]
  --horizon-days N: only markets whose call is on or before today + N (label close to the call)."""
import sys, json, os, math, random
from nsql import q  # noqa
outdir = sys.argv[1]; os.makedirs(outdir, exist_ok=True)
only_missing = "--only-missing" in sys.argv
lim = int(sys.argv[sys.argv.index("--limit")+1]) if "--limit" in sys.argv else None
sample = int(sys.argv[sys.argv.index("--sample")+1]) if "--sample" in sys.argv else None
horizon = int(sys.argv[sys.argv.index("--horizon-days")+1]) if "--horizon-days" in sys.argv else None
per_file = int(sys.argv[sys.argv.index("--per-file")+1]) if "--per-file" in sys.argv else 250

SQL = r"""
with mk as (
  select w.ticker, w.symbol, w.call_date_est, m.yes_sub_title word, m.event_ticker,
         (kalshi.mention_patterns(m.yes_sub_title))[1] pat, w.filing_url, w.filing_filed_at,
         coalesce(ct.call_start_est, w.call_date_est::timestamptz + interval '8 hours') call_start,
         ls.ls_call_date
  from kalshi.mv_word_counts w join kalshi.markets m on m.ticker = w.ticker
  left join kalshi.v_call_times ct on ct.event_ticker = m.event_ticker
  left join kalshi.mv_last_speaker ls on ls.ticker = w.ticker
  where coalesce(m.yes_sub_title,'') <> '' and w.call_date_est is not null
    and (not $1 or not exists (select 1 from kalshi.word_valence v where v.ticker = w.ticker))
    and ($2::text[] is null or w.ticker = any($2::text[]))
    and ($3::int is null or w.call_date_est <= current_date + $3::int)
), own as (
  select s.symbol, array(select s.symbol union select c.ticker from kalshi.company_map c where c.symbol = s.symbol and c.ticker is not null) syms
  from (select distinct symbol from mk) s
), tr as (   -- the prior call where the word was last said
  select distinct on (mk.ticker) mk.ticker, t.id
  from mk join own o using (symbol) join kalshi.transcripts t on t.symbol = any(o.syms) and t.call_date = mk.ls_call_date
  order by mk.ticker, case t.source when 'ir' then 0 when 'fool' then 1 when 'fortune' then 2 else 3 end, t.id
), cx as (
  select tr.ticker, x.ord,
    case when coalesce(s.role_resolved, s.role) = 'analyst' then 'analyst' else 'company' end || ' (' || s.section || ')' who,
    x.m[1] snip
  from tr join mk using (ticker) join kalshi.transcript_segments s on s.transcript_id = tr.id
  cross join lateral (select n ord, regexp_instr(s.text, mk.pat, 1, n, 0, 'i') pos from generate_series(1,2) n) p
  cross join lateral (select array[substr(s.text, greatest(p.pos - 250, 1), 520)] m, p.ord) x
  where s.text ~* mk.pat and p.pos > 0
), cxr as (
  select ticker, json_agg(json_build_object('who', who, 'text', snip)) ex
  from (select *, row_number() over (partition by ticker order by length(snip) desc) r from cx) z where r <= 3 group by ticker
), fx as (
  select mk.ticker, json_agg(z.snip) ex
  from mk join kalshi.filings f on f.exhibit_url = mk.filing_url and f.filed_at < mk.call_start
  cross join lateral (select substr(f.body_text, greatest(p - 250, 1), 520) snip
     from (select regexp_instr(coalesce(f.body_text,''), mk.pat, 1, n, 0, 'i') p from generate_series(1,2) n) y where p > 0) z
  group by mk.ticker
)
select mk.ticker, mk.symbol, c.name company, c.sic_description sector, mk.word, to_char(mk.call_date_est, 'YYYY-MM') call_month,
  mk.ls_call_date prior_call_date, coalesce(cxr.ex, '[]'::json) prior_call_excerpts,
  (mk.filing_filed_at < mk.call_start) has_release, coalesce(fx.ex, '[]'::json) release_excerpts
from mk left join kalshi.company_map c on c.symbol = mk.symbol
left join cxr on cxr.ticker = mk.ticker left join fx on fx.ticker = mk.ticker
order by mk.symbol, mk.call_date_est, mk.ticker
"""
tick = None
if sample:
    allt = [r["ticker"] for r in q("select ticker from kalshi.mv_word_counts where call_date_est is not null order by 1")]
    random.seed(7); tick = random.sample(allt, sample)
rows = []
if tick is None:   # chunk by symbol to keep each query small
    from concurrent.futures import ThreadPoolExecutor
    allt = q("select symbol, ticker from kalshi.mv_word_counts where call_date_est is not null order by 1, 2")
    chunks, cur = [], []
    for r in allt:   # ~80 markets per query keeps each one well under the HTTP timeout
        cur.append(r["ticker"])
        if len(cur) >= 80: chunks.append(cur); cur = []
    if cur: chunks.append(cur)
    def run(tk): return q(SQL, [only_missing, tk, horizon])
    with ThreadPoolExecutor(4) as ex:
        for i, res in enumerate(ex.map(run, chunks)):
            rows += res
            if i % 10 == 0: print(f"chunk {i+1}/{len(chunks)}: {len(rows)} rows", flush=True)
else:
    rows = q(SQL, [only_missing, tick, horizon])
if lim: rows = rows[:lim]
def dedupe(items, key=lambda x: x):
    out = []
    for it in items:
        t = key(it)
        if not any(t[150:300] in key(o) or key(o)[150:300] in t for o in out): out.append(it)
    return out
for r in rows:   # trim long excerpts, drop overlapping ones
    r["prior_call_excerpts"] = dedupe([{"who": e["who"], "text": " ".join(e["text"].split())[:600]} for e in (r["prior_call_excerpts"] or [])], key=lambda e: e["text"])
    r["release_excerpts"] = dedupe([" ".join(t.split())[:600] for t in (r["release_excerpts"] or [])])
n = math.ceil(len(rows) / per_file) if rows else 0
for i in range(n):
    json.dump(rows[i*per_file:(i+1)*per_file], open(f"{outdir}/in_{i:02d}.json", "w"))
print(len(rows), "rows in", n, "files")
