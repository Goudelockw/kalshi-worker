"""Write (company, listed word) pairs that have no phrasing label yet to JSON batches.
Usage: python phrasing_inputs.py <outdir> [--per-file N]"""
import sys, json, os, math
from nsql import q
outdir = sys.argv[1]; os.makedirs(outdir, exist_ok=True)
per_file = int(sys.argv[sys.argv.index("--per-file")+1]) if "--per-file" in sys.argv else 250
rows = q(r"""
select w.symbol, c.name company, c.sic_description sector,
       lower(btrim(regexp_replace(m.yes_sub_title, '\s*\(\d+\+ times\)', '', 'g'))) word_key, min(m.yes_sub_title) listed_as
from kalshi.markets m cross join lateral (select replace(m.series_ticker, 'KXEARNINGSMENTION', '') symbol) w
left join kalshi.company_map c on c.symbol = w.symbol
where m.series_ticker like 'KXEARNINGSMENTION%' and coalesce(m.yes_sub_title, '') <> ''
  and not exists (select 1 from kalshi.word_synonyms s where s.symbol = w.symbol
                  and s.word_key = lower(btrim(regexp_replace(m.yes_sub_title, '\s*\(\d+\+ times\)', '', 'g'))))
group by 1, 2, 3, 4 order by 1, 4""")
n = math.ceil(len(rows) / per_file) if rows else 0
for i in range(n):
    json.dump(rows[i*per_file:(i+1)*per_file], open(f"{outdir}/in_{i:02d}.json", "w"))
print(len(rows), "pairs in", n, "files")
