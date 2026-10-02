"""Tone of each listed word from the Loughran-McDonald finance word lists (via pysentiment2), plus a short
earnings-call supplement for terms LM doesn't cover. Writes kalshi.word_tone. Usage: python word_tone.py"""
import csv, os, re
import pysentiment2
from nsql import q, bulk
LM = {}
for r in csv.DictReader(open(os.path.join(os.path.dirname(pysentiment2.__file__), "static", "LM.csv"))):
    f = {k: r[k] != "0" for k in ("Negative", "Positive", "Uncertainty", "Litigious", "Constraining")}
    if any(f.values()): LM[r["Word"].lower()] = f
# Earnings-call language LM (built from 10-Ks) misses. Kept short and explicit.
CALL_POS = {"tailwind", "tailwinds", "momentum", "record", "records", "accelerate", "accelerating", "acceleration",
            "outperform", "outperformance", "beat", "upside", "robust", "resilient", "resilience", "milestone", "breakthrough"}
CALL_NEG = {"headwind", "headwinds", "softness", "soft", "slowdown", "deceleration", "decelerate", "pressure", "pressures",
            "inflation", "recession", "tariff", "tariffs", "layoff", "layoffs", "shortage", "shortages", "downturn", "volatility"}
words = [r["word_key"] for r in q("select distinct word_key from kalshi.word_synonyms")]
rows = []
for w in words:
    toks = set(re.findall(r"[a-z]+", w.lower()))
    pos = sum(1 for t in toks if (LM.get(t, {}).get("Positive")) or t in CALL_POS)
    neg = sum(1 for t in toks if (LM.get(t, {}).get("Negative")) or t in CALL_NEG)
    unc = any(LM.get(t, {}).get("Uncertainty") for t in toks)
    lit = any(LM.get(t, {}).get("Litigious") for t in toks)
    src = "lm+call" if toks & (CALL_POS | CALL_NEG) else ("lm" if any(t in LM for t in toks) else "none")
    rows.append({"word_key": w, "tone": (pos > 0) - (neg > 0), "n_pos": pos, "n_neg": neg, "uncertainty": unc, "litigious": lit, "source": src})
q("""create table if not exists kalshi.word_tone(word_key text primary key, tone smallint, n_pos int, n_neg int,
     uncertainty boolean, litigious boolean, source text, updated_at timestamptz default now())""")
bulk("""insert into kalshi.word_tone(word_key,tone,n_pos,n_neg,uncertainty,litigious,source)
  select * from json_to_recordset($1::json) as x(word_key text, tone int, n_pos int, n_neg int, uncertainty boolean, litigious boolean, source text)
  on conflict (word_key) do update set tone=excluded.tone, n_pos=excluded.n_pos, n_neg=excluded.n_neg,
    uncertainty=excluded.uncertainty, litigious=excluded.litigious, source=excluded.source, updated_at=now()""", rows)
from collections import Counter
print(len(rows), "words; tone:", Counter(r["tone"] for r in rows), "uncertainty:", sum(r["uncertainty"] for r in rows), "litigious:", sum(r["litigious"] for r in rows))
print("neg:", sorted(r["word_key"] for r in rows if r["tone"] < 0)[:40])
print("pos:", sorted(r["word_key"] for r in rows if r["tone"] > 0)[:40])
