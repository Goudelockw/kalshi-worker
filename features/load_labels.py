"""Load Claude's label files into Neon. Each out_NN.json must line up with in_NN.json.
Usage: python load_labels.py phrasing  <dir> [--labeled-by TEXT]
       python load_labels.py valence   <dir> [--labeled-by TEXT]
       python load_labels.py incentive <dir> [--labeled-by TEXT]
Existing rows are never overwritten (ON CONFLICT DO NOTHING); reviewed rows are final."""
import sys, json, glob, hashlib
from nsql import q, bulk
kind, d = sys.argv[1], sys.argv[2]
by = sys.argv[sys.argv.index("--labeled-by")+1] if "--labeled-by" in sys.argv else "claude scheduled"
rows, skipped = [], 0
for f in sorted(glob.glob(f"{d}/out_*.json")):
    inp = {r.get("ticker") or (r["symbol"], r["word_key"]): r for r in json.load(open(f.replace("out_", "in_")))}
    for o in json.load(open(f)):
        key = o.get("ticker") if kind in ("valence", "incentive") else (o.get("symbol"), o.get("word_key"))
        i = inp.get(key)
        if i is None: skipped += 1; continue
        conf = o.get("confidence") if o.get("confidence") in ("high", "low") else "low"
        if kind == "phrasing":
            alts = [x.strip() for x in (o.get("alternatives") or []) if isinstance(x, str) and x.strip()][:12]
            rows.append({"symbol": i["symbol"], "word_key": i["word_key"], "alternatives": alts,
                         "phrasing_risk": max(0, min(2, int(o["phrasing_risk"]))), "confidence": conf, "note": o.get("note") or None})
        elif kind == "incentive":
            h = hashlib.sha1(json.dumps([i["prior_call_excerpts"], i["release_excerpts"], i.get("older_excerpts")], sort_keys=True).encode()).hexdigest()[:16]
            clamp = lambda v, lo, hi: max(lo, min(hi, int(v)))
            rows.append({"ticker": i["ticker"], "symbol": i.get("symbol") or i["ticker"].split("-")[0].replace("KXEARNINGSMENTION", ""),
                         "word": i["word"], "mgmt_incentive": clamp(o["mgmt_incentive"], 0, 2), "story_central": clamp(o["story_central"], 0, 2),
                         "valence_prev": clamp(o["valence_prev"], -2, 2), "confidence": conf, "rationale": (o.get("rationale") or "")[:200], "inputs_hash": h})
        else:
            h = hashlib.sha1(json.dumps([i["prior_call_excerpts"], i["release_excerpts"]], sort_keys=True).encode()).hexdigest()[:16]
            rows.append({"ticker": i["ticker"], "symbol": i.get("symbol") or i["ticker"].split("-")[0].replace("KXEARNINGSMENTION", ""), "word": i["word"],
                         "valence": max(-2, min(2, int(o["valence"]))), "mixed": bool(o.get("mixed")), "confidence": conf,
                         "rationale": (o.get("rationale") or "")[:200], "inputs_hash": h,
                         "n_prior": len(i["prior_call_excerpts"]), "n_release": len(i["release_excerpts"])})
if kind == "phrasing":
    bulk(f"""insert into kalshi.word_synonyms(symbol,word_key,alternatives,n_alternatives,phrasing_risk,confidence,note,prompt_version,labeled_by)
      select symbol, word_key, array(select json_array_elements_text(alternatives)), json_array_length(alternatives), phrasing_risk, confidence,
             left(note,100), 'phrasing_v1', '{by}'
      from json_to_recordset($1::json) as x(symbol text, word_key text, alternatives json, phrasing_risk int, confidence text, note text)
      on conflict (symbol, word_key) do nothing""", rows, chunk=500)
    # drop alternatives that the settlement regex would already count
    q("""update kalshi.word_synonyms w set alternatives = f.keep, n_alternatives = coalesce(array_length(f.keep, 1), 0)
      from (select symbol, word_key, array(select a from unnest(alternatives) a where not a ~* (kalshi.mention_patterns(word_key))[1]) keep
            from kalshi.word_synonyms where not reviewed) f
      where f.symbol = w.symbol and f.word_key = w.word_key and coalesce(array_length(f.keep, 1), 0) <> w.n_alternatives""")
elif kind == "incentive":
    bulk(f"""insert into kalshi.word_incentive(ticker,symbol,word,mgmt_incentive,story_central,valence_prev,confidence,rationale,inputs_hash,prompt_version,labeled_by)
      select x.*, 'incentive_v1', '{by}'
      from json_to_recordset($1::json) as x(ticker text, symbol text, word text, mgmt_incentive int, story_central int, valence_prev int,
           confidence text, rationale text, inputs_hash text)
      on conflict (ticker) do nothing""", rows, chunk=500)
else:
    bulk(f"""insert into kalshi.word_valence(ticker,event_ticker,symbol,word,valence,mixed,confidence,rationale,inputs_hash,
                                            n_prior_excerpts,n_release_excerpts,prompt_version,labeled_by)
      select x.ticker, m.event_ticker, x.symbol, x.word, x.valence, x.mixed, x.confidence, x.rationale, x.inputs_hash,
             x.n_prior, x.n_release, 'valence_v1', '{by}'
      from json_to_recordset($1::json) as x(ticker text, symbol text, word text, valence int, mixed boolean, confidence text,
           rationale text, inputs_hash text, n_prior int, n_release int)
      left join kalshi.markets m on m.ticker = x.ticker
      on conflict (ticker) do nothing""", rows, chunk=500)
print(kind, "rows loaded:", len(rows), "skipped (no matching input):", skipped)
