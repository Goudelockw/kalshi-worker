# Context features for the mentions model

Five per-market features, all known before the call starts. Everything lands in `kalshi.v_context_features`
(one row per mentions market ticker). Tables and views are in `sql/014_context_features.sql`.

| Feature | Columns | How it's made |
|---|---|---|
| Stock trend | `ret_5d`, `ret_20d`, `ret_60d`, `excess_20d`, `excess_60d` (vs SPY), `trend_z_20d`, `stock_trend_20d` (climbing / flat / falling: 20-day return beyond ±0.5 of its 20-day volatility) | Yahoo daily closes before the call date → `kalshi.stock_prices` → `mv_market_context` |
| Beat / meet / miss | `eps_result`, `eps_surprise_pct` (the quarter reported on this call; the press release comes out before the call), `prev_eps_result`, `prev_eps_surprise_pct` (the quarter before), `beat_streak_prior` | Yahoo EPS estimate vs. reported (beat/miss = at least half a cent either side) → `kalshi.earnings_surprise` → `mv_market_context` |
| Last speaker | `ls_main_pos`, `ls_final_pos`, `ls_roles`, `ls_ceo`/`ls_cfo`/`ls_other_exec`/`ls_analyst`, `ls_n_speakers`, `ls_speakers_on_prev_call`, `ls_call_date`, `ls_calls_ago` | Speaker-tagged transcripts: the most recent prior call where anyone said the word → `mv_last_speaker` |
| Phrasing risk | `n_alternatives`, `phrasing_risk` (0–2), `alt_hits_last4`, `word_hits_last4`, `alt_share_last4` | Claude lists non-settling ways to say the same thing (`rubrics/phrasing_v1.md`) → `kalshi.word_synonyms`; prior-call usage counted in `mv_phrasing` |
| Valence | `valence` (−2..+2), `valence_mixed`, `valence_conf`, `valence_rationale` | Claude rates the topic for the company in that period from pre-call excerpts only (`rubrics/valence_v1.md`) → `kalshi.word_valence` |

Label rows are never overwritten; `reviewed = true` rows are final. Each label row stores its prompt version.

## Weekly refresh (scheduled Claude task "Weekly context features")

```
cd features && echo "$DATABASE_URL" > .dburl          # or export DATABASE_URL
pip install --break-system-packages yfinance
python market_data.py                                  # last 6 months of prices, last 8 EPS reports
python phrasing_inputs.py work/phr                     # new (company, word) pairs  -> label with rubrics/phrasing_v1.md
python load_labels.py phrasing work/phr
python valence_inputs.py work/val --only-missing --horizon-days 10   # unlabeled markets with a call in the next 10 days or past
python load_labels.py valence work/val
# then refresh mv_market_context, mv_last_speaker, mv_phrasing (concurrently)
```

`nsql.py` talks to Neon over its HTTPS SQL endpoint, so it works where port 5432 is blocked.
`python market_data.py --full` reloads everything since mid-2022.
