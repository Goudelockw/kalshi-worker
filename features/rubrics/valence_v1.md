# Valence rubric (prompt_version: valence_v1)

Each row is one Kalshi "mentions" market: will someone on this company's earnings call say this word?
You are rating the **valence of the topic for this company at the time of the call** — how good or bad
the subject behind the word is for the company and its investors in that period. You are NOT predicting
whether the word gets said.

## Point-in-time rule (most important)
Use only:
- the row's excerpts (from the most recent PRIOR call where the word came up, and from this quarter's
  press release, which was published before the call), and
- general knowledge of the company, its industry and the world **up to the call month** (`call_month`).

Do NOT use anything you know about what was said on this particular call, the stock's reaction to it, or
events after `call_month`. If you recognize the call, ignore that memory and rate from the period's context.

## Scale
- `valence`:
  - +2 = clear tailwind / good news for the company right now (strong growth driver, a win, a record)
  - +1 = mildly positive
  - 0 = neutral or purely descriptive (a routine line item, a place name, call jargon, a product that is
        neither helping nor hurting), or genuinely unknowable
  - -1 = mildly negative
  - -2 = clear headwind / bad news right now (a problem, a cost, a lawsuit, a disruption, a weak segment)
- `mixed`: true if the topic cuts both ways right now in a way investors would debate
  (e.g. heavy AI spending: growth story vs. margin worry; tariffs that hurt costs but help vs. rivals).
  A mixed topic can still have a nonzero valence if one side clearly dominates.
- `confidence`: "high" or "low". Low when excerpts are absent and the topic's status that quarter is unclear.
- `rationale`: one short sentence (under 140 characters) naming the reason.

Think from the investor's point of view: "Headwind" is negative by meaning, "Record" positive, "Tariff"
negative for importers, "Data center" positive for a chip supplier in a boom. A competitor's name is
negative if it is taking share, neutral otherwise. Macro words (inflation, recession, interest rates) take
the sign of their effect on THIS company in THAT period.
