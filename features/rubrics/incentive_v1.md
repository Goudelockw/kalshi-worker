# Incentive rubric (prompt_version: incentive_v1)

Each row is one Kalshi "mentions" market: will someone on this company's earnings call say this word?
You rate three things about the TOPIC behind the word. You are NOT predicting whether it gets said.

## Point-in-time rule (most important)
Use only the row's excerpts and general knowledge of the company, its industry and the world up to the
dates given. Never use anything you know about what was said on the call in `call_month`, the market's
result, or later events. If you recognize the call, ignore that memory.

## Inputs
- `call_month`: when the call being predicted happens.
- `prior_call_excerpts`, `prior_call_date`: the most recent earlier call where anyone used the word.
- `release_excerpts`: this quarter's press release, published before the call.
- `prev_call_date`: the company's previous call (one quarter before).
- `older_excerpts`, `older_call_date`: the latest call BEFORE `prev_call_date` where company speakers used the word.

## Fields
1. `mgmt_incentive` (0–2): would management want to bring this topic up **unprompted** on this call?
   - 2 = yes, eagerly: a win, a growth driver, a launch, a metric they're proud of, a story they're pushing.
   - 1 = they'd cover it if it's routine or required (a standard line item, guidance component, a known cost
     they need to explain), but they aren't seeking it out.
   - 0 = no: they'd avoid it or only answer if asked (a problem, a lawsuit, a competitor's name, politics,
     a topic outside their control that makes them look bad), or it's irrelevant to them.
   Note the difference from valence: a competitor's struggles can be good for the company, but executives
   rarely name the competitor (0 or 1). A headwind they must explain to bridge guidance can be a 1.
2. `story_central` (0–2): how central is the topic to the investment story management is telling right now?
   - 2 = a core pillar (the main growth driver, the strategic priority, the new segment they keep returning to)
   - 1 = a supporting part of the story
   - 0 = peripheral or unrelated to the story
3. `valence_prev` (−2..+2): the topic's valence for the company **as of `prev_call_date`**, using the same
   scale as the valence rubric (+2 clear tailwind … −2 clear headwind, 0 neutral/unknowable). Use the
   older excerpts and knowledge up to that date only. If there's no older excerpt, judge from general
   knowledge of the period; use 0 when you genuinely can't tell.
4. `confidence`: "high" or "low" (low when the excerpts are thin and the topic's status is unclear).
5. `rationale`: one short sentence, under 140 characters.

Output per row: {"ticker", "mgmt_incentive", "story_central", "valence_prev", "confidence", "rationale"}.
