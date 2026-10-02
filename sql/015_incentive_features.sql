-- 015: "does management want to say it?" features (2026-10-01)
--   word_tone (Loughran-McDonald + short earnings-call list, features/word_tone.py)
--   word_incentive (Claude labels, rubric incentive_v1): mgmt_incentive, story_central, valence_prev
--   mv_incentive_features: valence change vs. previous listing, new good news, press-release emphasis,
--                          word tone, peer valence this season
--   v_context_features gains all of the above.

create table if not exists kalshi.word_tone(word_key text primary key, tone smallint, n_pos int, n_neg int,
  uncertainty boolean, litigious boolean, source text, updated_at timestamptz default now());
create table if not exists kalshi.word_incentive(
  ticker text primary key, symbol text, word text,
  mgmt_incentive smallint check (mgmt_incentive between 0 and 2),
  story_central smallint check (story_central between 0 and 2),
  valence_prev smallint check (valence_prev between -2 and 2),
  confidence text check (confidence in ('high','low')), rationale text, inputs_hash text,
  prompt_version text not null, labeled_by text, labeled_at timestamptz default now(), reviewed boolean default false);

create materialized view kalshi.mv_incentive_features as
with mk as (
  select w.ticker, w.symbol, w.call_date_est, w.filing_url, w.filing_filed_at,
         lower(btrim(regexp_replace(m.yes_sub_title, '\s*\(\d+\+ times\)', '', 'g'))) word_key,
         (kalshi.mention_patterns(m.yes_sub_title))[1] pat, m.result,
         coalesce(ct.call_start_est, w.call_date_est::timestamptz + interval '8 hours') call_start,
         left(c.sic, 2) sic2
  from kalshi.mv_word_counts w join kalshi.markets m on m.ticker = w.ticker
  left join kalshi.v_call_times ct on ct.event_ticker = m.event_ticker
  left join kalshi.company_map c on c.symbol = w.symbol
  where w.call_date_est is not null and coalesce(m.yes_sub_title, '') <> ''
), v as (
  select mk.*, wv.valence from mk left join kalshi.word_valence wv on wv.ticker = mk.ticker
), prev_listing as (   -- same company and word, most recent earlier event (within ~2 quarters)
  select distinct on (a.ticker) a.ticker, b.valence prev_valence, b.call_date_est prev_listing_date
  from v a join v b on b.symbol = a.symbol and b.word_key = a.word_key
   and b.call_date_est < a.call_date_est - 20 and b.call_date_est >= a.call_date_est - 200
  where b.valence is not null
  order by a.ticker, b.call_date_est desc
), rel as (   -- this quarter's press release, only if filed before the call
  select mk.ticker, length(f.body_text) rel_len,
         regexp_instr(f.body_text, mk.pat, 1, 1, 0, 'i') first_pos,
         regexp_count(f.body_text, mk.pat, 1, 'i') rel_mentions,
         greatest(strpos(f.body_text, 'Exhibit 99'), 1) start_pos
  from mk join kalshi.filings f on f.exhibit_url = mk.filing_url and f.filed_at < mk.call_start
  where f.body_text is not null
), peer as (   -- other companies' markets on the same word whose calls came first this season
  select a.ticker,
    count(*) filter (where b.sic2 = a.sic2) peer_ind_n,
    round(avg(b.valence) filter (where b.sic2 = a.sic2), 2) peer_ind_valence,
    round(avg((b.result = 'yes')::int) filter (where b.sic2 = a.sic2 and b.result in ('yes','no')), 3) peer_ind_said,
    count(*) peer_all_n,
    round(avg(b.valence), 2) peer_all_valence,
    round(avg((b.result = 'yes')::int) filter (where b.result in ('yes','no')), 3) peer_all_said
  from v a join v b on b.word_key = a.word_key and b.symbol <> a.symbol
   and b.call_date_est < a.call_date_est - 1 and b.call_date_est >= a.call_date_est - 100
  group by a.ticker
)
select v.ticker,
  -- valence change vs. the same word's previous listing (label-based, a Claude prior-call rating fills gaps later)
  p.prev_valence, p.prev_listing_date,
  v.valence - p.prev_valence as valence_change_listing,
  -- new good news: positive topic that hasn't come up on the last 2 calls
  (v.valence >= 1 and (ls.ls_call_date is null or ls.ls_calls_ago >= 3)) as new_good_news,
  -- press-release emphasis
  (r.ticker is not null) as has_release,
  coalesce(r.rel_mentions, 0) as release_mentions,
  case when r.ticker is null then null
       when r.first_pos = 0 then 0
       when r.first_pos - r.start_pos < 2500 then 3                       -- headline / highlight bullets
       when (r.first_pos - r.start_pos)::numeric / nullif(r.rel_len - r.start_pos, 0) < 0.33 then 2
       else 1 end as release_emphasis,
  case when r.first_pos > 0 then round(((r.first_pos - r.start_pos)::numeric / nullif(r.rel_len - r.start_pos, 0)), 3) end as release_first_pos,
  -- tone of the word itself (names and places forced to 0)
  case when wc.category in ('own_product','other_company','geography','politics_geo','seasonal_event') then 0 else t.tone end as word_tone,
  t.uncertainty as word_uncertain, t.litigious as word_legal,
  -- peers this season
  pr.peer_ind_n, pr.peer_ind_valence, pr.peer_ind_said, pr.peer_all_n, pr.peer_all_valence, pr.peer_all_said,
  now() as refreshed_at
from v
left join prev_listing p on p.ticker = v.ticker
left join kalshi.mv_last_speaker ls on ls.ticker = v.ticker
left join rel r on r.ticker = v.ticker
left join kalshi.word_tone t on t.word_key = v.word_key
left join kalshi.word_categories wc on wc.symbol = v.symbol and wc.word_key = v.word_key
left join peer pr on pr.ticker = v.ticker;
create unique index on kalshi.mv_incentive_features(ticker);

create or replace view kalshi.v_context_features as
select w.ticker, w.symbol, w.call_date_est,
  -- last speaker: who said the word on the most recent prior call where it came up
  ls.ls_call_date, ls.ls_calls_ago, ls.ls_roles, ls.ls_main_pos, ls.ls_final_pos, ls.ls_n_speakers,
  ls.ls_ceo, ls.ls_cfo, ls.ls_other_exec, ls.ls_analyst, ls.ls_speakers_on_prev_call,
  -- phrasing risk
  ph.n_alternatives, ph.phrasing_risk, ph.alt_hits_last4, ph.word_hits_last4, ph.alt_share_last4,
  -- valence
  v.valence, v.mixed as valence_mixed, v.confidence as valence_conf, v.rationale as valence_rationale,
  -- stock trend and earnings surprise
  mc.ret_5d, mc.ret_20d, mc.ret_60d, mc.excess_20d, mc.excess_60d, mc.trend_z_20d, mc.stock_trend_20d,
  mc.eps_surprise_pct, mc.eps_result, mc.prev_eps_surprise_pct, mc.prev_eps_result, mc.beat_streak_prior,
  -- management incentive (sql/015)
  i.mgmt_incentive, i.story_central, i.valence_prev, i.confidence as incentive_conf, i.rationale as incentive_rationale,
  v.valence - i.valence_prev as valence_change,
  inc.prev_valence as valence_prev_listing, inc.valence_change_listing,
  inc.new_good_news, inc.has_release, inc.release_mentions, inc.release_emphasis, inc.release_first_pos,
  inc.word_tone, inc.word_uncertain, inc.word_legal,
  inc.peer_ind_n, inc.peer_ind_valence, inc.peer_ind_said, inc.peer_all_n, inc.peer_all_valence, inc.peer_all_said
from kalshi.mv_word_counts w
left join kalshi.mv_last_speaker ls on ls.ticker = w.ticker
left join kalshi.mv_phrasing ph on ph.ticker = w.ticker
left join kalshi.word_valence v on v.ticker = w.ticker
left join kalshi.mv_market_context mc on mc.ticker = w.ticker
left join kalshi.word_incentive i on i.ticker = w.ticker
left join kalshi.mv_incentive_features inc on inc.ticker = w.ticker;
