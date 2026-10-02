-- 014: context features for the mentions model (2026-10-01)
--   Stock trend + earnings surprise (Yahoo):  kalshi.stock_prices, kalshi.earnings_surprise, kalshi.yahoo_map -> mv_market_context
--   Last speaker (transcripts):               mv_last_speaker
--   Phrasing risk (Claude labels):            kalshi.word_synonyms -> mv_phrasing
--   Valence (Claude labels):                  kalshi.word_valence
--   Everything per market ticker:             kalshi.v_context_features
-- Filled weekly by the "Weekly context features" scheduled task (see features/README.md).

create table if not exists kalshi.stock_prices(
  yahoo_ticker text not null, d date not null, close numeric, adj_close numeric, volume bigint,
  fetched_at timestamptz default now(), primary key (yahoo_ticker, d));
create table if not exists kalshi.earnings_surprise(
  symbol text not null, yahoo_ticker text not null, report_ts timestamptz not null, report_date date not null,
  eps_est numeric, eps_actual numeric, surprise_pct numeric, source text default 'yahoo',
  fetched_at timestamptz default now(), primary key (symbol, report_date));
create table if not exists kalshi.yahoo_map(symbol text primary key, yahoo_ticker text not null, note text);
create table if not exists kalshi.word_synonyms(
  symbol text not null, word_key text not null, alternatives text[] not null default '{}',
  n_alternatives int not null, phrasing_risk smallint not null check (phrasing_risk between 0 and 2),
  confidence text check (confidence in ('high','low')), note text,
  prompt_version text not null, labeled_by text, labeled_at timestamptz default now(),
  reviewed boolean default false, primary key (symbol, word_key));
create table if not exists kalshi.word_valence(
  ticker text primary key, event_ticker text, symbol text, word text,
  valence smallint check (valence between -2 and 2), mixed boolean, confidence text check (confidence in ('high','low')),
  rationale text, inputs_hash text, n_prior_excerpts int, n_release_excerpts int,
  prompt_version text not null, labeled_by text, labeled_at timestamptz default now(), reviewed boolean default false);

create materialized view kalshi.mv_market_context as
with ev as (
  select distinct w.symbol, w.call_date_est, y.yahoo_ticker
  from kalshi.mv_word_counts w join kalshi.yahoo_map y using (symbol)
  where w.call_date_est is not null
), px as (   -- price history up to the day before the call
  select ev.symbol, ev.call_date_est, p.d, p.adj_close,
         row_number() over (partition by ev.symbol, ev.call_date_est order by p.d desc) k
  from ev join kalshi.stock_prices p on p.yahoo_ticker = ev.yahoo_ticker
   and p.d < ev.call_date_est and p.d >= ev.call_date_est - 130
), spy as (
  select ev.symbol, ev.call_date_est, p.d, p.adj_close,
         row_number() over (partition by ev.symbol, ev.call_date_est order by p.d desc) k
  from ev join kalshi.stock_prices p on p.yahoo_ticker = 'SPY'
   and p.d < ev.call_date_est and p.d >= ev.call_date_est - 130
), r as (
  select px.symbol, px.call_date_est,
    max(px.d) filter (where px.k = 1) as px_asof,
    max(px.adj_close) filter (where px.k = 1) / nullif(max(px.adj_close) filter (where px.k = 6), 0) - 1 as ret_5d,
    max(px.adj_close) filter (where px.k = 1) / nullif(max(px.adj_close) filter (where px.k = 21), 0) - 1 as ret_20d,
    max(px.adj_close) filter (where px.k = 1) / nullif(max(px.adj_close) filter (where px.k = 61), 0) - 1 as ret_60d
  from px group by 1,2
), vol as (  -- daily return volatility over the 60 days before the call
  select symbol, call_date_est, stddev_samp(lr) as vol_d
  from (select symbol, call_date_est, k, ln(adj_close / lead(adj_close) over (partition by symbol, call_date_est order by k)) lr
        from px where k <= 61) z
  group by 1,2
), s as (
  select symbol, call_date_est,
    max(adj_close) filter (where k = 1) / nullif(max(adj_close) filter (where k = 21), 0) - 1 as spy_20d,
    max(adj_close) filter (where k = 1) / nullif(max(adj_close) filter (where k = 61), 0) - 1 as spy_60d
  from spy group by 1,2
), es as (
  select e.*,
    case when eps_est is null then null
         when eps_actual >= eps_est + 0.005 then 'beat'
         when eps_actual <= eps_est - 0.005 then 'miss'
         else 'meet' end as result
  from kalshi.earnings_surprise e
), this_q as (   -- the quarter reported on this call (press release comes out before the call)
  select distinct on (ev.symbol, ev.call_date_est) ev.symbol, ev.call_date_est, es.report_date, es.eps_est, es.eps_actual, es.surprise_pct, es.result
  from ev join es on es.symbol = ev.symbol and es.report_date between ev.call_date_est - 3 and ev.call_date_est + 1
  order by ev.symbol, ev.call_date_est, abs(es.report_date - ev.call_date_est)
), prev_q as (   -- the quarter before that
  select distinct on (ev.symbol, ev.call_date_est) ev.symbol, ev.call_date_est, es.report_date, es.surprise_pct, es.result
  from ev join es on es.symbol = ev.symbol and es.report_date < ev.call_date_est - 20
  order by ev.symbol, ev.call_date_est, es.report_date desc
), streak as (   -- consecutive beats going into this call (excludes this call)
  select ev.symbol, ev.call_date_est,
    coalesce(min(z.n) filter (where z.result <> 'beat') - 1, count(z.n))::int as beat_streak_prior
  from ev left join lateral (
    select es.result, row_number() over (order by es.report_date desc) n
    from es where es.symbol = ev.symbol and es.report_date < ev.call_date_est - 20 and es.result is not null
    order by es.report_date desc limit 8) z on true
  group by 1,2
), evf as (
  select ev.symbol, ev.call_date_est, r.px_asof,
    round(r.ret_5d::numeric, 4) ret_5d, round(r.ret_20d::numeric, 4) ret_20d, round(r.ret_60d::numeric, 4) ret_60d,
    round((r.ret_20d - s.spy_20d)::numeric, 4) excess_20d, round((r.ret_60d - s.spy_60d)::numeric, 4) excess_60d,
    round((r.ret_20d / nullif(vol.vol_d * sqrt(20), 0))::numeric, 3) trend_z_20d,
    case when r.ret_20d is null or vol.vol_d is null then null
         when r.ret_20d / nullif(vol.vol_d * sqrt(20), 0) >  0.5 then 'climbing'
         when r.ret_20d / nullif(vol.vol_d * sqrt(20), 0) < -0.5 then 'falling'
         else 'flat' end as stock_trend_20d,
    t.report_date eps_report_date, t.eps_est, t.eps_actual, round(t.surprise_pct, 2) eps_surprise_pct, t.result eps_result,
    p.report_date prev_eps_report_date, round(p.surprise_pct, 2) prev_eps_surprise_pct, p.result prev_eps_result,
    st.beat_streak_prior
  from ev
  left join r using (symbol, call_date_est) left join s using (symbol, call_date_est) left join vol using (symbol, call_date_est)
  left join this_q t using (symbol, call_date_est) left join prev_q p using (symbol, call_date_est)
  left join streak st using (symbol, call_date_est)
)
select w.ticker, evf.*, now() as refreshed_at
from kalshi.mv_word_counts w left join evf using (symbol, call_date_est);
create unique index on kalshi.mv_market_context(ticker);

create materialized view kalshi.mv_last_speaker as
with mk as (
  select w.ticker, w.symbol, m.yes_sub_title, w.call_date_est
  from kalshi.mv_word_counts w join kalshi.markets m using (ticker)
  where coalesce(m.yes_sub_title,'') <> ''
), own as (
  select s.symbol, array(select s.symbol union select c.ticker from kalshi.company_map c where c.symbol = s.symbol and c.ticker is not null) syms
  from (select distinct symbol from mk) s
), alts as (
  select distinct mk.symbol, mk.yes_sub_title, p.p as pat
  from mk cross join lateral unnest(kalshi.mention_patterns(mk.yes_sub_title)) p(p)
), tprio as (
  select o.symbol, t.id, t.call_date
  from own o join kalshi.transcripts t on t.symbol = any(o.syms)
  where not exists (select 1 from kalshi.transcripts t2 where t2.symbol = t.symbol and t2.id <> t.id and abs(t2.call_date - t.call_date) <= 3
    and (case t2.source when 'ir' then 0 when 'fool' then 1 when 'fortune' then 2 else 3 end, t2.id)
      < (case t.source when 'ir' then 0 when 'fool' then 1 when 'fortune' then 2 else 3 end, t.id))
), seg as (
  select t.symbol, t.id, t.call_date, s.seq, s.speaker, s.text,
    case
      when coalesce(s.role_resolved, s.role) = 'analyst' then 'analyst'
      when coalesce(s.role_resolved, s.role) = 'operator' or s.speaker ~* 'operator' then 'operator'
      when s.speaker_title ~* '(chief executive|\mceo\M)' and s.speaker_title !~* '(former|deputy|vice|division|segment|region|ceo of [a-z])' then 'ceo'
      when s.speaker_title ~* '(chief financial|\mcfo\M)' and s.speaker_title !~* '(former|deputy|vice|division|segment|region)' then 'cfo'
      when coalesce(s.role_resolved, s.role) = 'exec' then 'other_exec'
      else 'unknown'
    end as pos
  from tprio t join kalshi.transcript_segments s on s.transcript_id = t.id
), hits as (
  select a.symbol, a.yes_sub_title, seg.id, seg.call_date, seg.seq, seg.speaker, seg.pos,
         sum(regexp_count(seg.text, a.pat, 1, 'i'))::int as n
  from alts a join seg on seg.symbol = a.symbol
  where seg.text ~* a.pat
  group by 1,2,3,4,5,6,7
), calls as (   -- prior calls for each market, numbered newest first
  select mk.ticker, t.id, t.call_date, row_number() over (partition by mk.ticker order by t.call_date desc) k
  from mk join (select distinct symbol, id, call_date from tprio) t on t.symbol = mk.symbol
  where t.call_date < mk.call_date_est - 2
), lastcall as (  -- most recent prior call where the word was said by anyone
  select distinct on (c.ticker) c.ticker, c.id, c.call_date, c.k
  from calls c join mk using (ticker)
  where exists (select 1 from hits h where h.id = c.id and h.symbol = mk.symbol and h.yes_sub_title = mk.yes_sub_title)
  order by c.ticker, c.k
), lh as (
  select l.ticker, h.seq, h.speaker, h.pos, h.n
  from lastcall l join mk using (ticker)
  join hits h on h.id = l.id and h.symbol = mk.symbol and h.yes_sub_title = mk.yes_sub_title
  where h.pos <> 'operator'
), prev_speakers as (  -- who spoke at all on the most recent prior call
  select c.ticker, array_agg(distinct s.speaker) sp
  from calls c join kalshi.transcript_segments s on s.transcript_id = c.id
  where c.k = 1 group by c.ticker
)
select mk.ticker,
  l.call_date as ls_call_date,
  l.k as ls_calls_ago,
  (select string_agg(distinct pos, ',' order by pos) from lh where lh.ticker = mk.ticker) as ls_roles,
  (select count(distinct speaker) from lh where lh.ticker = mk.ticker)::int as ls_n_speakers,
  (select array_agg(distinct speaker) from lh where lh.ticker = mk.ticker) as ls_speakers,
  (select pos from lh where lh.ticker = mk.ticker order by seq desc limit 1) as ls_final_pos,
  (select pos from lh where lh.ticker = mk.ticker group by pos order by sum(n) desc, pos limit 1) as ls_main_pos,
  coalesce((select bool_or(pos='ceo') from lh where lh.ticker = mk.ticker), false) as ls_ceo,
  coalesce((select bool_or(pos='cfo') from lh where lh.ticker = mk.ticker), false) as ls_cfo,
  coalesce((select bool_or(pos='other_exec') from lh where lh.ticker = mk.ticker), false) as ls_other_exec,
  coalesce((select bool_or(pos='analyst') from lh where lh.ticker = mk.ticker), false) as ls_analyst,
  (select count(distinct lh.speaker) from lh, prev_speakers p
     where lh.ticker = mk.ticker and p.ticker = mk.ticker and lh.speaker = any(p.sp))::int as ls_speakers_on_prev_call,
  now() as refreshed_at
from mk left join lastcall l using (ticker);
create unique index on kalshi.mv_last_speaker(ticker);

create materialized view kalshi.mv_phrasing as
with mk as (
  select w.ticker, w.symbol, w.call_date_est,
         lower(btrim(regexp_replace(m.yes_sub_title, '\s*\(\d+\+ times\)', '', 'g'))) word_key, m.yes_sub_title
  from kalshi.mv_word_counts w join kalshi.markets m using (ticker)
), syn as (
  select s.symbol, s.word_key, s.n_alternatives, s.phrasing_risk,
         case when s.n_alternatives > 0 then (kalshi.mention_patterns(array_to_string(s.alternatives, '/')))[1] end alt_pat
  from kalshi.word_synonyms s
), own as (
  select s.symbol, array(select s.symbol union select c.ticker from kalshi.company_map c where c.symbol = s.symbol and c.ticker is not null) syms
  from (select distinct symbol from mk) s
), tprio as (
  select o.symbol, t.id, t.call_date
  from own o join kalshi.transcripts t on t.symbol = any(o.syms)
  where not exists (select 1 from kalshi.transcripts t2 where t2.symbol = t.symbol and t2.id <> t.id and abs(t2.call_date - t.call_date) <= 3
    and (case t2.source when 'ir' then 0 when 'fool' then 1 when 'fortune' then 2 else 3 end, t2.id)
      < (case t.source when 'ir' then 0 when 'fool' then 1 when 'fortune' then 2 else 3 end, t.id))
), tx as (   -- company speakers only, same as the word counts
  select t.symbol, t.id, t.call_date,
         string_agg(s.text, E'\n' order by s.seq) filter (where coalesce(s.role_resolved, s.role) <> 'analyst') txt
  from tprio t join kalshi.transcript_segments s on s.transcript_id = t.id group by 1,2,3
), pc as (
  select mk.ticker, tx.txt, row_number() over (partition by mk.ticker order by tx.call_date desc) k
  from mk join tx on tx.symbol = mk.symbol and tx.call_date < mk.call_date_est - 2
), cnt as (
  select pc.ticker,
    sum(regexp_count(coalesce(pc.txt,''), syn.alt_pat, 1, 'i'))::int alt_hits_last4,
    sum(regexp_count(coalesce(pc.txt,''), (kalshi.mention_patterns(mk.yes_sub_title))[1], 1, 'i'))::int word_hits_last4,
    count(*)::int calls_last4
  from pc join mk using (ticker) join syn on syn.symbol = mk.symbol and syn.word_key = mk.word_key
  where pc.k <= 4 and syn.alt_pat is not null
  group by pc.ticker
)
select mk.ticker, syn.n_alternatives, syn.phrasing_risk,
  coalesce(cnt.alt_hits_last4, 0) alt_hits_last4, coalesce(cnt.word_hits_last4, 0) word_hits_last4, cnt.calls_last4,
  case when coalesce(cnt.alt_hits_last4,0) + coalesce(cnt.word_hits_last4,0) > 0
       then round(cnt.alt_hits_last4::numeric / (cnt.alt_hits_last4 + cnt.word_hits_last4), 3) end alt_share_last4,
  now() refreshed_at
from mk left join syn on syn.symbol = mk.symbol and syn.word_key = mk.word_key
left join cnt using (ticker);
create unique index on kalshi.mv_phrasing(ticker);

-- One row per mentions market. All values are known before the call starts
-- (stock trend uses closes before the call date; this quarter's EPS comes from the press release, which precedes the call).
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
  mc.eps_surprise_pct, mc.eps_result, mc.prev_eps_surprise_pct, mc.prev_eps_result, mc.beat_streak_prior
from kalshi.mv_word_counts w
left join kalshi.mv_last_speaker ls on ls.ticker = w.ticker
left join kalshi.mv_phrasing ph on ph.ticker = w.ticker
left join kalshi.word_valence v on v.ticker = w.ticker
left join kalshi.mv_market_context mc on mc.ticker = w.ticker;
