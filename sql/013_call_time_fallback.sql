-- Call-start fallback for earnings-mention events with neither a call time in the release text nor
-- a first 1-minute lock after the release. The old fallback (hourly_lock_peak_minus_90m) guessed the
-- start from the busiest hourly-lock candle and was wrong for about 10 of 27 events, sometimes by
-- days. New order for those events:
--   1. kalshi.call_time_overrides                                           -> manual_override
--   2. kalshi.event_date(sub_title, event_ticker) + the company's usual call time of day (median,
--      America/New_York, of its earlier calls whose start came from release_text or
--      first_lock_minus_20m; company = company_map.ticker, else the Kalshi symbol)
--                                                                           -> event_date_plus_company_time
--   3. that event date + 08:30 ET when the release came before 09:30 ET on that date (before the
--      market open), else 17:00 ET                                          -> event_date_plus_default_time
--   4. the old hourly-peak guess                                            -> hourly_lock_peak_minus_90m_lowconf
-- A 2 or 3 start is only used when it falls before the event's first market close and within 3 days
-- of it (the markets close when the call ends); otherwise the next rule applies. Without this guard
-- a morning caller with no release (close ~10:00 ET) got 17:00 ET, after its own close, and a
-- placeholder ticker date (-26JUN30) could be months from the real call.
-- v_call_times keeps its column list: an override still wins for every event, otherwise it passes
-- the base through (its release_at + median-gap step is replaced by 2-3 above).

CREATE OR REPLACE VIEW kalshi.v_call_times_base AS
WITH ev AS (
    SELECT m.event_ticker,
           replace(min(m.series_ticker), 'KXEARNINGSMENTION', '') AS symbol,
           min(m.close_time) AS closed_at
    FROM kalshi.markets m
    WHERE m.series_ticker LIKE 'KXEARNINGSMENTION%' AND m.result = ANY (ARRAY['yes', 'no'])
    GROUP BY m.event_ticker
), rel AS (
    SELECT v.event_ticker, v.filed_at AS release_at, f.raw_text
    FROM kalshi.v_release_timing v
    JOIN kalshi.filings f USING (accession)
), sched AS (
    SELECT rel.event_ticker, rel.release_at,
           regexp_match(rel.raw_text, '(?:conference call|webcast|call)[^.]{0,200}?\m(\d{1,2})(?::(\d{2}))?\s*(a\.?m\.?|p\.?m\.?)\s*\(?\s*(ET|EDT|EST|Eastern|PT|PDT|PST|Pacific|CT|CDT|CST|Central|MT|MDT|Mountain)', 'i') AS m
    FROM rel
), sched2 AS (
    SELECT sched.event_ticker, sched.release_at,
           CASE WHEN sched.m IS NULL THEN NULL::timestamptz
                ELSE ((sched.release_at AT TIME ZONE
                         CASE WHEN sched.m[4] ~* '^(P)' THEN 'America/Los_Angeles'
                              WHEN sched.m[4] ~* '^(C)' THEN 'America/Chicago'
                              WHEN sched.m[4] ~* '^M' THEN 'America/Denver'
                              ELSE 'America/New_York' END)::date
                      + make_time(sched.m[1]::integer % 12 + CASE WHEN sched.m[3] ~* '^p' THEN 12 ELSE 0 END,
                                  COALESCE(sched.m[2], '0')::integer, 0::double precision))
                     AT TIME ZONE
                         CASE WHEN sched.m[4] ~* '^(P)' THEN 'America/Los_Angeles'
                              WHEN sched.m[4] ~* '^(C)' THEN 'America/Chicago'
                              WHEN sched.m[4] ~* '^M' THEN 'America/Denver'
                              ELSE 'America/New_York' END
           END AS scheduled_start
    FROM sched
), jumps AS (
    SELECT ev_1.event_ticker, c.ts, m.ticker
    FROM ev ev_1
    JOIN kalshi.markets m ON m.event_ticker = ev_1.event_ticker AND m.result = 'yes'
    JOIN kalshi.candles c ON c.ticker = m.ticker AND c.period_minutes = 60
                         AND c.ts <= ev_1.closed_at + interval '1 hour' AND c.ts >= ev_1.closed_at - interval '4 days'
    WHERE c.yes_bid_close >= 0.95
      AND COALESCE((SELECT q.yes_bid FROM kalshi.quote_at(m.ticker, c.ts - interval '2 hours', interval '6 hours')
                           q(ts, yes_bid, yes_ask, price_close, period_minutes)), 0::numeric) < 0.85
), peak AS (
    SELECT DISTINCT ON (x.event_ticker) x.event_ticker, x.ts AS peak_hour
    FROM (SELECT jumps.event_ticker, jumps.ts, count(DISTINCT jumps.ticker) AS n
          FROM jumps GROUP BY jumps.event_ticker, jumps.ts) x
    ORDER BY x.event_ticker, x.n DESC, x.ts
), lock AS (
    SELECT ev_1.event_ticker, min(c.ts) AS first_lock
    FROM ev ev_1
    JOIN kalshi.markets m ON m.event_ticker = ev_1.event_ticker AND m.result = 'yes'
    JOIN rel ON rel.event_ticker = ev_1.event_ticker
    JOIN kalshi.candles c ON c.ticker = m.ticker AND c.period_minutes = 1 AND c.ts > rel.release_at AND c.ts <= ev_1.closed_at
    WHERE c.yes_bid_close >= 0.95
      AND COALESCE((SELECT q.yes_bid FROM kalshi.quote_at(m.ticker, rel.release_at, interval '6 hours')
                           q(ts, yes_bid, yes_ask, price_close, period_minutes)), 0::numeric) < 0.85
    GROUP BY ev_1.event_ticker
), core AS (
    SELECT ev.event_ticker, ev.symbol, ev.closed_at, s.release_at, s.scheduled_start, l.first_lock, pk.peak_hour,
           CASE WHEN s.scheduled_start IS NOT NULL
                 AND s.scheduled_start < COALESCE(l.first_lock, ev.closed_at) + interval '5 minutes'
                 AND s.scheduled_start > s.release_at - interval '1 hour'
                THEN s.scheduled_start END AS text_start
    FROM ev
    LEFT JOIN sched2 s USING (event_ticker)
    LEFT JOIN lock l USING (event_ticker)
    LEFT JOIN peak pk USING (event_ticker)
), known AS (          -- calls with a release-text or first-lock start: the company's usual time of day
    SELECT COALESCE(cm.ticker, c.symbol) AS co,
           COALESCE(c.text_start, c.first_lock - interval '20 minutes') AS call_start
    FROM core c
    LEFT JOIN kalshi.company_map cm ON cm.symbol = c.symbol
    WHERE c.text_start IS NOT NULL OR c.first_lock IS NOT NULL
), fb0 AS (            -- fallback inputs for events with neither
    SELECT c.event_ticker, c.closed_at, c.release_at,
           o.call_start AS override_start,
           kalshi.event_date(e.sub_title, c.event_ticker) AS ev_date,
           (SELECT percentile_cont(0.5) WITHIN GROUP (
                       ORDER BY extract(epoch FROM (k.call_start AT TIME ZONE 'America/New_York')::time))
              FROM known k
             WHERE k.co = COALESCE(cm.ticker, c.symbol)
               AND (k.call_start AT TIME ZONE 'America/New_York')::date
                   < kalshi.event_date(e.sub_title, c.event_ticker)) AS usual_secs
    FROM core c
    LEFT JOIN kalshi.events e ON e.event_ticker = c.event_ticker
    LEFT JOIN kalshi.company_map cm ON cm.symbol = c.symbol
    LEFT JOIN kalshi.call_time_overrides o ON o.event_ticker = c.event_ticker
    WHERE c.text_start IS NULL AND c.first_lock IS NULL
), fb1 AS (
    SELECT fb0.*,
           (fb0.ev_date + make_interval(secs => fb0.usual_secs)) AT TIME ZONE 'America/New_York' AS company_start,
           (fb0.ev_date + CASE WHEN fb0.release_at IS NOT NULL
                                AND fb0.release_at < (fb0.ev_date + time '09:30') AT TIME ZONE 'America/New_York'
                               THEN time '08:30' ELSE time '17:00' END) AT TIME ZONE 'America/New_York' AS default_start
    FROM fb0
), fb AS (             -- a candidate counts only before the first market close and within 3 days of it
    SELECT fb1.event_ticker, fb1.override_start,
           CASE WHEN fb1.company_start < fb1.closed_at AND fb1.company_start > fb1.closed_at - interval '3 days'
                THEN fb1.company_start END AS company_start,
           CASE WHEN fb1.default_start < fb1.closed_at AND fb1.default_start > fb1.closed_at - interval '3 days'
                THEN fb1.default_start END AS default_start
    FROM fb1
)
SELECT c.event_ticker,
       c.symbol,
       c.closed_at,
       c.release_at,
       c.scheduled_start,
       c.first_lock,
       CASE WHEN c.scheduled_start IS NOT NULL
             AND c.scheduled_start >= c.release_at - interval '1 hour'
             AND c.scheduled_start < COALESCE(c.first_lock, c.closed_at) + interval '5 minutes'
             AND c.scheduled_start > c.release_at - interval '6 hours'
            THEN c.scheduled_start END AS scheduled_start_ok,
       CASE WHEN c.text_start IS NOT NULL THEN c.text_start
            WHEN c.first_lock IS NOT NULL THEN c.first_lock - interval '20 minutes'
            WHEN fb.override_start IS NOT NULL THEN fb.override_start
            WHEN fb.company_start IS NOT NULL THEN fb.company_start
            WHEN fb.default_start IS NOT NULL THEN fb.default_start
            WHEN c.peak_hour IS NOT NULL
                 THEN LEAST(c.peak_hour - interval '90 minutes', c.closed_at - interval '30 minutes')
       END AS call_start_est,
       CASE WHEN c.text_start IS NOT NULL THEN 'release_text'
            WHEN c.first_lock IS NOT NULL THEN 'first_lock_minus_20m'
            WHEN fb.override_start IS NOT NULL THEN 'manual_override'
            WHEN fb.company_start IS NOT NULL THEN 'event_date_plus_company_time'
            WHEN fb.default_start IS NOT NULL THEN 'event_date_plus_default_time'
            WHEN c.peak_hour IS NOT NULL THEN 'hourly_lock_peak_minus_90m_lowconf'
       END AS call_start_source,
       round(EXTRACT(epoch FROM c.first_lock - c.scheduled_start) / 60::numeric) AS lock_minutes_after_scheduled
FROM core c
LEFT JOIN fb USING (event_ticker);

CREATE OR REPLACE VIEW kalshi.v_call_times AS
SELECT b.event_ticker,
       b.symbol,
       b.closed_at,
       b.release_at,
       b.scheduled_start,
       b.first_lock,
       b.scheduled_start_ok,
       CASE WHEN o.event_ticker IS NOT NULL THEN o.call_start ELSE b.call_start_est END AS call_start_est,
       CASE WHEN o.event_ticker IS NOT NULL THEN 'manual_override' ELSE b.call_start_source END AS call_start_source,
       b.lock_minutes_after_scheduled
FROM kalshi.v_call_times_base b
LEFT JOIN kalshi.call_time_overrides o ON o.event_ticker = b.event_ticker;
