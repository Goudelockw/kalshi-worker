-- Equibles (third transcript source) gap filling.
-- v_transcript_gaps: calls we have no transcript for, one row per (symbol, target_date):
--   * settled mention events whose label audit found no transcript (v_label_audit.audit =
--     'no_transcript'), target = call_date_actual;
--   * earnings releases in kalshi.filings from the last 2 years for tracked symbols with no
--     transcript (any source) within 3 days of the filing's ET date, target = that date.
-- Symbols without a US listing (no SEC CIK in company_map, e.g. AC, ARITZIA) are skipped.
-- equibles_attempts logs every lookup; the job retries a gap at most once every 7 days.
CREATE TABLE IF NOT EXISTS kalshi.equibles_attempts (
  symbol text NOT NULL,
  target_date date NOT NULL,
  tried_at timestamptz NOT NULL DEFAULT now(),
  ok boolean NOT NULL,
  note text,
  PRIMARY KEY (symbol, target_date, tried_at)
);

CREATE OR REPLACE VIEW kalshi.v_transcript_gaps AS
WITH tracked AS (
    SELECT DISTINCT regexp_replace(series_ticker, '^KXEARNINGSMENTION', '') AS symbol
    FROM kalshi.markets WHERE series_ticker LIKE 'KXEARNINGSMENTION%'
), audit AS (
    SELECT DISTINCT symbol, call_date_actual AS target_date, 'label_audit' AS reason
    FROM kalshi.v_label_audit WHERE audit = 'no_transcript' AND call_date_actual IS NOT NULL
), rel AS (
    SELECT DISTINCT f.symbol, (f.filed_at AT TIME ZONE 'America/New_York')::date AS target_date, 'filing' AS reason
    FROM kalshi.filings f JOIN tracked USING (symbol)
    WHERE f.filed_at >= now() - interval '2 years'
      AND NOT EXISTS (SELECT 1 FROM kalshi.transcripts t
                      WHERE t.symbol = f.symbol
                        AND t.call_date BETWEEN (f.filed_at AT TIME ZONE 'America/New_York')::date - 3
                                            AND (f.filed_at AT TIME ZONE 'America/New_York')::date + 3)
)
SELECT g.symbol, g.target_date, string_agg(DISTINCT g.reason, ',' ORDER BY g.reason) AS reasons
FROM (SELECT * FROM audit UNION ALL SELECT * FROM rel) g
WHERE EXISTS (SELECT 1 FROM kalshi.company_map cm WHERE cm.symbol = g.symbol AND cm.cik IS NOT NULL)
GROUP BY g.symbol, g.target_date;
