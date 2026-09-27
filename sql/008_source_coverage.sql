-- Source coverage per earnings-mention company: one row per KXEARNINGSMENTION<symbol> series,
-- with stored transcripts / 8-K + 6-K earnings releases, the SEC CIK (null when the symbol
-- isn't in the SEC ticker file) and whether the transcripts / filings jobs have run their
-- per-symbol backfill (sync_state rows transcripts_backfill:<SYMBOL> / filings_backfill:<SYMBOL>).
CREATE OR REPLACE VIEW kalshi.v_source_coverage AS
WITH s AS (
    SELECT DISTINCT regexp_replace(series_ticker, '^KXEARNINGSMENTION', '') AS symbol
    FROM kalshi.markets WHERE series_ticker LIKE 'KXEARNINGSMENTION%'
), t AS (
    SELECT symbol, count(*) AS n, max(call_date) AS last_call FROM kalshi.transcripts GROUP BY symbol
), f AS (
    SELECT symbol, count(*) AS n, max(filed_at) AS last_filing FROM kalshi.filings GROUP BY symbol
)
SELECT s.symbol,
       coalesce(t.n, 0)::int AS n_transcripts,
       t.last_call,
       coalesce(f.n, 0)::int AS n_filings,
       f.last_filing,
       cm.cik,
       EXISTS (SELECT 1 FROM kalshi.sync_state ss WHERE ss.job = 'transcripts_backfill:' || s.symbol) AS backfilled_transcripts,
       EXISTS (SELECT 1 FROM kalshi.sync_state ss WHERE ss.job = 'filings_backfill:' || s.symbol) AS backfilled_filings
FROM s
LEFT JOIN t USING (symbol)
LEFT JOIN f USING (symbol)
LEFT JOIN kalshi.company_map cm USING (symbol)
WHERE s.symbol <> '';
