-- Base-rate table for earnings-call mention markets: one row per transcript and per distinct
-- word ever used as a yes_sub_title in that company's KXEARNINGSMENTION<symbol> markets.
-- A yes_sub_title like "Generative AI / Gen AI" is split on "/" into separate words;
-- punctuation is stripped; `said` is true when any speaker's segment contains the word
-- (case-insensitive, whole-word, spaces between words flexible).
CREATE OR REPLACE VIEW kalshi.v_transcript_mentions AS
WITH words AS (
    SELECT DISTINCT
           substring(m.series_ticker FROM '^KXEARNINGSMENTION(.+)$') AS symbol,
           lower(btrim(regexp_replace(regexp_replace(w, '[^[:alnum:][:space:]]', '', 'g'), '\s+', ' ', 'g'))) AS word
    FROM kalshi.markets m
    CROSS JOIN LATERAL unnest(string_to_array(m.yes_sub_title, '/')) AS w
    WHERE m.series_ticker LIKE 'KXEARNINGSMENTION%'
      AND coalesce(m.yes_sub_title, '') <> ''
)
SELECT t.symbol,
       t.fiscal_year,
       t.fiscal_quarter,
       t.call_date,
       w.word,
       EXISTS (SELECT 1
               FROM kalshi.transcript_segments s
               WHERE s.transcript_id = t.id
                 AND s.text ~* ('\m' || replace(w.word, ' ', '\s+') || '\M')) AS said
FROM kalshi.transcripts t
JOIN words w ON w.symbol = t.symbol
WHERE w.word <> '';
