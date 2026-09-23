-- Press-release body vs. disclaimer boilerplate. body_text is everything before the first
-- forward-looking-statements / cautionary-statement / safe-harbor phrase or "About <Company>"
-- line found in the second half of raw_text; boilerplate_text is the rest. Mention matching
-- now runs against body_text so disclaimer wording does not count as "said".
ALTER TABLE kalshi.filings ADD COLUMN IF NOT EXISTS body_text text;
ALTER TABLE kalshi.filings ADD COLUMN IF NOT EXISTS boilerplate_text text;

CREATE OR REPLACE VIEW kalshi.v_filing_mentions AS
WITH words AS (
    SELECT DISTINCT
           substring(m.series_ticker FROM '^KXEARNINGSMENTION(.+)$') AS symbol,
           lower(btrim(regexp_replace(regexp_replace(w, '[^[:alnum:][:space:]]', '', 'g'), '\s+', ' ', 'g'))) AS word
    FROM kalshi.markets m
    CROSS JOIN LATERAL unnest(string_to_array(m.yes_sub_title, '/')) AS w
    WHERE m.series_ticker LIKE 'KXEARNINGSMENTION%'
      AND coalesce(m.yes_sub_title, '') <> ''
)
SELECT f.symbol,
       f.accession,
       f.filed_at,
       w.word,
       (f.body_text ~* ('\m' || replace(w.word, ' ', '\s+'))) AS said
FROM kalshi.filings f
JOIN words w ON w.symbol = f.symbol
WHERE w.word <> '';
