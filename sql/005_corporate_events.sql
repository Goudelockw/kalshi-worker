-- Corporate-event 8-Ks (and 8-K/As) for companies with earnings-mention markets: items 1.01
-- (material agreement), 1.02 (terminated agreement), 2.01 (acquisition/disposition), 2.05
-- (exit/restructuring costs), 2.06 (impairment) and 5.02 (officer/director change), with the
-- primary document as text. company_map also keeps the SEC industry code.
CREATE TABLE kalshi.corporate_events (
  accession text PRIMARY KEY, symbol text NOT NULL, cik text NOT NULL,
  form text NOT NULL, items text[] NOT NULL, filed_at timestamptz NOT NULL,
  doc_url text, raw_text text, fetched_at timestamptz DEFAULT now());
CREATE INDEX ON kalshi.corporate_events (symbol, filed_at DESC);
ALTER TABLE kalshi.company_map ADD COLUMN IF NOT EXISTS sic text, ADD COLUMN IF NOT EXISTS sic_description text;

-- One row per open (active) KXEARNINGSMENTION market. Word alternatives are built exactly as in
-- v_mentions_screen: split yes_sub_title on "/", drop any "(N+ times)" suffix, keep letters,
-- digits, spaces and hyphens, allow any run of spaces/hyphens between words, and accept a
-- plural or possessive ending, matched case-insensitively on word boundaries.
CREATE OR REPLACE VIEW kalshi.v_structural_flags AS
WITH om AS (
    SELECT m.ticker,
           regexp_replace(m.series_ticker, '^KXEARNINGSMENTION', '') AS symbol,
           m.yes_sub_title AS word
    FROM kalshi.markets m
    WHERE m.series_ticker LIKE 'KXEARNINGSMENTION%' AND m.status = 'active'
), alts AS (
    SELECT om.ticker, om.symbol,
           '\m' || regexp_replace(lower(btrim(regexp_replace(regexp_replace(a.a, '\(\d+\+?\s*times\)', '', 'gi'),
                                                            '[^[:alnum:][:space:]-]', '', 'g'))),
                                  '[\s-]+', '[\\s-]+', 'g')
                || '(s|es|[''’]s|s[''’])?\M' AS pat
    FROM om
    CROSS JOIN LATERAL unnest(string_to_array(om.word, '/')) AS a(a)
    WHERE btrim(a.a) <> ''
), ev AS (
    SELECT ce.symbol,
           array_agg(DISTINCT i.item ORDER BY i.item) AS items,
           bool_or(i.item = '5.02') AS exec_change
    FROM kalshi.corporate_events ce
    CROSS JOIN LATERAL unnest(ce.items) AS i(item)
    WHERE ce.filed_at > now() - interval '120 days'
      AND i.item IN ('1.01', '1.02', '2.01', '2.05', '2.06', '5.02')
    GROUP BY ce.symbol
), hits AS (
    SELECT al.ticker, array_agg(DISTINCT ce.accession ORDER BY ce.accession) AS accessions
    FROM alts al
    JOIN kalshi.corporate_events ce
      ON ce.symbol = al.symbol
     AND ce.filed_at > now() - interval '365 days'
     AND ce.items && ARRAY['1.01', '1.02', '2.01', '2.05', '2.06']
     AND ce.raw_text ~* al.pat
    GROUP BY al.ticker
)
SELECT om.ticker,
       om.symbol,
       om.word,
       coalesce(ev.items, '{}'::text[])        AS company_events_120d,
       coalesce(ev.exec_change, false)         AS exec_change_120d,
       hits.accessions IS NOT NULL             AS word_in_event_filing,
       coalesce(hits.accessions, '{}'::text[]) AS word_event_accessions
FROM om
LEFT JOIN ev ON ev.symbol = om.symbol
LEFT JOIN hits ON hits.ticker = om.ticker;
