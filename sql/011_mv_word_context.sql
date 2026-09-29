-- kalshi.mv_word_context: same-quarter-last-year and peer-industry (2-digit SIC) word features
-- per earnings-mention market; joined by kalshi.mv_model_input. Created by hand in Neon; this is
-- its definition as of pg_get_viewdef('kalshi.mv_word_context'::regclass, true), kept as a
-- record. Reads mv_word_counts, mv_segment_tsv and mv_word_corpus_hits; refreshed after
-- mv_word_corpus in kalshi_worker/db.py REFRESH_VIEWS. Unqualified names resolve in schema kalshi.
SET search_path = kalshi, public;

CREATE MATERIALIZED VIEW IF NOT EXISTS kalshi.mv_word_context AS
 WITH sym_map AS (
         SELECT DISTINCT ON (z.tx_sym) z.tx_sym,
            z.kalshi_sym,
            z.sic2
           FROM ( SELECT company_map.symbol AS tx_sym,
                    company_map.symbol AS kalshi_sym,
                    "left"(company_map.sic, 2) AS sic2,
                    0 AS pr
                   FROM company_map
                UNION ALL
                 SELECT company_map.ticker,
                    company_map.symbol,
                    "left"(company_map.sic, 2) AS "left",
                    1
                   FROM company_map
                  WHERE company_map.ticker IS NOT NULL AND company_map.ticker <> company_map.symbol) z
          ORDER BY z.tx_sym, z.pr
        ), calls AS (
         SELECT DISTINCT s.transcript_id,
            s.symbol AS tx_sym,
            s.call_date,
            sm.sic2
           FROM mv_segment_tsv s
             LEFT JOIN sym_map sm ON sm.tx_sym = s.symbol
        ), mk AS (
         SELECT w.ticker,
            w.symbol,
            m.yes_sub_title AS word,
            w.call_date_est AS d,
            ( SELECT "left"(c.sic, 2) AS "left"
                   FROM company_map c
                  WHERE c.symbol = w.symbol) AS sic2,
            ARRAY( SELECT w.symbol
                UNION
                 SELECT c.ticker
                   FROM company_map c
                  WHERE c.symbol = w.symbol AND c.ticker IS NOT NULL) AS own_syms
           FROM mv_word_counts w
             JOIN markets m USING (ticker)
          WHERE w.call_date_est IS NOT NULL
        ), yoy AS (
         SELECT DISTINCT ON (mk_1.ticker) mk_1.ticker,
            c.transcript_id,
            c.call_date
           FROM mk mk_1
             JOIN calls c ON (c.tx_sym = ANY (mk_1.own_syms)) AND c.call_date >= (mk_1.d - 400) AND c.call_date <= (mk_1.d - 330)
          ORDER BY mk_1.ticker, (abs(mk_1.d - 365 - c.call_date))
        ), pden AS (
         SELECT mk_1.ticker,
            count(*) AS n,
            count(DISTINCT c.tx_sym) AS cos
           FROM mk mk_1
             JOIN calls c ON c.sic2 = mk_1.sic2 AND NOT (c.tx_sym = ANY (mk_1.own_syms)) AND c.call_date < (mk_1.d - 2) AND c.call_date >= (mk_1.d - 180)
          GROUP BY mk_1.ticker
        ), phit AS (
         SELECT mk_1.ticker,
            count(*) AS h
           FROM mk mk_1
             JOIN mv_word_corpus_hits h ON h.word = mk_1.word AND NOT (h.symbol = ANY (mk_1.own_syms)) AND h.call_date < (mk_1.d - 2) AND h.call_date >= (mk_1.d - 180)
             JOIN calls c ON c.transcript_id = h.transcript_id AND c.sic2 = mk_1.sic2
          GROUP BY mk_1.ticker
        )
 SELECT mk.ticker,
    yoy.transcript_id IS NOT NULL AS has_yoy,
    yoy.call_date AS yoy_call_date,
        CASE
            WHEN yoy.transcript_id IS NULL THEN NULL::boolean
            ELSE (EXISTS ( SELECT 1
               FROM mv_word_corpus_hits h
              WHERE h.word = mk.word AND h.transcript_id = yoy.transcript_id))
        END AS said_yoy,
    word_tsquery(mk.word) IS NOT NULL AS word_searchable,
    COALESCE(pden.n, 0::bigint)::integer AS peer_calls_180,
    COALESCE(pden.cos, 0::bigint)::integer AS peer_cos_180,
    COALESCE(phit.h, 0::bigint)::integer AS peer_hits_180,
    round((COALESCE(phit.h, 0::bigint)::numeric + 0.5) / (COALESCE(pden.n, 0::bigint)::numeric + 1.0), 5) AS peer_rate_180,
    now() AS refreshed_at
   FROM mk
     LEFT JOIN yoy USING (ticker)
     LEFT JOIN pden USING (ticker)
     LEFT JOIN phit USING (ticker);

CREATE UNIQUE INDEX IF NOT EXISTS mv_word_context_ticker ON kalshi.mv_word_context USING btree (ticker);
