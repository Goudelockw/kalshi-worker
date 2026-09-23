-- Paragraph-level boilerplate filter for 8-K press releases. kalshi.split_boilerplate(raw,
-- company) is the SQL twin of kalshi_worker.filings.split_boilerplate: paragraphs (split on
-- blank lines) are boilerplate when they mention forward-looking statements, safe harbor,
-- risks and uncertainties, undertake(s) no obligation or the Private Securities Litigation
-- Reform Act, are a "Non-GAAP Financial Measures" heading, or start with "About <company>"
-- (a bare About heading also takes the paragraph after it); a paragraph that is exactly a
-- Contact / Investor Relations / Media Contact heading takes everything after it. Used once
-- to re-split every stored filing in place; `filings --reparse` does the same in Python for
-- rows whose body_text is null.
CREATE OR REPLACE FUNCTION kalshi.split_boilerplate(raw text, company text, OUT body text, OUT boilerplate text)
LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE
  para text; w text; about_re text := NULL; about_ci boolean := true;
  kept text[] := '{}'; dropped text[] := '{}';
  after_contact boolean := false; absorb_next boolean := false; is_about boolean;
BEGIN
  FOREACH w IN ARRAY regexp_split_to_array(coalesce(company, ''), '\s+') LOOP
    IF w <> '' AND lower(w) NOT IN ('the','inc','inc.','corp','corp.','corporation','co','co.','company','plc','ltd','ltd.','llc','holdings','group','&') THEN
      about_re := '^About\s+' || regexp_replace(rtrim(w, ',.'), '([\\^$.|?*+()\[\]{}])', '\\\1', 'g');
      EXIT;
    END IF;
  END LOOP;
  IF about_re IS NULL THEN about_re := '^About\s+[A-Z]'; about_ci := false; END IF;
  FOREACH para IN ARRAY regexp_split_to_array(coalesce(raw, ''), '\n\s*\n') LOOP
    para := btrim(para, E' \t\r\n');
    CONTINUE WHEN para = '';
    IF after_contact OR para ~* '^\W*(contacts?|investor relations|investor contacts?|media contacts?|media relations|press contacts?)\W*$' THEN
      after_contact := true; dropped := dropped || para;
    ELSIF absorb_next THEN
      dropped := dropped || para; absorb_next := false;
    ELSE
      is_about := CASE WHEN about_ci THEN para ~* about_re ELSE para ~ about_re END;
      IF para ~* 'forward[- ]looking|safe harbor|risks and uncertainties|undertakes? no obligation|private securities litigation'
         OR para ~* '^\W*(use of |reconciliation of )?non-gaap financial measures\W*$'
         OR is_about THEN
        dropped := dropped || para;
        absorb_next := is_about AND position(E'\n' IN para) = 0
                       AND array_length(regexp_split_to_array(para, '\s+'), 1) <= 8 AND right(rtrim(para), 1) <> '.';
      ELSE
        kept := kept || para;
      END IF;
    END IF;
  END LOOP;
  body := array_to_string(kept, E'\n\n');
  boilerplate := array_to_string(dropped, E'\n\n');
END $$;

-- one-off: re-split every stored filing with the new rules
UPDATE kalshi.filings f
SET (body_text, boilerplate_text) = (
    SELECT s.body, s.boilerplate
    FROM kalshi.split_boilerplate(f.raw_text, (SELECT cm.name FROM kalshi.company_map cm WHERE cm.symbol = f.symbol)) s);
