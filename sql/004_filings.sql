-- 8-K earnings press releases (Item 2.02, Exhibit 99.1) from SEC EDGAR for companies with
-- earnings-mention markets; company_map caches the symbol -> CIK lookup.
CREATE TABLE kalshi.company_map (symbol text PRIMARY KEY, cik text NOT NULL, name text, updated_at timestamptz DEFAULT now());
CREATE TABLE kalshi.filings (
  accession text PRIMARY KEY, cik text NOT NULL, symbol text NOT NULL, form text NOT NULL,
  items text[], filed_at timestamptz NOT NULL, period date, exhibit_url text,
  raw_text text, word_count int, fetched_at timestamptz DEFAULT now());
CREATE INDEX ON kalshi.filings (symbol, filed_at DESC);
CREATE INDEX ON kalshi.filings USING gin (to_tsvector('english', raw_text));
