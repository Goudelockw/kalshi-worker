-- Exchange ticker per Kalshi earnings-mention symbol. Kalshi series symbols aren't always
-- tickers (KXEARNINGSMENTIONADOBE -> ADBE, ARITZIA -> ATZ on the TSX); the aliases live in
-- kalshi_worker/tickers.py. cik becomes nullable for listings with no SEC filer (TSX-only).
ALTER TABLE kalshi.company_map ADD COLUMN IF NOT EXISTS ticker text;
ALTER TABLE kalshi.company_map ALTER COLUMN cik DROP NOT NULL;
UPDATE kalshi.company_map SET ticker = symbol WHERE ticker IS NULL;
