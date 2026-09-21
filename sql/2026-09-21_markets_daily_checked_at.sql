-- markets.daily_checked_at: when the backfill last fetched daily candles for a settled
-- market (set whether or not candles came back). Backfilled for markets that already hold
-- daily candles so the first run after this change only visits the rest.
ALTER TABLE kalshi.markets ADD COLUMN IF NOT EXISTS daily_checked_at timestamptz;
UPDATE kalshi.markets m SET daily_checked_at = now()
WHERE EXISTS (SELECT 1 FROM kalshi.candles c WHERE c.ticker = m.ticker AND c.period_minutes = 1440);
