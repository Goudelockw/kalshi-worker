-- transcript_sources.published_date: the date in the Fool URL path (call_date on the queue has
-- always held the same value; both are set by discovery so ordering and joins keep working).
ALTER TABLE kalshi.transcript_sources ADD COLUMN IF NOT EXISTS published_date date;
UPDATE kalshi.transcript_sources SET published_date = call_date WHERE published_date IS NULL;
