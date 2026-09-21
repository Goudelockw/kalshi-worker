-- transcripts.call_date now holds the call date parsed from the page; the date in the Fool
-- URL (what the queue carries) is kept separately as published_date.
ALTER TABLE kalshi.transcripts ADD COLUMN IF NOT EXISTS published_date date;
