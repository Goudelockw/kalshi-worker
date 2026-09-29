-- Exact publish times for news articles (kalshi_worker/news_dates.py, `news-dates` job).
-- Serper's dates are mostly relative ("10 months ago", measured from the fetch) and Google re-dates
-- stale pages, so they can't prove an article predates the call. An article now counts only once
-- news-dates has read its publish time from the page: exact_published_at < cutoff, not before
-- news_counts.window_start, not a stale re-date, not a recap title.
-- Every existing row is reset to unverified here; `python -m kalshi_worker news-dates` re-verifies
-- them. date_raw and published_at (Serper's date) stay for reference.

alter table kalshi.news_articles
  add column if not exists exact_published_at timestamptz,   -- from the page (JSON-LD, meta, <time>)
  add column if not exists date_source text,                 -- json_ld | article_published_time | meta_name | itemprop | article_time | none
  add column if not exists http_status int,                  -- null: the request failed
  add column if not exists date_checked_at timestamptz;

alter table kalshi.news_counts
  add column if not exists dates_verified boolean not null default false;   -- all of the market's and its company baseline's articles checked

update kalshi.news_articles set counted = false, reject_reason = 'unverified';
update kalshi.news_counts set dates_verified = false;
