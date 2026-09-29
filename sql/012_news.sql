-- Pre-call news volume from the Serper Google News API (kalshi_worker/news.py, `news` job).
-- news_articles keeps every article Serper returned, counted or not, for auditing;
-- news_counts holds the counted totals per market. Nothing published at or after
-- call_start - 2h may be counted; the job checks
--   select count(*) from kalshi.news_articles where counted and published_at >= cutoff;
-- at the end of every run and fails if it is above 0.

create table if not exists kalshi.news_articles (      -- one row per article returned, for auditing
  ticker text not null,              -- kalshi market ticker ('__company__' + event_ticker for the company baseline)
  event_ticker text not null,
  url text not null,
  title text, source text,
  date_raw text,                     -- Serper's date string exactly as returned
  published_at timestamptz,          -- parsed absolute time; null if unparseable
  call_start timestamptz not null,   -- from kalshi.mv_call_times.call_start_est at fetch time
  cutoff timestamptz not null,       -- call_start - 2 hours
  counted boolean not null,          -- true only if it passed every check below
  reject_reason text,                -- 'no_date' | 'after_cutoff' | 'recap_title' | null
  fetched_at timestamptz not null default now(),
  primary key (ticker, url)
);
create table if not exists kalshi.news_counts (
  ticker text primary key,
  event_ticker text not null,
  symbol text not null,
  query text not null,
  call_start timestamptz not null,
  window_start date not null,        -- (call_start in America/New_York)::date - 14
  window_end date not null,          -- (call_start in America/New_York)::date - 1   <- never the call day
  n_word_articles int,               -- counted articles for company + word
  n_company_articles int,            -- counted articles for the company alone (event baseline)
  n_rejected int,                    -- returned but not counted (dates or recap titles)
  fetched_at timestamptz not null default now(),
  error text
);
