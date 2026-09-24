-- Market reaction to 8-K earnings press releases, one row per (market, filing): the filer's
-- KXEARNINGSMENTION<symbol> markets closing within 36 hours after filed_at, the yes bid/ask
-- midpoint from the last 1-minute candle at or before each offset from filed_at (candles with
-- both quotes present), and 1-minute volume from -60 to +60 minutes. Candles come from the
-- `reactions` command (period_minutes = 1; a candle's ts is the end of its minute).

CREATE OR REPLACE FUNCTION kalshi.mid_at(p_ticker text, p_at timestamptz) RETURNS numeric
LANGUAGE sql STABLE AS $$
    SELECT (c.yes_bid_close + c.yes_ask_close) / 2
    FROM kalshi.candles c
    WHERE c.ticker = p_ticker AND c.period_minutes = 1 AND c.ts <= p_at
      AND c.yes_bid_close IS NOT NULL AND c.yes_ask_close IS NOT NULL
    ORDER BY c.ts DESC
    LIMIT 1
$$;

CREATE OR REPLACE VIEW kalshi.v_release_reaction AS
WITH pairs AS (
    SELECT m.ticker, f.symbol, m.yes_sub_title AS word, f.accession, f.filed_at, m.result
    FROM kalshi.filings f
    JOIN kalshi.markets m ON m.series_ticker = 'KXEARNINGSMENTION' || f.symbol
     AND m.close_time >= f.filed_at AND m.close_time <= f.filed_at + interval '36 hours'
)
SELECT p.ticker,
       p.symbol,
       p.word,
       p.filed_at,
       p.accession,
       p.result,
       kalshi.mid_at(p.ticker, p.filed_at - interval '180 minutes') AS mid_m180,
       kalshi.mid_at(p.ticker, p.filed_at - interval '60 minutes')  AS mid_m60,
       kalshi.mid_at(p.ticker, p.filed_at - interval '15 minutes')  AS mid_m15,
       kalshi.mid_at(p.ticker, p.filed_at - interval '5 minutes')   AS mid_m5,
       kalshi.mid_at(p.ticker, p.filed_at + interval '5 minutes')   AS mid_p5,
       kalshi.mid_at(p.ticker, p.filed_at + interval '15 minutes')  AS mid_p15,
       kalshi.mid_at(p.ticker, p.filed_at + interval '30 minutes')  AS mid_p30,
       kalshi.mid_at(p.ticker, p.filed_at + interval '60 minutes')  AS mid_p60,
       (SELECT sum(c.volume) FROM kalshi.candles c
        WHERE c.ticker = p.ticker AND c.period_minutes = 1
          AND c.ts > p.filed_at - interval '60 minutes' AND c.ts <= p.filed_at + interval '60 minutes') AS volume_m60_p60
FROM pairs p;
