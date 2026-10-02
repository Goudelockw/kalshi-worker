"""Load daily prices and EPS estimate/actual from Yahoo for every mentions company.
Usage: python market_data.py [--full]   (default: last ~120 days of prices, last 8 earnings)"""
import sys, time, math, warnings
import yfinance as yf
from nsql import q, bulk
warnings.filterwarnings("ignore")
FULL = "--full" in sys.argv
OVERRIDE = {"AC": "AC.TO", "ARITZIA": "ATZ.TO"}

syms = [r["s"] for r in q("""select distinct replace(series_ticker,'KXEARNINGSMENTION','') s
  from kalshi.markets where series_ticker like 'KXEARNINGSMENTION%' order by 1""")]
cmap = {r["symbol"]: r["ticker"] for r in q("select symbol, ticker from kalshi.company_map where ticker is not null")}
ymap = {s: OVERRIDE.get(s, cmap.get(s, s)) for s in syms}
bulk("""insert into kalshi.yahoo_map(symbol, yahoo_ticker)
  select symbol, yahoo_ticker from json_to_recordset($1::json) as x(symbol text, yahoo_ticker text)
  on conflict (symbol) do update set yahoo_ticker = excluded.yahoo_ticker""",
     [{"symbol": s, "yahoo_ticker": y} for s, y in ymap.items()])

def num(v):
    try:
        v = float(v); return None if math.isnan(v) else v
    except Exception: return None

tickers = sorted(set(ymap.values()) | {"SPY"})
start = "2022-06-01" if FULL else None
bad_px, bad_eps, n_px, n_eps = [], [], 0, 0
for i, yt in enumerate(tickers):
    try:
        h = yf.Ticker(yt).history(start=start, auto_adjust=False) if FULL else yf.Ticker(yt).history(period="6mo", auto_adjust=False)
        rows = [{"yahoo_ticker": yt, "d": str(ix.date()), "close": num(r["Close"]), "adj_close": num(r.get("Adj Close", r["Close"])),
                 "volume": int(r["Volume"]) if num(r["Volume"]) is not None else None} for ix, r in h.iterrows() if num(r["Close"]) is not None]
        if rows:
            bulk("""insert into kalshi.stock_prices(yahoo_ticker,d,close,adj_close,volume)
              select * from json_to_recordset($1::json) as x(yahoo_ticker text, d date, close numeric, adj_close numeric, volume bigint)
              on conflict (yahoo_ticker,d) do update set close=excluded.close, adj_close=excluded.adj_close, volume=excluded.volume, fetched_at=now()""", rows)
            n_px += len(rows)
        else: bad_px.append(yt)
    except Exception as e:
        bad_px.append(f"{yt}:{str(e)[:60]}")
    time.sleep(0.4)
for s, yt in sorted(ymap.items()):
    try:
        ed = yf.Ticker(yt).get_earnings_dates(limit=28 if FULL else 8)
        rows = []
        if ed is not None:
            for ix, r in ed.iterrows():
                act = num(r.get("Reported EPS"))
                if act is None: continue
                rows.append({"symbol": s, "yahoo_ticker": yt, "report_ts": ix.isoformat(), "report_date": str(ix.date()),
                             "eps_est": num(r.get("EPS Estimate")), "eps_actual": act, "surprise_pct": num(r.get("Surprise(%)"))})
        rows = list({r["report_date"]: r for r in rows}.values())  # Yahoo can list a date twice
        if rows:
            bulk("""insert into kalshi.earnings_surprise(symbol,yahoo_ticker,report_ts,report_date,eps_est,eps_actual,surprise_pct)
              select * from json_to_recordset($1::json) as x(symbol text, yahoo_ticker text, report_ts timestamptz, report_date date, eps_est numeric, eps_actual numeric, surprise_pct numeric)
              on conflict (symbol,report_date) do update set eps_est=excluded.eps_est, eps_actual=excluded.eps_actual, surprise_pct=excluded.surprise_pct, report_ts=excluded.report_ts, fetched_at=now()""", rows)
            n_eps += len(rows)
        else: bad_eps.append(yt)
    except Exception as e:
        bad_eps.append(f"{yt}:{str(e)[:60]}")
    time.sleep(1.0)
print(f"price rows {n_px}, eps rows {n_eps}")
print("no prices:", bad_px)
print("no eps:", bad_eps)
