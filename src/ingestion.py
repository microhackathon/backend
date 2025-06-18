# ingestion.py  – run with:  python ingestion.py
import os, time, json, requests, yfinance as yf
from datetime import datetime, timezone
import pathway as pw
from newsapi import NewsApiClient
from pathlib import Path


# ───────────────────────────
# 1) configuration
# ───────────────────────────
TICKERS         = ["AAPL", "TSLA", "NVDA"]          # add whatever you like
POLL_PRICE_SECS = 60                                # Yahoo price refresh cadence
POLL_NEWS_SECS  = 300                               # NewsAPI cadence (rate-limit friendly)
FMP_API_KEY     = os.getenv("c739F1h6jEt0BZxfvYuvMbeKcb7XMNtN")          # FinancialModelingPrep key-metrics
NEWS_API_KEY    = os.getenv("3dd476b0d6a84fcbb6b35a1fbd153de0")          # NewsAPI key

news_api        = NewsApiClient(api_key=NEWS_API_KEY)

# ──────────────────────────────
# 1) BASIC CONFIG
# ──────────────────────────────
TICKERS = ["AAPL", "TSLA", "NVDA"]   # add/remove symbols if needed
POLL_PRICE_SECS = 60                    # fetch cadence (seconds)
DATA_DIR = Path("data")                # root folder for persisted files

# ensure base directory exists
DATA_DIR.mkdir(parents=True, exist_ok=True)

# ──────────────────────────────
# 2) SCHEMA  – defines Pathway columns
# ──────────────────────────────
class PriceSchema(pw.Schema):
    ticker: str
    timestamp: str   # ISO‑8601 string
    price: float
    volume: int

# ──────────────────────────────
# 3) helper: append a row to <data>/<TICKER>/prices.csv
# ──────────────────────────────

def persist_row(row: dict) -> None:
    ticker_dir = DATA_DIR / row["ticker"]
    ticker_dir.mkdir(exist_ok=True)
    csv_path = ticker_dir / "prices.csv"

    write_header = not csv_path.exists()
    with csv_path.open("a", newline="") as fp:
        writer = csv.writer(fp)
        if write_header:
            writer.writerow(["timestamp", "price", "volume"])
        writer.writerow([row["timestamp"], row["price"], row["volume"]])

# ──────────────────────────────
# 4) CONNECTOR SUBJECT – pulls data & streams rows (plus persists)
# ──────────────────────────────
class PriceSubject(pw.io.python.ConnectorSubject):
    """Continuously fetches latest price & volume for each ticker and persists to CSV."""

    deletions_enabled = False  # we only append new rows

    def run(self):
        while True:
            utc = datetime.now(timezone.utc).isoformat()
            for symbol in TICKERS:
                info = yf.Ticker(symbol).fast_info
                price = info.get("last_price") or 0.0
                vol = info.get("last_volume") or 0

                row = {
                    "ticker": symbol,
                    "timestamp": utc,
                    "price": float(price),
                    "volume": int(vol),
                }

                # emit row to Pathway
                self.next_json(row)

                # persist to per‑ticker CSV
                persist_row(row)

                # immediate feedback
                print(f"{utc} | {symbol} | ${price:.2f} | vol {vol:,}")
            time.sleep(POLL_PRICE_SECS)

# ──────────────────────────────
# 5) BUILD THE PATHWAY TABLE & RUN
# ──────────────────────────────
prices = pw.io.python.read(PriceSubject(), schema=PriceSchema)

# Optional: do something with `prices` inside Pathway (vector store, etc.)

print("\nStreaming live prices and saving CSVs… Ctrl‑C to stop.\n")

pw.run()



# # ───────────────────────────
# # 2) Pathway schema
# # ───────────────────────────
# class DocSchema(pw.Schema):
#     ticker    : str | None     # None for macro news
#     type      : str            # price | ratios | news | market_news
#     timestamp : str            # ISO-8601
#     content   : str            # human-readable blob for embedding / retrieval

# # ───────────────────────────
# # 3) custom Pathway connector subjects
# # ───────────────────────────
# class PriceSubject(pw.io.python.ConnectorSubject):
#     deletions_enabled = False
#     def run(self):
#         while True:
#             utc = datetime.now(timezone.utc).isoformat()
#             for t in TICKERS:
#                 info = yf.Ticker(t).fast_info
#                 price = info.get("last_price")
#                 vol   = info.get("last_volume")
#                 self.output(ticker=t, type="price", timestamp=utc,
#                           content=f"{t} trades at {price:.2f} USD, volume {vol:,}")
#             time.sleep(POLL_PRICE_SECS)

# class RatiosSubject(pw.io.python.ConnectorSubject):
#     deletions_enabled = False
#     def run(self):
#         while True:
#             for t in TICKERS:
#                 url = (f"https://financialmodelingprep.com/api/v3/key-metrics/{t}"
#                        f"?limit=1&apikey={FMP_API_KEY}")
#                 r = requests.get(url, timeout=10).json()
#                 if r:
#                     m = r[0]
#                     text = (f"{t} fundamentals – PE {m.get('peRatio')}, "
#                             f"EPS {m.get('eps')}, ROE {m.get('roe')}, "
#                             f"Debt/Equity {m.get('debtEquityRatio')}")
#                     self.output(ticker=t, type="ratios",
#                               timestamp=m.get("date", datetime.utcnow().isoformat()),
#                               content=text)
#             time.sleep(24*3600)          # update daily

# class CompanyNewsSubject(pw.io.python.ConnectorSubject):
#     deletions_enabled = False
#     def run(self):
#         while True:
#             for t in TICKERS:
#                 res = news_api.get_everything(q=t,
#                                               language="en",
#                                               sort_by="publishedAt",
#                                               page_size=5)
#                 for art in res["articles"]:
#                     self.output(ticker=t, type="news",
#                               timestamp=art["publishedAt"],
#                               content=f"{art['title']}. {art['description'] or ''}")
#             time.sleep(POLL_NEWS_SECS)

# class MacroNewsSubject(pw.io.python.ConnectorSubject):
#     deletions_enabled = False
#     def run(self):
#         while True:
#             res = news_api.get_top_headlines(category="business",
#                                              language="en",
#                                              page_size=10)
#             for art in res["articles"]:
#                 self.output(ticker=None, type="market_news",
#                           timestamp=art["publishedAt"],
#                           content=f"{art['title']}. {art['description'] or ''}")
#             time.sleep(POLL_NEWS_SECS)

# # ───────────────────────────
# # 4) build Pathway tables
# # ───────────────────────────
# prices  = pw.io.python.read(PriceSubject(),   schema=DocSchema)
# ratios  = pw.io.python.read(RatiosSubject(),  schema=DocSchema)
# c_news  = pw.io.python.read(CompanyNewsSubject(), schema=DocSchema)
# m_news  = pw.io.python.read(MacroNewsSubject(),   schema=DocSchema)

# merged1 = pw.Table.promise_universes_are_disjoint(prices, ratios).concat()
# merged2 = pw.Table.promise_universes_are_disjoint(c_news, m_news).concat()
# documents = pw.Table.promise_universes_are_disjoint(merged1, merged2).concat()

# # OPTIONAL: write to console to verify it works
# pw.debug.compute_and_print(documents, include_id=False)

# # Keep the pipeline alive
# pw.run()
