# ingestion_prices_only.py – stream live stock prices **and persist per‑ticker CSV**
# ------------------------------------------------------------------
# HOW TO RUN (inside your `env` venv):
#     python backend/src/ingestion_prices_only.py
# ------------------------------------------------------------------

import csv
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import yfinance as yf
import pathway as pw

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

# ──────────────────────────────
# 6) ATTACH A SINK SO THE ENGINE STAYS ALIVE
#    Without a downstream consumer the Pathway graph finishes instantly.
#    `compute_and_print` gives the engine a node that references `prices`,
#    so `pw.run()` keeps streaming.
# ──────────────────────────────
# print every record (good for quick debugging)
_ = pw.debug.compute_and_print(prices, include_id=False)

print("\nStreaming live prices and saving CSVs… Ctrl‑C to stop.\n")

pw.run()
