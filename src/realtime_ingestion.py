
import csv
import time
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import yfinance as yf
import pathway as pw

# ──────────────────────────────
# 1) BASIC CONFIG
# ──────────────────────────────
TICKERS = ["AAPL", "TSLA", "NVDA"]
POLL_PRICE_SECS = 60          # price cadence (seconds)
POLL_TECH_SECS  = 300          # indicators cadence (5 min for dev); set back to 43200 for prod  # indicators cadence (~12 h)
DATA_DIR = Path("data")
DATA_DIR.mkdir(parents=True, exist_ok=True)

# ──────────────────────────────
# 2) PATHWAY SCHEMAS
# ──────────────────────────────
class PriceSchema(pw.Schema):
    ticker: str
    timestamp: str
    price: float
    volume: int

class IndicatorSchema(pw.Schema):
    ticker: str
    timestamp: str
    pe: float | None
    eps: float | None
    sma50: float | None
    rsi14: float | None

# ──────────────────────────────
# 3) CSV HELPERS
# ──────────────────────────────

def append_csv(ticker: str, filename: str, header: list[str], row: list):
    tdir = DATA_DIR / ticker
    tdir.mkdir(exist_ok=True)
    path = tdir / filename
    write_header = not path.exists()
    with path.open("a", newline="") as fp:
        w = csv.writer(fp)
        if write_header:
            w.writerow(header)
        w.writerow(row)

# ──────────────────────────────
# 4) SIMPLE RSI IMPLEMENTATION (14‑period)
# ──────────────────────────────

def rsi(series: pd.Series, length: int = 14) -> float | None:
    if len(series) < length + 1:
        return None
    delta = series.diff().dropna()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    avg_gain = gain.rolling(window=length).mean().iloc[-1]
    avg_loss = loss.rolling(window=length).mean().iloc[-1]
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

# ──────────────────────────────
# 5) PRICE SUBJECT (minute‑level)
# ──────────────────────────────
class PriceSubject(pw.io.python.ConnectorSubject):
    deletions_enabled = False

    def run(self):
        while True:
            utc = datetime.now(timezone.utc).isoformat()
            for symbol in TICKERS:
                info = yf.Ticker(symbol).fast_info or {}
                price = float(info.get("last_price") or 0)
                vol   = int(info.get("last_volume") or 0)

                # fallback if zero
                if price == 0:
                    try:
                        h = yf.Ticker(symbol).history(period="1d", interval="1m")
                        if not h.empty:
                            price = float(h["Close"].iloc[-1])
                            vol   = int(h["Volume"].iloc[-1])
                    except Exception:
                        pass

                row = {"ticker": symbol, "timestamp": utc, "price": price, "volume": vol}
                self.next_json(row)
                # price persistence disabled per user request
                print(f"{utc} | {symbol:<5} | $ {price:,.2f} | vol {vol:,}")
            time.sleep(POLL_PRICE_SECS)

# ──────────────────────────────
# 6) INDICATOR SUBJECT (~12‑hour cadence)
# ──────────────────────────────
class IndicatorSubject(pw.io.python.ConnectorSubject):
    deletions_enabled = False

    def run(self):
        while True:
            utc = datetime.now(timezone.utc).isoformat()
            for symbol in TICKERS:
                tkr = yf.Ticker(symbol)
                info = tkr.info or {}
                pe   = info.get("trailingPE")
                eps  = info.get("trailingEps")

                sma50 = rsi14 = None
                try:
                    hist = tkr.history(period="6mo", interval="1d")
                    if not hist.empty:
                        closes = hist["Close"]
                        sma50 = float(closes.tail(50).mean()) if len(closes) >= 50 else None
                        rsi14 = rsi(closes, length=14)
                except Exception:
                    pass

                row = {
                    "ticker": symbol,
                    "timestamp": utc,
                    "pe": pe,
                    "eps": eps,
                    "sma50": sma50,
                    "rsi14": rsi14,
                }
                self.next_json(row)
                append_csv(
                    symbol,
                    "indicators.csv",
                    ["timestamp", "pe", "eps", "sma50", "rsi14"],
                    [utc, pe, eps, sma50, rsi14],
                )
                print(f"✔ indicators.csv updated for {symbol}")
                print(f"{utc} | {symbol:<5} | PE {pe} | SMA50 {sma50} | RSI14 {rsi14}")
            time.sleep(POLL_TECH_SECS)

# ──────────────────────────────
# 7) PATHWAY READERS & RUN GRAPH
# ──────────────────────────────
prices_tbl = pw.io.python.read(PriceSubject(), schema=PriceSchema)
ind_tbl    = pw.io.python.read(IndicatorSubject(), schema=IndicatorSchema)

# attach sinks so the graph stays alive
_ = pw.debug.compute_and_print(prices_tbl, include_id=False)
_ = pw.debug.compute_and_print(ind_tbl, include_id=False)

print("\nStreaming live data and persisting CSVs… Ctrl‑C to stop.\n")

pw.run()