from typing import Dict
from fastapi import FastAPI, Body, Request
import pandas as pd
import fitz  # PyMuPDF
# andy
from andy import run, FinancialDashboard, makeDashboard
import pathway as pw
import pandas as pd
import numpy as np
import json
import time
import yfinance as yf
import requests
from datetime import datetime, timedelta
from typing import List, Dict, Any
from sentence_transformers import SentenceTransformer
import streamlit as st
from bs4 import BeautifulSoup
import io
import os
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"] ,
    allow_headers=["*"]
)

@app.post("/followup/{ticker}")
def followup(ticker: str, body: Dict):
  query = body.get('query')
  dash = makeDashboard(ticker)
  dash.load_ticker(ticker)
  result = dash.query_ticker(query, ticker)
  return result


@app.get("/andy/{ticker}")
def andy(ticker: str):
  results = run(ticker)
  return results


@app.get("/analyze/{ticker}")
def analyze_stock(ticker: str):
  stock_df, pdf_paths = None, None
  # conditionally load stock data and news for them
  match ticker:
    case "RTX":
      stock_df = pd.read_csv('./RTX.csv')
      pdf_paths = [
        "./rtx_cbs.pdf",
        "./rtx_market.pdf",
        "./rtx_yahoo.pdf",
        "general_news.pdf",
        "general_news_2.pdf"
      ]
    case "AAPL":
      stock_df = pd.read_csv('./AAPL.csv')
      pdf_paths = [
        "./apple_1.pdf",
        "./apple_2.pdf",
        "./apple_3.pdf",
        "general_news.pdf",
        "general_news_2.pdf"
      ]
    case "TSLA":
      stock_df = pd.read_csv('./TSLA.csv')
      pdf_paths = [
        "./rtx_cbs.pdf",
        "./rtx_market.pdf",
        "./rtx_yahoo.pdf",
        "general_news.pdf",
        "general_news_2.pdf"
      ]

  # Clean and parse stock data
  stock_df.columns = stock_df.columns.str.strip().str.lower()
  stock_df['date'] = pd.to_datetime(stock_df['date'])
  stock_df = stock_df.sort_values('date')
  stock_df['close_shift_7'] = stock_df['close'].shift(7)
  stock_df['7d_return'] = (stock_df['close'] - stock_df['close_shift_7']) / stock_df['close_shift_7']

  # Get the most recent 7-day return
  latest_price = stock_df.iloc[-1][['date', 'close', '7d_return']]

  all_text = ""
  for path in pdf_paths:
    doc = fitz.open(path)
    for page in doc:
        all_text += page.get_text()
    doc.close()


  # Rule-based sentiment scoring
  positive_words = ["growth", "demand", "outperform", "strong", "boost", "rising", "interest", "positive", "gain"]
  negative_words = ["war", "conflict", "decline", "risk", "tension", "fear", "drop", "uncertain", "loss"]


  positive_score = sum(all_text.lower().count(word) for word in positive_words)
  negative_score = sum(all_text.lower().count(word) for word in negative_words)


  sentiment_score = positive_score - negative_score


  # Combine signal
  trend = latest_price['7d_return']
  if sentiment_score > 0 and trend > 0.01:
    decision = "BUY"
  elif sentiment_score < 0 and trend < -0.01:
    decision = "SELL"
  else:
    decision = "HOLD"

  # Final summary
  result = {
    "latest_price_date": str(latest_price['date'].date()),
    "latest_close_price": latest_price['close'],
    "7_day_return": round(trend, 4),
    "sentiment_score": sentiment_score,
    "decision": decision
  }

  print("📊 Analysis Result:")
  text_results = ''
  for k, v in result.items():
    print(f"{k}: {v}")
    text_results += f"{k}: {v}\n"

  
  print(text_results)
  return text_results
