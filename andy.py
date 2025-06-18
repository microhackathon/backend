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

class StockDataIngester:
  """Ingests real-time stock data, news, and market info"""
  
  def __init__(self):
      self.embedder = SentenceTransformer('all-MiniLM-L6-v2')
      
  def get_stock_data(self, ticker: str):
      """Get real-time stock data using yfinance"""
      try:
          stock = yf.Ticker(ticker)
          
          # Get current info
          info = stock.info
          
          # Get recent price data
          hist = stock.history(period="5d")
          current_price = hist['Close'].iloc[-1] if not hist.empty else 0
          
          # Calculate basic metrics
          price_change = hist['Close'].iloc[-1] - hist['Close'].iloc[-2] if len(hist) >= 2 else 0
          percent_change = (price_change / hist['Close'].iloc[-2] * 100) if len(hist) >= 2 else 0
          
          stock_data = {
              "ticker": ticker.upper(),
              "current_price": round(current_price, 2),
              "price_change": round(price_change, 2),
              "percent_change": round(percent_change, 2),
              "volume": hist['Volume'].iloc[-1] if not hist.empty else 0,
              "market_cap": info.get('marketCap', 'N/A'),
              "pe_ratio": info.get('trailingPE', 'N/A'),
              "company_name": info.get('longName', ticker.upper()),
              "sector": info.get('sector', 'Unknown'),
              "industry": info.get('industry', 'Unknown'),
              "timestamp": datetime.now().isoformat(),
              "data_type": "stock_data"
          }
          
          return stock_data
          
      except Exception as e:
          print(f"Error fetching stock data for {ticker}: {e}")
          return None
  
  def get_stock_news(self, ticker: str, limit: int = 5):
      """Get recent news for a stock ticker"""
      try:
          stock = yf.Ticker(ticker)
          news = stock.news
          
          news_data = []
          for article in news[:limit]:
              news_item = {
                  "ticker": ticker.upper(),
                  "title": article.get('title', ''),
                  "summary": article.get('summary', ''),
                  "link": article.get('link', ''),
                  "publisher": article.get('publisher', ''),
                  "publish_time": datetime.fromtimestamp(article.get('providerPublishTime', time.time())).isoformat(),
                  "timestamp": datetime.now().isoformat(),
                  "data_type": "news"
              }
              news_data.append(news_item)
          
          return news_data
          
      except Exception as e:
          print(f"Error fetching news for {ticker}: {e}")
          return []
  
  def get_industry_context(self, sector: str, industry: str):
      """Get industry and market context"""
      industry_data = {
          "sector": sector,
          "industry": industry,
          "context": f"Analysis of {industry} sector within {sector} industry",
          "timestamp": datetime.now().isoformat(),
          "data_type": "industry_context"
      }
      
      return industry_data
  
  def get_market_overview(self):
      """Get overall market indicators"""
      try:
          # Get major indices
          indices = {
              "^GSPC": "S&P 500",
              "^DJI": "Dow Jones",
              "^IXIC": "NASDAQ"
          }
          
          market_data = []
          for symbol, name in indices.items():
              try:
                  ticker = yf.Ticker(symbol)
                  hist = ticker.history(period="2d")
                  
                  if not hist.empty:
                      current = hist['Close'].iloc[-1]
                      previous = hist['Close'].iloc[-2] if len(hist) >= 2 else current
                      change = current - previous
                      percent_change = (change / previous * 100) if previous != 0 else 0
                      
                      market_data.append({
                          "index_name": name,
                          "symbol": symbol,
                          "current_value": round(current, 2),
                          "change": round(change, 2),
                          "percent_change": round(percent_change, 2),
                          "timestamp": datetime.now().isoformat(),
                          "data_type": "market_overview"
                      })
              except:
                  continue
          
          return market_data
          
      except Exception as e:
          print(f"Error fetching market overview: {e}")
          return []


# ===============================
# PHASE 3: PATHWAY ETL PIPELINE
# ===============================


class PathwayETLPipeline:
  """Pathway-based ETL pipeline for financial data"""
  
  def __init__(self):
      self.ingester = StockDataIngester()
      self.embedder = SentenceTransformer('all-MiniLM-L6-v2')
      self.data_store = []
      self.embeddings_store = []
      
  def ingest_ticker_data(self, ticker: str):
      """Complete ETL pipeline for a ticker"""
      print(f"🔄 Starting ETL pipeline for {ticker}...")
      
      all_data = []
      
      # 1. Extract Stock Data
      print("📊 Extracting stock data...")
      stock_data = self.ingester.get_stock_data(ticker)
      if stock_data:
          all_data.append(stock_data)
      
      # 2. Extract News Data
      print("📰 Extracting news data...")
      news_data = self.ingester.get_stock_news(ticker)
      all_data.extend(news_data)
      
      # 3. Extract Industry Context
      if stock_data:
          print("🏭 Extracting industry context...")
          industry_data = self.ingester.get_industry_context(
              stock_data.get('sector', 'Unknown'),
              stock_data.get('industry', 'Unknown')
          )
          all_data.append(industry_data)
      
      # 4. Extract Market Overview
      print("🌍 Extracting market overview...")
      market_data = self.ingester.get_market_overview()
      all_data.extend(market_data)
      
      # 5. Transform to Pathway Tables
      print("⚙️ Transforming data with Pathway...")
      pathway_tables = self.transform_to_pathway(all_data, ticker)
      
      # 6. Load to Vector Store
      print("🗂️ Loading to vector store...")
      self.load_to_vector_store(all_data, ticker)
      
      print(f"✅ ETL pipeline complete for {ticker}!")
      return pathway_tables, all_data
  
  def transform_to_pathway(self, data: List[Dict], ticker: str):
      """Transform data into Pathway tables"""
      
      # Separate data by type
      stock_data = [d for d in data if d.get('data_type') == 'stock_data']
      news_data = [d for d in data if d.get('data_type') == 'news']
      market_data = [d for d in data if d.get('data_type') == 'market_overview']
      
      tables = {}
      
      # Stock Data Table
      if stock_data:
          stock_rows = []
          for item in stock_data:
              stock_rows.append((
                  item['ticker'],
                  item['company_name'],
                  float(item['current_price']),
                  float(item['price_change']),
                  float(item['percent_change']),
                  str(item['volume']),
                  str(item['market_cap']),
                  str(item['pe_ratio']),
                  item['sector'],
                  item['industry'],
                  item['timestamp']
              ))
          
          tables['stock'] = pw.debug.table_from_rows(
              schema=pw.schema_from_types(
                  ticker=str, company_name=str, current_price=float,
                  price_change=float, percent_change=float, volume=str,
                  market_cap=str, pe_ratio=str, sector=str, industry=str, timestamp=str
              ),
              rows=stock_rows
          )
      
      # News Data Table
      if news_data:
          news_rows = []
          for item in news_data:
              news_rows.append((
                  item['ticker'],
                  item['title'],
                  item['summary'],
                  item['publisher'],
                  item['publish_time'],
                  item['timestamp']
              ))
          
          tables['news'] = pw.debug.table_from_rows(
              schema=pw.schema_from_types(
                  ticker=str, title=str, summary=str,
                  publisher=str, publish_time=str, timestamp=str
              ),
              rows=news_rows
          )
      
      # Market Data Table
      if market_data:
          market_rows = []
          for item in market_data:
              market_rows.append((
                  item['index_name'],
                  item['symbol'],
                  float(item['current_value']),
                  float(item['change']),
                  float(item['percent_change']),
                  item['timestamp']
              ))
          
          tables['market'] = pw.debug.table_from_rows(
              schema=pw.schema_from_types(
                  index_name=str, symbol=str, current_value=float,
                  change=float, percent_change=float, timestamp=str
              ),
              rows=market_rows
          )
      
      return tables
  
  def load_to_vector_store(self, data: List[Dict], ticker: str):
      """Load data to vector store for RAG"""
      
      for item in data:
          # Create searchable text based on data type
          if item.get('data_type') == 'stock_data':
              text_content = f"{item['company_name']} ({item['ticker']}) - Current Price: ${item['current_price']}, Change: {item['percent_change']:.2f}%, Sector: {item['sector']}, Industry: {item['industry']}"
              
          elif item.get('data_type') == 'news':
              text_content = f"{item['title']} - {item['summary']} (Publisher: {item['publisher']})"
              
          elif item.get('data_type') == 'market_overview':
              text_content = f"{item['index_name']} Index - Current: {item['current_value']}, Change: {item['percent_change']:.2f}%"
              
          elif item.get('data_type') == 'industry_context':
              text_content = f"Industry Analysis: {item['industry']} sector within {item['sector']} industry"
              
          else:
              text_content = str(item)
          
          # Generate embedding
          embedding = self.embedder.encode(text_content)
          
          # Store in vector database
          vector_item = {
              "id": f"{ticker}_{item.get('data_type', 'unknown')}_{len(self.embeddings_store)}",
              "ticker": ticker,
              "text": text_content,
              "embedding": embedding,
              "metadata": item,
              "timestamp": datetime.now().isoformat()
          }
          
          self.embeddings_store.append(vector_item)
          self.data_store.append(item)


# ===============================
# PHASE 4: VECTOR RAG SYSTEM
# ===============================


class FinancialRAGSystem:
  """RAG system for financial queries"""
  
  def __init__(self, etl_pipeline: PathwayETLPipeline):
      self.pipeline = etl_pipeline
      self.embedder = etl_pipeline.embedder
  
  def query(self, question: str, ticker: str = None, top_k: int = 5):
      """Query the financial knowledge base"""
      print(f"🔍 Searching for: '{question}'")
      
      if not self.pipeline.embeddings_store:
          return {"answer": "No data available. Please load ticker data first.", "sources": []}
      
      # Filter by ticker if specified
      search_space = self.pipeline.embeddings_store
      if ticker:
          search_space = [item for item in search_space if item['ticker'].upper() == ticker.upper()]
      
      if not search_space:
          return {"answer": f"No data available for ticker {ticker}.", "sources": []}
      
      # Get query embedding
      query_embedding = self.embedder.encode(question)
      
      # Calculate similarities
      similarities = []
      for item in search_space:
          similarity = np.dot(query_embedding, item["embedding"]) / (
              np.linalg.norm(query_embedding) * np.linalg.norm(item["embedding"])
          )
          similarities.append((similarity, item))
      
      # Sort and get top results
      similarities.sort(key=lambda x: x[0], reverse=True)
      top_results = [item[1] for item in similarities[:top_k] if item[0] > 0.3]
      
      # Generate answer
      answer = self.generate_answer(question, top_results)
      
      return {
          "answer": answer,
          "sources": top_results,
          "num_sources": len(top_results)
      }
  
  def generate_answer(self, question: str, sources: List[Dict]):
      """Generate answer from sources"""
      if not sources:
          return "I couldn't find relevant information to answer your question."
      
      # Simple answer generation based on question type
      question_lower = question.lower()
      
      if "price" in question_lower or "stock" in question_lower:
          # Find stock data
          stock_sources = [s for s in sources if s['metadata'].get('data_type') == 'stock_data']
          if stock_sources:
              stock_info = stock_sources[0]['metadata']
              return f"{stock_info['company_name']} ({stock_info['ticker']}) is currently trading at ${stock_info['current_price']}, which is {stock_info['percent_change']:+.2f}% from the previous close. The stock operates in the {stock_info['industry']} industry within the {stock_info['sector']} sector."
      
      elif "news" in question_lower or "latest" in question_lower:
          # Find news data
          news_sources = [s for s in sources if s['metadata'].get('data_type') == 'news']
          if news_sources:
              news_items = [f"• {item['metadata']['title']}" for item in news_sources[:3]]
              return f"Here are the latest news items:\n" + "\n".join(news_items)
      
      elif "market" in question_lower or "index" in question_lower:
          # Find market data
          market_sources = [s for s in sources if s['metadata'].get('data_type') == 'market_overview']
          if market_sources:
              market_info = []
              for item in market_sources:
                  data = item['metadata']
                  market_info.append(f"{data['index_name']}: {data['current_value']} ({data['percent_change']:+.2f}%)")
              return f"Current market overview:\n" + "\n".join(market_info)
      
      # Default: return summary of top source
      top_source = sources[0]
      return f"Based on the available data: {top_source['text'][:200]}..."


# ===============================
# PHASE 5: FRONTEND INTERFACE
# ===============================


class FinancialDashboard:
  """Simple dashboard interface"""
  
  def __init__(self, ticker):
      self.etl = PathwayETLPipeline()
      self.rag = FinancialRAGSystem(self.etl)
      self.loaded_tickers = set()
      self.ticker = ticker
  
  def load_ticker(self, ticker: str):
      """Load ticker data into the system"""
      if ticker.upper() in self.loaded_tickers:
          print(f"📊 {ticker.upper()} already loaded!")
          return
      
      print(f"🚀 Loading data for {ticker.upper()}...")
      
      try:
          tables, data = self.etl.ingest_ticker_data(ticker)
          self.loaded_tickers.add(ticker.upper())
          
          print(f"✅ Successfully loaded {ticker.upper()}!")
          print(f"📈 Stock data: {len([d for d in data if d.get('data_type') == 'stock_data'])} items")
          print(f"📰 News items: {len([d for d in data if d.get('data_type') == 'news'])} items")
          print(f"🌍 Market data: {len([d for d in data if d.get('data_type') == 'market_overview'])} items")
          
          return tables, data
          
      except Exception as e:
          print(f"❌ Error loading {ticker}: {e}")
          return None, None
  
  def query_ticker(self, question: str, ticker: str = None):
      """Query the loaded ticker data"""
      result = self.rag.query(question, self.ticker)
      
      print(f"\n🤖 Answer: {result['answer']}")
      print(f"📚 Based on {result['num_sources']} sources")
      
      return (f"{result['answer']}\n📚 Based on {result['num_sources']} sources")
  
  def run_demo(self):
      """Run interactive demo"""
      print("\n" + "="*80)
      print("🏦 FINANCIAL TICKER INTELLIGENCE SYSTEM")
      print("="*80)
      
      # Demo with AAPL
      print("\n🍎 Loading Apple Inc. (AAPL) data...")
      self.load_ticker(self.ticker)
      
      # Demo queries
      queries = [
          "What is the current stock price?",
          "What are the latest news about this company?",
          "How is the overall market performing?",
          "What industry does this company operate in?",
      ]
      
      result = []
      for query in queries:
          print(f"\n" + "="*50)
          print(f"🔍 Query: {query}")
          print("="*50)
          print_res = self.query_ticker(query, self.ticker)
          result.append(print_res)
          time.sleep(1)  # Pause for readability
      
      print("\n🎉 Demo complete!")
      return result


# ===============================
# PHASE 6: MAIN EXECUTION
# ===============================


def run(ticker):
  """Main execution function"""
  
  print("🚀 Starting Financial Ticker Intelligence System...")
  
  # Initialize dashboard
  dashboard = FinancialDashboard(ticker)
  
  # Run demo
  results = dashboard.run_demo()
  
  return results

def makeDashboard(ticker):
    return FinancialDashboard(ticker)


# # Execute the main function
# dashboard_instance = main()


# ===============================
# INTERACTIVE PLAYGROUND
# ===============================


print("\n" + "="*80)
print("🎮 INTERACTIVE PLAYGROUND")
print("Available functions:")
print("1. dashboard_instance.load_ticker('TICKER')")
print("2. dashboard_instance.query_ticker('question', 'TICKER')")
print("="*80)


# def load_ticker_interactive(ticker: str):
#   """Interactive ticker loading"""
#   return dashboard_instance.load_ticker(ticker)


# def query_interactive(question: str, ticker: str = None):
#   """Interactive querying"""
#   return dashboard_instance.query_ticker(question, ticker)


print("\n💡 Example Usage:")
print("load_ticker_interactive('TSLA')")
print("query_interactive('What is the current stock price?', 'TSLA')")
print("query_interactive('How is the market performing today?')")


print("\n🎯 Ready for your financial intelligence queries!")
