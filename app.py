from flask import Flask, render_template, jsonify
import yfinance as yf
import pandas as pd
import concurrent.futures
import time
import datetime

app = Flask(__name__)

# Cache to store data temporarily so we don't fetch on every refresh immediately
# In a real app, use Redis or similar. Here, a global var is fine for a demo.
cached_data = []
last_fetch_time = 0
CACHE_DURATION = 300  # 5 minutes

def get_sp500_tickers():
    """Scrape S&P 500 tickers from Wikipedia."""
    try:
        import requests
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        table = pd.read_html(response.text)
        df = table[0]
        # Get Symbol, Security (Name), Sector, Industry
        tickers = []
        for index, row in df.iterrows():
            symbol = row['Symbol'].replace('.', '-')
            name = row['Security']
            sector = row['GICS Sector']
            industry = row['GICS Sub-Industry']
            tickers.append({
                'Symbol': symbol, 
                'Name': name,
                'Sector': sector,
                'Industry': industry
            })
        return tickers
    except Exception as e:
        print(f"Error fetching tickers: {e}")
        return []

# Configure session with retries - REMOVED to avoid compatibility issues
import random

def get_stock_data(ticker_obj):
    """Fetch data for a single stock."""
    symbol = ticker_obj['Symbol']
    name = ticker_obj['Name']
    sector = ticker_obj.get('Sector', 'N/A')
    industry = ticker_obj.get('Industry', 'N/A')
    
    # Add sleep to rate limit
    time.sleep(0.25)  # Increased from 0.1s to reduce rate limits
    
    try:
        stock = yf.Ticker(symbol)
        
        # Fetch history
        try:
            history = stock.history(period="210d")
        except Exception:
            return None
        
        if history.empty or len(history) < 50:
            return None

        close = history["Close"]
        current_price = close.iloc[-1]
        
        # MAs
        ma_50d = close.tail(50).mean() if len(close) >= 50 else None
        ma_200d = close.tail(200).mean() if len(close) >= 200 else None
        
        # Trend Strength
        trend_strength = ((ma_50d / ma_200d) - 1) if (ma_50d and ma_200d) else None
        
        # Returns
        try: ret_5d = (close.iloc[-1] / close.iloc[-6] - 1) if len(close) >= 6 else None
        except: ret_5d = None
        try: ret_1m = (close.iloc[-1] / close.iloc[-22] - 1) if len(close) >= 22 else None
        except: ret_1m = None
        try: ret_6m = (close.iloc[-1] / close.iloc[-127] - 1) if len(close) >= 127 else None
        except: ret_6m = None
        
        # Extended Data (PE, Market Cap)
        pe_ratio = None
        market_cap = None
        
        try:
            # Try fast_info first for MktCap
            if hasattr(stock, 'fast_info'):
                market_cap = stock.fast_info.market_cap
            
            # INFO request is what usually gets rate limited. 
            # We skip it if we have basic data, or try carefully.
            # Ideally we only call .info if we really need PE.
            info = stock.info
            pe_ratio = info.get("trailingPE")
            if not market_cap: 
                market_cap = info.get("marketCap")
        except:
            pass # It's okay if we miss PE/Cap, we have the rest.

        return {
            "Symbol": symbol,
            "Name": name,
            "Price": round(current_price, 2),
            "50D MA": round(ma_50d, 2) if ma_50d else None,
            "200D MA": round(ma_200d, 2) if ma_200d else None,
            "Trend Strength": round(trend_strength * 100, 2) if trend_strength is not None else None,
            "5D Return": round(ret_5d * 100, 2) if ret_5d is not None else None,
            "1M Return": round(ret_1m * 100, 2) if ret_1m is not None else None,
            "6M Return": round(ret_6m * 100, 2) if ret_6m is not None else None,
            "P/E Ratio": round(pe_ratio, 2) if pe_ratio else None,
            "Market Cap": market_cap,
            "Sector": sector,
            "Industry": industry,
            "LastUpdated": datetime.date.today().isoformat()
        }

    except Exception as e:
        print(f"Error fetching data for {symbol}: {e}")
        return None

import json
import os

DATA_FILE = 'sp500_data.json'

def fetch_all_data(force_refresh=False):
    """
    Fetch all S&P 500 data in small batches with caching and retry.
    """
    global cached_data
    
    # Load existing cache from memory or file
    existing_data = {}
    if cached_data:
        existing_data = {item['Symbol']: item for item in cached_data}
    elif os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, 'r') as f:
                loaded = json.load(f)
                existing_data = {item['Symbol']: item for item in loaded}
                cached_data = loaded
        except:
            pass

    today_str = datetime.date.today().isoformat()
    
    # Use cache if available and not forcing refresh
    if not force_refresh:
        if cached_data:
            print("Using in-memory cache.")
            return cached_data
        if existing_data:
            print("Using file cache.")
            return list(existing_data.values())
    
    tickers = get_sp500_tickers()
    if not tickers:
        return list(existing_data.values())  # fallback to cache

    # Determine which tickers need fetching
    tickers_to_fetch = []
    final_data = []
    for t in tickers:
        symbol = t['Symbol']
        if symbol in existing_data and existing_data[symbol].get("LastUpdated") == today_str:
            final_data.append(existing_data[symbol])
        else:
            tickers_to_fetch.append(t)

    print(f"Skipping {len(final_data)} stocks already updated today.")
    print(f"Fetching data for {len(tickers_to_fetch)} stocks from Yahoo Finance...")

    # Helper: fetch with retry
    def fetch_with_retry(ticker_obj, retries=3):
        for i in range(retries):
            data = get_stock_data(ticker_obj)
            if data:
                return data
            time.sleep(2 ** i)  # exponential backoff
        return None

    # Small batch fetching
    batch_size = 5
    for i in range(0, len(tickers_to_fetch), batch_size):
        batch = tickers_to_fetch[i:i+batch_size]
        print(f"Fetching batch {i//batch_size + 1} ({len(batch)} tickers)")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            future_to_stock = {executor.submit(fetch_with_retry, t): t for t in batch}
            for future in concurrent.futures.as_completed(future_to_stock):
                ticker_obj = future_to_stock[future]
                symbol = ticker_obj['Symbol']
                try:
                    res = future.result()
                    if res:
                        final_data.append(res)
                    else:
                        # Use cached data if fetch fails
                        if symbol in existing_data:
                            print(f"Fetch failed for {symbol}, using cached data.")
                            final_data.append(existing_data[symbol])
                        else:
                            print(f"No data for {symbol}.")
                except Exception as e:
                    print(f"Exception for {symbol}: {e}")
                    if symbol in existing_data:
                        final_data.append(existing_data[symbol])

        # Short delay between batches to avoid rate limits
        time.sleep(1)

        # Save partial results to file after each batch
        try:
            with open(DATA_FILE, 'w') as f:
                json.dump(final_data, f)
            print(f"Partial data saved ({len(final_data)} stocks).")
        except Exception as e:
            print(f"Error saving cache file: {e}")

    # Sort final data
    final_data.sort(key=lambda x: x['Symbol'])
    cached_data = final_data

    return final_data


def calculate_sector_data(data):
    """Aggregate data by sector."""
    if not data:
        return []
    
    df = pd.DataFrame(data)
    if df.empty or 'Sector' not in df.columns:
        return []
        
    # Group by Sector
    sectors = []
    for sector_name, group in df.groupby("Sector"):
        if sector_name == "N/A":
            continue
            
        total_mcap = group['Market Cap'].sum()
        
        # Weighted P/E: Sum(PE * Mcap) / Sum(Mcap) (only for stocks with positive PE)
        pe_group = group.dropna(subset=['P/E Ratio', 'Market Cap'])
        if not pe_group.empty and total_mcap > 0:
            weighted_pe = (pe_group['P/E Ratio'] * pe_group['Market Cap']).sum() / pe_group['Market Cap'].sum()
        else:
            weighted_pe = None
            
        # Avg MAs (Simple Average for now, or could be weighted)
        avg_50d = group['50D MA'].mean()
        avg_200d = group['200D MA'].mean()
        
        sectors.append({
            "Sector": sector_name,
            "Market Cap": total_mcap,
            "Weighted P/E": round(weighted_pe, 2) if weighted_pe else None,
            "Avg 50D MA": round(avg_50d, 2),
            "Avg 200D MA": round(avg_200d, 2),
            "Stock Count": len(group)
        })
    
    return sectors

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/sector')
def sector_page():
    return render_template('sector.html')

@app.route('/api/data', methods=['GET', 'POST'])
def api_data():
    from flask import request
    force_refresh = request.args.get('refresh') == 'true' or request.method == 'POST'
    data = fetch_all_data(force_refresh=force_refresh)
    return jsonify(data)

@app.route('/api/sector_data')
def api_sector_data():
    data = fetch_all_data(force_refresh=False) # Use cached if available
    sector_data = calculate_sector_data(data)
    return jsonify(sector_data)

@app.route('/api/history/<symbol>')
def api_history(symbol):
    """Fetch 1 year daily history for a stock."""
    try:
        stock = yf.Ticker(symbol)
        hist = stock.history(period="1y")
        if hist.empty:
            return jsonify({'dates': [], 'prices': []})
        
        # Format dates as YYYY-MM-DD
        dates = hist.index.strftime('%Y-%m-%d').tolist()
        prices = hist['Close'].round(2).tolist()
        
        return jsonify({
            'symbol': symbol,
            'dates': dates,
            'prices': prices
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/market-news')
def market_news_page():
    return render_template('market_news.html')

@app.route('/news/<symbol>')
def stock_news_page(symbol):
    return render_template('stock_news.html', symbol=symbol)

def fetch_google_news_rss(query):
    """Fetch news from Google News RSS feed."""
    try:
        # q={query}
        # hl=en-US&gl=US&ceid=US:en -> English, US location
        url = f"https://news.google.com/rss/search?q={query}&hl=en-US&gl=US&ceid=US:en"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        
        import xml.etree.ElementTree as ET
        root = ET.fromstring(response.content)
        
        news_items = []
        # Iterate over <item> in <channel>
        # Limit to 10 items
        for item in root.findall('./channel/item')[:10]:
            title = item.find('title').text if item.find('title') is not None else 'No Title'
            link = item.find('link').text if item.find('link') is not None else '#'
            pubDate = item.find('pubDate').text if item.find('pubDate') is not None else ''
            
            # Source is usually in <source> tag or part of title "Title - Source"
            source_elem = item.find('source')
            source = source_elem.text if source_elem is not None else 'Google News'
            
            # Description often contains HTML, we might just use Title for cleanness or strip it.
            # description = item.find('description').text
            
            news_items.append({
                'title': title,
                'link': link,
                'pubDate': pubDate,
                'source': source
            })
            
        return news_items
    except Exception as e:
        print(f"Error fetching Google News for {query}: {e}")
        return []

@app.route('/api/news/market')
def api_market_news():
    """Fetch top market news."""
    # Query: "Stock Market" or "Economy"
    news = fetch_google_news_rss("Stock Market Economy")
    return jsonify(news)

@app.route('/api/news/<symbol>')
def api_stock_news(symbol):
    """Fetch news for a specific stock."""
    # Query: "{Symbol} stock" helps narrow it down
    news = fetch_google_news_rss(f"{symbol} stock")
    return jsonify(news)


if __name__ == '__main__':
    app.run(debug=True)
