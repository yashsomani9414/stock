from flask import Flask, render_template, jsonify, request
import yfinance as yf
import pandas as pd
import concurrent.futures
import time
import datetime
import json
import os
import requests

app = Flask(__name__)

DATA_FILE = 'sp500_data.json'
cached_data = []

def get_sp500_tickers():
    """Scrape S&P 500 tickers from Wikipedia."""
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        df = pd.read_html(response.text)[0]
        tickers = []
        for _, row in df.iterrows():
            symbol = row['Symbol'].replace('.', '-')
            tickers.append({
                'Symbol': symbol,
                'Name': row['Security'],
                'Sector': row['GICS Sector'],
                'Industry': row['GICS Sub-Industry']
            })
        return tickers
    except Exception as e:
        print(f"Error fetching tickers: {e}")
        return []

def get_stock_data(ticker_obj):
    """Fetch data for a single stock with basic info and 210d history."""
    symbol = ticker_obj['Symbol']
    try:
        stock = yf.Ticker(symbol)
        history = stock.history(period="210d")
        if history.empty or len(history) < 50:
            print(f"{symbol}: No price data found, symbol may be delisted.")
            return None

        close = history['Close']
        ma_50 = close.tail(50).mean() if len(close) >= 50 else None
        ma_200 = close.tail(200).mean() if len(close) >= 200 else None
        trend_strength = ((ma_50 / ma_200) - 1) if ma_50 and ma_200 else None

        # Returns
        def safe_return(n):
            return (close.iloc[-1] / close.iloc[-n-1] - 1) if len(close) > n else None

        ret_5d = safe_return(5)
        ret_1m = safe_return(22)
        ret_6m = safe_return(127)

        # Extended info
        pe_ratio = None
        market_cap = None
        try:
            if hasattr(stock, 'fast_info'):
                market_cap = stock.fast_info.market_cap
            info = stock.info
            pe_ratio = info.get('trailingPE')
            if not market_cap:
                market_cap = info.get('marketCap')
        except:
            pass

        data = {
            "Symbol": symbol,
            "Name": ticker_obj['Name'],
            "Price": round(close.iloc[-1],2),
            "50D MA": round(ma_50,2) if ma_50 else None,
            "200D MA": round(ma_200,2) if ma_200 else None,
            "Trend Strength": round(trend_strength*100,2) if trend_strength else None,
            "5D Return": round(ret_5d*100,2) if ret_5d else None,
            "1M Return": round(ret_1m*100,2) if ret_1m else None,
            "6M Return": round(ret_6m*100,2) if ret_6m else None,
            "P/E Ratio": round(pe_ratio,2) if pe_ratio else None,
            "Market Cap": market_cap,
            "Sector": ticker_obj['Sector'],
            "Industry": ticker_obj['Industry'],
            "LastUpdated": datetime.date.today().isoformat()
        }
        return data
    except Exception as e:
        print(f"Failed to get ticker '{symbol}' reason: {e}")
        return None

def fetch_all_data(force_refresh=False):
    """Fetch all S&P 500 data incrementally."""
    global cached_data
    today_str = datetime.date.today().isoformat()
    cached_data = []

    # Load existing cache
    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, 'r') as f:
                cached_data = json.load(f)
        except:
            cached_data = []

    existing_symbols = {item['Symbol']: item for item in cached_data}

    # Force refresh logic
    tickers = get_sp500_tickers()
    if not tickers:
        return cached_data

    # Determine which tickers need fetching
    to_fetch = []
    final_data = []
    for t in tickers:
        symbol = t['Symbol']
        if (symbol in existing_symbols and existing_symbols[symbol].get("LastUpdated") == today_str) and not force_refresh:
            final_data.append(existing_symbols[symbol])
        else:
            to_fetch.append(t)

    print(f"Fetching {len(to_fetch)} tickers...")
    for ticker_obj in to_fetch:
        # Retry up to 3 times
        data = None
        for i in range(3):
            data = get_stock_data(ticker_obj)
            if data:
                break
            time.sleep(2 ** i)  # exponential backoff
        if data:
            final_data.append(data)
        elif ticker_obj['Symbol'] in existing_symbols:
            final_data.append(existing_symbols[ticker_obj['Symbol']])
        time.sleep(0.5)  # avoid API rate limit

        # Save partial cache immediately
        try:
            with open(DATA_FILE, 'w') as f:
                json.dump(final_data, f)
        except Exception as e:
            print(f"Error saving cache: {e}")

    # Sort by symbol
    final_data.sort(key=lambda x: x['Symbol'])
    cached_data = final_data
    return final_data

def calculate_sector_data(data):
    if not data:
        return []
    df = pd.DataFrame(data)
    sectors = []
    for sector, group in df.groupby('Sector'):
        if sector == "N/A":
            continue
        total_mcap = group['Market Cap'].sum()
        pe_group = group.dropna(subset=['P/E Ratio','Market Cap'])
        weighted_pe = (pe_group['P/E Ratio'] * pe_group['Market Cap']).sum() / pe_group['Market Cap'].sum() if not pe_group.empty else None
        sectors.append({
            "Sector": sector,
            "Market Cap": total_mcap,
            "Weighted P/E": round(weighted_pe,2) if weighted_pe else None,
            "Avg 50D MA": round(group['50D MA'].mean(),2),
            "Avg 200D MA": round(group['200D MA'].mean(),2),
            "Stock Count": len(group)
        })
    return sectors

# --- Routes ---
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/sector')
def sector_page():
    return render_template('sector.html')

@app.route('/api/data', methods=['GET','POST'])
def api_data():
    force_refresh = request.args.get('refresh')=='true' or request.method=='POST'
    data = fetch_all_data(force_refresh=force_refresh)
    return jsonify(data)

@app.route('/api/sector_data')
def api_sector_data():
    data = fetch_all_data(force_refresh=False)
    return jsonify(calculate_sector_data(data))

@app.route('/api/history/<symbol>')
def api_history(symbol):
    try:
        stock = yf.Ticker(symbol)
        hist = stock.history(period="1y")
        dates = hist.index.strftime('%Y-%m-%d').tolist()
        prices = hist['Close'].round(2).tolist()
        return jsonify({'symbol': symbol, 'dates': dates, 'prices': prices})
    except:
        return jsonify({'symbol': symbol, 'dates': [], 'prices': []})

# --- News ---
def fetch_google_news_rss(query):
    try:
        url = f"https://news.google.com/rss/search?q={query}&hl=en-US&gl=US&ceid=US:en"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        import xml.etree.ElementTree as ET
        root = ET.fromstring(response.content)
        news_items = []
        for item in root.findall('./channel/item')[:10]:
            title = item.find('title').text if item.find('title') else 'No Title'
            link = item.find('link').text if item.find('link') else '#'
            pubDate = item.find('pubDate').text if item.find('pubDate') else ''
            source_elem = item.find('source')
            source = source_elem.text if source_elem is not None else 'Google News'
            news_items.append({'title':title,'link':link,'pubDate':pubDate,'source':source})
        return news_items
    except Exception as e:
        print(f"News fetch error: {e}")
        return []

@app.route('/market-news')
def market_news_page():
    return render_template('market_news.html')

@app.route('/news/<symbol>')
def stock_news_page(symbol):
    return render_template('stock_news.html', symbol=symbol)

@app.route('/api/news/market')
def api_market_news():
    return jsonify(fetch_google_news_rss("Stock Market Economy"))

@app.route('/api/news/<symbol>')
def api_stock_news(symbol):
    return jsonify(fetch_google_news_rss(f"{symbol} stock"))

if __name__ == '__main__':
    app.run(debug=True)
