from flask import Flask, render_template, jsonify, request
import yfinance as yf
import pandas as pd
import threading, time, json, os, datetime
import requests
import xml.etree.ElementTree as ET

app = Flask(__name__)

# -----------------------------
# Global cache
# -----------------------------
DATA_FILE = 'sp500_data.json'
cached_data = []

# -----------------------------
# Utility functions
# -----------------------------
def load_cache():
    global cached_data
    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, 'r') as f:
                cached_data = json.load(f)
        except Exception as e:
            print(f"Error loading cache: {e}")

def save_cache():
    global cached_data
    try:
        with open(DATA_FILE, 'w') as f:
            json.dump(cached_data, f)
    except Exception as e:
        print(f"Error saving cache: {e}")

# -----------------------------
# Fetch S&P 500 tickers
# -----------------------------
def get_sp500_tickers():
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        response = requests.get(url, headers=headers, timeout=10)
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

# -----------------------------
# Fetch stock data for a ticker
# -----------------------------
def get_stock_data(ticker_obj):
    symbol = ticker_obj['Symbol']
    name = ticker_obj['Name']
    sector = ticker_obj.get('Sector', 'N/A')
    industry = ticker_obj.get('Industry', 'N/A')
    
    time.sleep(0.25)  # avoid rate limit
    
    try:
        stock = yf.Ticker(symbol)
        history = stock.history(period="210d")
        if history.empty or len(history) < 50:
            print(f"{symbol}: No price data found")
            return None

        close = history["Close"]
        current_price = close.iloc[-1]
        ma_50d = close.tail(50).mean() if len(close) >= 50 else None
        ma_200d = close.tail(200).mean() if len(close) >= 200 else None
        trend_strength = ((ma_50d / ma_200d) - 1) if (ma_50d and ma_200d) else None
        
        # Returns
        ret_5d = (close.iloc[-1] / close.iloc[-6] - 1) if len(close) >= 6 else None
        ret_1m = (close.iloc[-1] / close.iloc[-22] - 1) if len(close) >= 22 else None
        ret_6m = (close.iloc[-1] / close.iloc[-127] - 1) if len(close) >= 127 else None
        
        # PE & Market Cap
        pe_ratio = None
        market_cap = None
        try:
            if hasattr(stock, 'fast_info'):
                market_cap = stock.fast_info.market_cap
            info = stock.info
            pe_ratio = info.get("trailingPE")
            if not market_cap: 
                market_cap = info.get("marketCap")
        except:
            pass

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
        print(f"Error fetching {symbol}: {e}")
        return None

# -----------------------------
# Background fetcher
# -----------------------------
def fetch_sp500_background():
    global cached_data
    while True:
        print("Background fetch starting...")
        try:
            tickers = get_sp500_tickers()
            if not tickers:
                time.sleep(60*60)
                continue

            final_data = []
            for i, t in enumerate(tickers):
                data = get_stock_data(t)
                if data:
                    final_data.append(data)
                # Save partial cache every 20 stocks
                if i % 20 == 0 and final_data:
                    cached_data = final_data
                    save_cache()
                time.sleep(0.25)  # avoid Yahoo rate limit

            cached_data = final_data
            save_cache()
            print(f"Background fetch finished for {len(final_data)} stocks.")
        except Exception as e:
            print(f"Background fetch error: {e}")
        
        # Sleep 6 hours before next fetch
        time.sleep(6*60*60)

threading.Thread(target=fetch_sp500_background, daemon=True).start()

# -----------------------------
# Sector aggregation
# -----------------------------
def calculate_sector_data(data):
    if not data:
        return []
    
    df = pd.DataFrame(data)
    if df.empty or 'Sector' not in df.columns:
        return []

    sectors = []
    for sector_name, group in df.groupby("Sector"):
        if sector_name == "N/A":
            continue
        total_mcap = group['Market Cap'].sum()
        pe_group = group.dropna(subset=['P/E Ratio', 'Market Cap'])
        weighted_pe = (pe_group['P/E Ratio'] * pe_group['Market Cap']).sum() / pe_group['Market Cap'].sum() if not pe_group.empty and total_mcap>0 else None
        avg_50d = group['50D MA'].mean()
        avg_200d = group['200D MA'].mean()
        sectors.append({
            "Sector": sector_name,
            "Market Cap": total_mcap,
            "Weighted P/E": round(weighted_pe,2) if weighted_pe else None,
            "Avg 50D MA": round(avg_50d,2),
            "Avg 200D MA": round(avg_200d,2),
            "Stock Count": len(group)
        })
    return sectors

# -----------------------------
# Google News RSS
# -----------------------------
def fetch_google_news_rss(query):
    try:
        url = f"https://news.google.com/rss/search?q={query}&hl=en-US&gl=US&ceid=US:en"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        root = ET.fromstring(response.content)
        news_items = []
        for item in root.findall('./channel/item')[:10]:
            news_items.append({
                'title': item.find('title').text if item.find('title') is not None else 'No Title',
                'link': item.find('link').text if item.find('link') is not None else '#',
                'pubDate': item.find('pubDate').text if item.find('pubDate') is not None else '',
                'source': item.find('source').text if item.find('source') is not None else 'Google News'
            })
        return news_items
    except Exception as e:
        print(f"News fetch error: {e}")
        return []

# -----------------------------
# Routes
# -----------------------------
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/sector')
def sector_page():
    return render_template('sector.html')

@app.route('/api/data', methods=['GET','POST'])
def api_data():
    force_refresh = request.args.get('refresh') == 'true' or request.method == 'POST'
    if force_refresh:
        print("Force refresh requested — but background fetch handles updates.")
    return jsonify(cached_data)

@app.route('/api/sector_data')
def api_sector_data():
    return jsonify(calculate_sector_data(cached_data))

@app.route('/api/history/<symbol>')
def api_history(symbol):
    try:
        stock = yf.Ticker(symbol)
        hist = stock.history(period="1y")
        if hist.empty:
            return jsonify({'dates': [], 'prices': []})
        dates = hist.index.strftime('%Y-%m-%d').tolist()
        prices = hist['Close'].round(2).tolist()
        return jsonify({'symbol': symbol, 'dates': dates, 'prices': prices})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

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

# -----------------------------
# Run App
# -----------------------------
if __name__ == '__main__':
    load_cache()
    app.run(debug=True)
