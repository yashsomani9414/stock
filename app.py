from flask import Flask, render_template, jsonify, request
import yfinance as yf
import pandas as pd
import concurrent.futures
import time
import datetime
import json
import os
import requests
import xml.etree.ElementTree as ET

app = Flask(__name__)

# Cache
cached_data = []
CACHE_DURATION = 300  # seconds
DATA_FILE = 'sp500_data.json'

def get_sp500_tickers():
    """Scrape S&P 500 tickers from Wikipedia."""
    try:
        headers = {
            "User-Agent": "Mozilla/5.0"
        }
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

def get_stock_data(ticker_obj, retries=3):
    """Fetch 210d stock data with retry and exponential backoff."""
    symbol = ticker_obj['Symbol']
    name = ticker_obj['Name']
    sector = ticker_obj.get('Sector', 'N/A')
    industry = ticker_obj.get('Industry', 'N/A')

    for attempt in range(retries):
        try:
            stock = yf.Ticker(symbol)
            hist = stock.history(period="210d")
            if hist.empty or len(hist) < 50:
                raise ValueError("Not enough history")

            close = hist["Close"]
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
        except (ValueError, json.JSONDecodeError) as e:
            print(f"Temporary error fetching {symbol} (attempt {attempt+1}): {e}")
            time.sleep(2 + attempt)
        except Exception as e:
            print(f"Error fetching {symbol}: {e}")
            break

    print(f"Failed to fetch {symbol} after {retries} attempts.")
    return None

def fetch_all_data(force_refresh=False):
    global cached_data

    existing_data = {}
    if cached_data:
        existing_data = {item['Symbol']: item for item in cached_data}
    elif os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, 'r') as f:
                loaded = json.load(f)
                cached_data = loaded
                existing_data = {item['Symbol']: item for item in loaded}
        except:
            pass

    today_str = datetime.date.today().isoformat()

    if not force_refresh:
        if cached_data:
            print("Using in-memory cache.")
            return cached_data
        if existing_data:
            print("Using file cache.")
            return list(existing_data.values())

    tickers = get_sp500_tickers()
    if not tickers:
        return list(existing_data.values())

    tickers_to_fetch = []
    final_data = []
    for t in tickers:
        symbol = t['Symbol']
        if symbol in existing_data and existing_data[symbol].get("LastUpdated") == today_str:
            final_data.append(existing_data[symbol])
        else:
            tickers_to_fetch.append(t)

    print(f"Skipping {len(final_data)} stocks already updated today.")
    print(f"Fetching data for {len(tickers_to_fetch)} stocks...")

    def fetch_with_retry(ticker_obj):
        return get_stock_data(ticker_obj, retries=3)

    batch_size = 2  # small to avoid worker timeout
    for i in range(0, len(tickers_to_fetch), batch_size):
        batch = tickers_to_fetch[i:i+batch_size]
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            future_to_stock = {executor.submit(fetch_with_retry, t): t for t in batch}
            for future in concurrent.futures.as_completed(future_to_stock):
                ticker_obj = future_to_stock[future]
                symbol = ticker_obj['Symbol']
                try:
                    res = future.result()
                    if res:
                        final_data.append(res)
                    elif symbol in existing_data:
                        final_data.append(existing_data[symbol])
                except:
                    if symbol in existing_data:
                        final_data.append(existing_data[symbol])
        time.sleep(1)
        # Save partial results
        try:
            with open(DATA_FILE, 'w') as f:
                json.dump(final_data, f)
        except:
            pass

    final_data.sort(key=lambda x: x['Symbol'])
    cached_data = final_data
    return final_data

def calculate_sector_data(data):
    if not data:
        return []
    df = pd.DataFrame(data)
    sectors = []
    for sector_name, group in df.groupby("Sector"):
        if sector_name == "N/A":
            continue
        total_mcap = group['Market Cap'].sum()
        pe_group = group.dropna(subset=['P/E Ratio', 'Market Cap'])
        weighted_pe = (pe_group['P/E Ratio'] * pe_group['Market Cap']).sum() / pe_group['Market Cap'].sum() if not pe_group.empty else None
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

# -------------------- Routes --------------------
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/sector')
def sector_page():
    return render_template('sector.html')

@app.route('/api/data', methods=['GET', 'POST'])
def api_data():
    force_refresh = request.args.get('refresh') == 'true' or request.method == 'POST'
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
        if hist.empty:
            return jsonify({'dates': [], 'prices': []})
        return jsonify({
            'symbol': symbol,
            'dates': hist.index.strftime('%Y-%m-%d').tolist(),
            'prices': hist['Close'].round(2).tolist()
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
        print(f"Error fetching Google News for {query}: {e}")
        return []

@app.route('/api/news/market')
def api_market_news():
    news = fetch_google_news_rss("Stock Market Economy")
    return jsonify(news)

@app.route('/api/news/<symbol>')
def api_stock_news(symbol):
    news = fetch_google_news_rss(f"{symbol} stock")
    return jsonify(news)

# -------------------- Run App --------------------
if __name__ == '__main__':
    app.run(debug=True)
