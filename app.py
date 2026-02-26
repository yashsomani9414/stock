from flask import Flask, render_template, jsonify, request
import pandas as pd
import datetime
import json
import os
import requests
import time
import yfinance as yf
from bs4 import BeautifulSoup
import threading
import re

app = Flask(__name__)


# Global state for background refresh
refresh_status = {
    "is_running": False,
    "current": 0,
    "total": 0,
    "status": "idle",
    "message": ""
}
refresh_lock = threading.Lock()

# Path to JSON data
DATA_FILE = 'sp500_data.json'

def sanitize_data(obj):
    """Recursively replace NaN values with None for JSON serializability."""
    if isinstance(obj, dict):
        return {k: sanitize_data(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [sanitize_data(x) for x in obj]
    elif isinstance(obj, float):
        if pd.isna(obj):
            return None
    return obj

def load_sp500_data():
    """Load S&P 500 stock data from JSON file."""
    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, 'r') as f:
                data = json.load(f)
            return data
        except Exception as e:
            print(f"Error loading {DATA_FILE}: {e}")
            return []
    return []

def calculate_sector_data(data):
    """Aggregate data by sector."""
    if not data:
        return []
    
    df = pd.DataFrame(data)
    if df.empty or 'Sector' not in df.columns:
        return []

    sectors = []
    for sector_name, group in df.groupby("Sector"):
        if sector_name == "N/A":
            continue

        total_mcap = sum([x for x in group['Market Cap'] if x is not None])

        # Weighted P/E
        pe_group = group.dropna(subset=['P/E Ratio', 'Market Cap'])
        if not pe_group.empty and total_mcap > 0:
            weighted_pe = (pe_group['P/E Ratio'] * pe_group['Market Cap']).sum() / pe_group['Market Cap'].sum()
        else:
            weighted_pe = None

        avg_50d = group['50D MA'].mean()
        avg_200d = group['200D MA'].mean()
        avg_trend = group['Trend Strength'].mean()
        avg_ret_5d = group.get('5D Return', pd.Series([None])).mean()
        avg_ret_1m = group.get('1M Return', pd.Series([None])).mean()
        avg_ret_6m = group.get('6M Return', pd.Series([None])).mean()

        sectors.append({
            "Sector": sector_name,
            "Market Cap": total_mcap,
            "Weighted P/E": round(weighted_pe, 2) if weighted_pe and not pd.isna(weighted_pe) else None,
            "Avg 50D MA": round(avg_50d, 2) if avg_50d and not pd.isna(avg_50d) else None,
            "Avg 200D MA": round(avg_200d, 2) if avg_200d and not pd.isna(avg_200d) else None,
            "Avg Trend Strength": round(avg_trend, 2) if avg_trend is not None and not pd.isna(avg_trend) else None,
            "Avg 5D Return": round(avg_ret_5d, 2) if avg_ret_5d is not None and not pd.isna(avg_ret_5d) else None,
            "Avg 1M Return": round(avg_ret_1m, 2) if avg_ret_1m is not None and not pd.isna(avg_ret_1m) else None,
            "Avg 6M Return": round(avg_ret_6m, 2) if avg_ret_6m is not None and not pd.isna(avg_ret_6m) else None,
            "Stock Count": len(group)
        })
    return sectors

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/sector')
def sector_page():
    return render_template('sector.html')

@app.route('/earnings')
def earnings_page():
    return render_template('earnings.html')

@app.route('/stock/<symbol>')
def stock_detail(symbol):
    return render_template('stock_detail.html', symbol=symbol.upper())

@app.route('/market-news')
def market_news_page():
    return render_template('market_news.html')

@app.route('/api/data')
def api_data():
    data = load_sp500_data()
    return jsonify(sanitize_data(data))

@app.route('/api/sector_data')
def api_sector_data():
    data = load_sp500_data()
    sector_data = calculate_sector_data(data)
    return jsonify(sanitize_data(sector_data))

@app.route('/api/history/<symbol>')
def api_history(symbol):
    """Return historical prices from Yahoo Finance for the chart."""
    try:
        stock = yf.Ticker(symbol)
        # Fetch 1 year of daily data
        hist = stock.history(period="1y")
        
        if hist is None or hist.empty:
            return jsonify({'error': 'No history found'}), 404
            
        # Format for Chart.js
        # Check if 'Close' exists
        if 'Close' not in hist.columns:
            return jsonify({'error': 'Close price data missing'}), 404

        prices = hist['Close'].tolist()
        dates = hist.index.strftime('%Y-%m-%d').tolist()
        
        return jsonify({
            'symbol': symbol,
            'prices': [round(p, 2) for p in prices],
            'dates': dates
        })
    except Exception as e:
        print(f"Error fetching history for {symbol}: {e}")
        return jsonify({'error': str(e)}), 500

def parse_yf_news(news_list):
    """Helper to parse yfinance news list into a consistent format."""
    if not news_list:
        return []
        
    formatted = []
    for item in news_list:
        if not item or not isinstance(item, dict):
            continue
            
        content = item.get("content", {})
        if not content:
            content = {}
            
        # Title
        title = content.get("title") or item.get("title")
        
        # Link - Robust nested check
        link_obj = content.get("clickThroughUrl")
        link = (link_obj.get("url") if isinstance(link_obj, dict) else None) or item.get("link")
        
        # Publisher - Robust nested check
        provider_obj = content.get("provider")
        publisher = (provider_obj.get("displayName") if isinstance(provider_obj, dict) else None) or item.get("publisher")
        
        # Date - try content.pubDate (ISO string), then providerPublishTime (epoch)
        pub_date_str = content.get("pubDate") or content.get("displayTime")
        pub_time = item.get("providerPublishTime")
        
        # If we only have pub_time (epoch), convert to ISO string
        if not pub_date_str and pub_time:
            try:
                pub_date_str = datetime.datetime.fromtimestamp(pub_time).isoformat()
            except:
                pass
        
        formatted.append({
            "title": title,
            "link": link,
            "publisher": publisher,
            "source": publisher, # Legacy
            "pubDate": pub_date_str,
            "providerPublishTime": pub_time or 0
        })
    return formatted

@app.route('/api/news/<symbol>')
def api_stock_news(symbol):
    """API endpoint for stock news, expected by some frontend components."""
    try:
        ticker = yf.Ticker(symbol)
        news = ticker.news
        if not news:
            return jsonify([])
        return jsonify(parse_yf_news(news))
    except Exception as e:
        print(f"Error fetching news for {symbol}: {e}")
        return jsonify({"error": str(e)}), 500


# News route for symbol
@app.route('/news/<symbol>')
def stock_news_page(symbol):
    return render_template('stock_news.html', symbol=symbol)


# -----------------------------------
# MOVING AVERAGES: SINGLE HISTORY CALL
# -----------------------------------

def get_sp500_tickers():
    """Scrape S&P 500 tickers from Wikipedia using robust manual parsing."""
    try:
        headers = {
            "User-Agent": "Mozilla/5.0"
        }
        url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        table = soup.find('table', {'id': 'constituents'})
        
        if not table:
            print("Could not find table with id 'constituents'")
            return []

        tickers = []
        rows = table.find_all('tr')
        # Skip header row
        for row in rows[1:]:
            cells = row.find_all('td')
            if len(cells) > 0:
                # The first cell is usually the Symbol
                symbol = cells[0].text.strip()
                # Clean symbol (e.g. BRK.B -> BRK-B)
                sym = symbol.replace('.', '-')
                tickers.append(sym)
        
        print(f"Manually scraped {len(tickers)} tickers.")
        return tickers
    except Exception as e:
        print(f"Error fetching tickers: {e}")
        return []

def get_stock_info(symbol, delay=1.5):
    """Fetch fundamental data for a single ticker with rate limiting."""
    try:
        time.sleep(delay)
        stock = yf.Ticker(symbol)
        info = stock.info
        
        return {
            "Ticker": symbol,
            "Name": info.get("shortName", "N/A"),
            "Sector": info.get("sector", "N/A"),
            "Industry": info.get("industry", "N/A"),
            "Market Cap": info.get("marketCap"),
            "P/E Ratio": info.get("trailingPE"),
            "Price": info.get("currentPrice") or info.get("regularMarketPrice"),
            "EarningsDate": extract_earnings_date(stock, info)
        }
    except Exception as e:
        print(f"Error fetching info for {symbol}: {e}")
        return {
            "Ticker": symbol,
            "Name": "N/A",
            "Sector": "N/A",
            "Industry": "N/A",
            "Market Cap": None,
            "P/E Ratio": None,
            "Price": None,
            "EarningsDate": None
        }

def extract_earnings_date(ticker_obj, info):
    """Robustly extract the next earnings date from yfinance data."""
    # 1. Try info timestamps
    for key in ['earningsTimestampStart', 'earningsTimestamp', 'earningsCallTimestampStart']:
        ts = info.get(key)
        if ts:
            try:
                # If it's a list (sometimes happens), take first
                if isinstance(ts, list): ts = ts[0]
                dt = datetime.datetime.fromtimestamp(ts)
                return dt.strftime('%Y-%m-%d')
            except:
                continue

    # 2. Try calendar
    try:
        cal = ticker_obj.calendar
        if cal and 'Earnings Date' in cal:
            edates = cal['Earnings Date']
            if edates and isinstance(edates, list) and len(edates) > 0:
                return edates[0].strftime('%Y-%m-%d')
    except:
        pass
    
    return None

def get_batch_stock_info(symbols, delay=1.5):
    """Fetch fundamental data for a batch of tickers."""
    batch_results = []
    # yfinance Tickers object allows batch operations
    tickers_obj = yf.Tickers(" ".join(symbols))
    
    for symbol in symbols:
        try:
            # Note: yfinance still performs individual info calls under the hood 
            # for the .info property, but this groups the logic into batches of 20
            # as requested by the user.
            info = tickers_obj.tickers[symbol].info
            batch_results.append({
                "Ticker": symbol,
                "Name": info.get("shortName", "N/A"),
                "Sector": info.get("sector", "N/A"),
                "Industry": info.get("industry", "N/A"),
                "Market Cap": info.get("marketCap"),
                "P/E Ratio": info.get("trailingPE"),
                "Price": info.get("currentPrice") or info.get("regularMarketPrice"),
                "EarningsDate": extract_earnings_date(tickers_obj.tickers[symbol], info)
            })
        except Exception as e:
            print(f"Error fetching fundamental info for {symbol}: {e}")
            batch_results.append({
                "Ticker": symbol,
                "Name": "N/A",
                "Sector": "N/A",
                "Industry": "N/A",
                "Market Cap": None,
                "P/E Ratio": None,
                "Price": None,
                "EarningsDate": None
            })
    
    # Delay after processing the batch
    time.sleep(delay)
    return batch_results


# -----------------------------------
# BULK DOWNLOAD & PROCESSING
# -----------------------------------

def fetch_and_update_data():
    """Orchestrates the split fetching process using bulk download."""
    global refresh_status
    
    try:
        with refresh_lock:
            refresh_status["is_running"] = True
            refresh_status["status"] = "starting"
            refresh_status["message"] = "Starting data refresh..."
            refresh_status["current"] = 0
            refresh_status["total"] = 0

        print("Starting data refresh...")
        tickers = get_sp500_tickers()
        
        with refresh_lock:
            refresh_status["total"] = len(tickers)
            refresh_status["status"] = "fetching_ohlcv"
            refresh_status["message"] = f"Found {len(tickers)} tickers. Fetching OHLCV data..."

        print(f"Found {len(tickers)} tickers. Fetching OHLCV data in bulk...")
        
        ma_rows = []
        
        # Batch download to be safe (e.g. 50 tickers at a time)
        batch_size = 50
        for i in range(0, len(tickers), batch_size):
            batch = tickers[i:i+batch_size]
            print(f"Downloading batch {i//batch_size + 1}/{(len(tickers)-1)//batch_size + 1} ({len(batch)} tickers)...")
            
            with refresh_lock:
                refresh_status["message"] = f"Downloading batch {i//batch_size + 1}..."

            try:
                # Download 1y history for the batch
                data = yf.download(batch, period="1y", group_by='ticker', progress=False)
                
                # Iterate through the batch tickers and calculate MAs
                for symbol in batch:
                    try:
                        if len(batch) > 1:
                            if symbol not in data.columns.levels[0]:
                                continue
                            stock_hist = data[symbol]
                        else:
                            stock_hist = data
                        
                        close = stock_hist['Close'].dropna()
                        volume = stock_hist['Volume'].dropna()
                        if len(close) < 200 or len(volume) < 20:
                            continue
                            
                        # Price Metrics
                        ma_50d = close.tail(50).mean()
                        ma_200d = close.tail(200).mean()
                        
                        ret_1d = ((close.iloc[-1] / close.iloc[-2]) - 1) * 100 if len(close) >= 2 else None
                        ret_5d = ((close.iloc[-1] / close.iloc[-6]) - 1) * 100 if len(close) >= 6 else None
                        ret_1m = ((close.iloc[-1] / close.iloc[-21]) - 1) * 100 if len(close) >= 21 else None
                        ret_6m = ((close.iloc[-1] / close.iloc[-126]) - 1) * 100 if len(close) >= 126 else None

                        # 6M Volatility (Annualized StdDev of daily returns)
                        if len(close) >= 126:
                            daily_rets = close.tail(126).pct_change().dropna()
                            vol_6m = daily_rets.std() * (252**0.5) * 100
                        else:
                            vol_6m = None

                        # Volume Metrics
                        # 1D Volume / Avg(20D Volume) - 1
                        # 5D Volume / Avg(20D Volume) - 1
                        avg_vol_20d = volume.tail(20).mean()
                        if isinstance(avg_vol_20d, pd.Series): avg_vol_20d = avg_vol_20d.iloc[0]
                        avg_vol_20d = float(avg_vol_20d)

                        current_vol = volume.iloc[-1]
                        if isinstance(current_vol, pd.Series): current_vol = current_vol.iloc[0]
                        current_vol = float(current_vol)

                        avg_vol_5d = volume.tail(5).mean()
                        if isinstance(avg_vol_5d, pd.Series): avg_vol_5d = avg_vol_5d.iloc[0]
                        avg_vol_5d = float(avg_vol_5d)

                        vol_chg_1d = (current_vol / avg_vol_20d - 1) * 100 if avg_vol_20d > 0 else None
                        vol_chg_5d = (avg_vol_5d / avg_vol_20d - 1) * 100 if avg_vol_20d > 0 else None

                        ma_rows.append({
                            "Symbol": symbol,
                            "50D MA": round(ma_50d, 2),
                            "200D MA": round(ma_200d, 2),
                            "1D Return": round(ret_1d, 2) if ret_1d is not None else None,
                            "5D Return": round(ret_5d, 2) if ret_5d is not None else None,
                            "1M Return": round(ret_1m, 2) if ret_1m is not None else None,
                            "6M Return": round(ret_6m, 2) if ret_6m is not None else None,
                            "6M Volatility": round(vol_6m, 2) if vol_6m is not None else None,
                            "Volume": int(current_vol),
                            "Vol Change 1D": round(vol_chg_1d, 2) if vol_chg_1d is not None else None,
                            "Vol Change 5D": round(vol_chg_5d, 2) if vol_chg_5d is not None else None
                        })
                    except Exception as e:
                        print(f"Error calculating MA for {symbol}: {e}")
            except Exception as e:
                print(f"Batch download failed: {e}")
            time.sleep(1)

        if not ma_rows:
            with refresh_lock:
                refresh_status["is_running"] = False
                refresh_status["status"] = "error"
                refresh_status["message"] = "No MA data found."
            return 0

        ma_df = pd.DataFrame(ma_rows)
        print(f"Calculated MAs for {len(ma_df)} stocks. Fetching Fundamentals...")

        with refresh_lock:
            refresh_status["status"] = "fetching_fundamentals"
            refresh_status["total"] = len(ma_df)

        info_rows = []
        symbols = ma_df["Symbol"].tolist()
        batch_size = 20
        total = len(symbols)
        
        for i in range(0, total, batch_size):
            batch = symbols[i : i + batch_size]
            print(f"[{min(i + batch_size, total)}/{total}] Fetching fundamental batch...")
            
            with refresh_lock:
                refresh_status["current"] = i
                refresh_status["message"] = f"Fetching fundamentals: {i}/{total}"

            batch_info = get_batch_stock_info(batch, delay=1.5)
            info_rows.extend(batch_info)
        
        info_df = pd.DataFrame(info_rows)

        final_df = (
            ma_df
            .merge(info_df, left_on="Symbol", right_on="Ticker", how="left")
            .drop(columns=["Ticker"])
        )

        # Fallback for EarningsDate if not in info
        # Some yfinance versions put it in .calendar
        print("Scraping additional earnings data for stocks with missing dates...")
        for idx, row in final_df.iterrows():
            if not row.get('EarningsDate'):
                try:
                    # Only do this for a subset or if missing to avoid too many calls
                    # but for now let's try to get it if it's a key stock or just try a few
                    pass # We will rely on info mostly, or we could add a specific fetcher later
                except:
                    pass
        
        final_df['Trend Strength'] = final_df.apply(
            lambda row: round(((row['50D MA'] / row['200D MA']) - 1) * 100, 2) 
            if row['200D MA'] and row['200D MA'] != 0 else None, axis=1
        )
        
        # --- QUANTITATIVE TRADING ENGINE ---
        print("Calculating sector medians and trading scores...")
        
        # Calculate Sector Medians for P/E and Volatility
        sector_pe_medians = final_df.groupby('Sector')['P/E Ratio'].median().to_dict()
        sector_vol_medians = final_df.groupby('Sector')['6M Volatility'].median().to_dict()
        
        def calculate_score(row, sector_pe_medians, sector_vol_medians):
            try:
                score = 0
                
                # A) TREND (0–35 points)
                price = row.get('Price') or 0
                ma50 = row.get('50D MA') or 0
                ma200 = row.get('200D MA') or 0
                trend_strength = row.get('Trend Strength') or 0
                
                if price >= ma200:
                    if price > ma50: score += 8
                    if ma50 > ma200: score += 8
                    if price > ma200: score += 8
                    if trend_strength > 0: score += 11
                else:
                    score += 0 # Trend points = 0 if below 200D MA
                
                # B) MOMENTUM (0–25 points)
                ret5d = row.get('5D Return') or 0
                ret1m = row.get('1M Return') or 0
                ret6m = row.get('6M Return') or 0
                
                if ret6m > 0: score += 5
                if ret1m > 0: score += 5
                if ret5d > 0: score += 5
                if ret6m > ret1m > ret5d: score += 10
                if ret5d < 0 and ret1m > 0: score -= 5
                
                # C) VOLUME (0–20 points)
                ret1d = row.get('1D Return') or 0
                vol1d_ratio = 1 + (row.get('Vol Change 1D') or 0) / 100
                vol5d_ratio = 1 + (row.get('Vol Change 5D') or 0) / 100
                
                if vol5d_ratio >= 1.2: score += 5
                if vol1d_ratio >= 1.5: score += 10
                
                # Rising volume on up days
                if ret1d > 0 and (row.get('Vol Change 1D') or 0) > 0:
                    score += 5
                # Down-day volume >= 2x avg
                if ret1d < 0 and vol1d_ratio >= 2.0:
                    score -= 10
                
                # D) RISK & VALUATION (0–20 points)
                mcap = row.get('Market Cap') or 0
                if mcap > 10e9: score += 5
                
                pe = row.get('P/E Ratio')
                sector = row.get('Sector')
                median_pe = sector_pe_medians.get(sector)
                
                if pe and median_pe:
                    if pe < median_pe:
                        score += 5
                    elif pe > median_pe and (ret1m > 0 or ret6m > 0):
                        score += 3
                
                # NEW: Volatility (6M) < sector median
                vol6m = row.get('6M Volatility')
                median_vol = sector_vol_medians.get(sector)
                if vol6m and median_vol and vol6m < median_vol:
                    score += 7
                
                final_points = round(max(0, score))

                # --- DECISION STEPS (1-7) ---
                
                # STEP 1: UNIVERSAL FILTER (HARD REJECTION)
                avg_vol_20d = row.get('Volume') / (1 + (row.get('Vol Change 1D') or 0) / 100) if row.get('Volume') and row.get('Vol Change 1D') is not None else 0
                if mcap < 2e9 or avg_vol_20d < 500000 or price < 10:
                    return final_points, "Rejected – Universal Filter"

                # STEP 2: HARD SELL (CAPITAL PROTECTION)
                # Rules: Price < 200D MA, Down-day vol >= 2x, Trend strength < 0, Score < 45
                if price < ma200 or (ret1d < 0 and vol1d_ratio >= 2.0) or trend_strength < 0 or final_points < 45:
                    return final_points, "Sell"

                # STEP 3: EARNINGS RISK FREEZE
                edate_str = row.get('EarningsDate')
                if edate_str:
                    try:
                        edate = datetime.datetime.strptime(edate_str, '%Y-%m-%d').date()
                        today = datetime.date.today()
                        days_diff = (edate - today).days
                        if -1 <= days_diff <= 7:
                            return final_points, "Hold"
                    except: pass

                # STEP 4: REDUCE (RISK CONTROL)
                # Rules: Score dropped (proxy < 65) OR BOTH 5D < 0 AND 1M < 0
                if (final_points < 65) or (ret5d < 0 and ret1m < 0):
                    return final_points, "Reduce"

                # STEP 5: STRONG BUY (ADD AGGRESSIVELY)
                if (final_points >= 80 and price > ma50 and price > ma200 and vol5d_ratio >= 1.2 and ret6m > 0):
                    edate_dist = 999
                    if edate_str:
                        try:
                            edate_dist = (datetime.datetime.strptime(edate_str, '%Y-%m-%d').date() - datetime.date.today()).days
                        except: pass
                    if edate_dist > 7:
                        return final_points, "Strong Buy"

                # STEP 6: BUY (SMALL / ADD CAUTIOUSLY)
                if (70 <= final_points <= 79 and price > ma50 and price > ma200 and ret6m > 0):
                    edate_dist = 999
                    if edate_str:
                        try:
                            edate_dist = (datetime.datetime.strptime(edate_str, '%Y-%m-%d').date() - datetime.date.today()).days
                        except: pass
                    if edate_dist > 7:
                        return final_points, "Buy (Small)"

                # STEP 7: HOLD (DEFAULT)
                return final_points, "Hold"

            except Exception as e:
                print(f"Error scoring {row.get('Symbol')}: {e}")
                return 0, "ERROR"

        scores_decisions = final_df.apply(lambda r: calculate_score(r, sector_pe_medians, sector_vol_medians), axis=1)
        final_df['Score'] = [s[0] for s in scores_decisions]
        final_df['Trade Decision'] = [s[1] for s in scores_decisions]

        final_df['LastUpdated'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        all_data = final_df.where(pd.notnull(final_df), None).to_dict(orient='records')

        with open(DATA_FILE, 'w') as f:
            json.dump(all_data, f, indent=2)
        
        print(f"Saved {len(all_data)} stocks to {DATA_FILE}")
        
        with refresh_lock:
            refresh_status["is_running"] = False
            refresh_status["status"] = "success"
            refresh_status["current"] = total
            refresh_status["message"] = f"Successfully refreshed {len(all_data)} stocks."
            
        return len(all_data)
        
    except Exception as e:
        print(f"Refresh task failed: {e}")
        with refresh_lock:
            refresh_status["is_running"] = False
            refresh_status["status"] = "error"
            refresh_status["message"] = f"Error: {str(e)}"
        return 0

@app.route('/api/refresh')
def api_refresh():
    """Trigger data refresh (Asynchronous)."""
    global refresh_status
    force = request.args.get('force', 'false').lower() == 'true'
    
    with refresh_lock:
        if refresh_status["is_running"]:
            return jsonify({
                "status": "error", 
                "message": "Refresh already in progress.",
                "details": refresh_status
            }), 400

    # Check if we already have data for today
    if not force:
        data = load_sp500_data()
        if data and len(data) > 0:
            last_updated = data[0].get('LastUpdated')
            if last_updated:
                # Extract date part 2026-02-22 from 2026-02-22 19:10:13
                last_date = last_updated.split(' ')[0]
                today = datetime.date.today().isoformat()
                if last_date == today:
                    return jsonify({
                        "status": "success", 
                        "message": "Data is already up to date for today. Use ?force=true to override.",
                        "count": len(data)
                    }), 200

    # Start refresh in background
    thread = threading.Thread(target=fetch_and_update_data)
    thread.daemon = True
    thread.start()
    
    return jsonify({
        "status": "success", 
        "message": "Data refresh started in background."
    }), 202

@app.route('/api/refresh_status')
def api_refresh_status():
    """Return the current refresh status."""
    return jsonify(refresh_status)

def _get_clean_filing_lines(url):
    """Fetch a SEC filing, strip XBRL/metadata, return clean text lines."""
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
    }
    response = requests.get(url, headers=headers, timeout=30)
    if response.status_code != 200:
        return []
    soup = BeautifulSoup(response.text, 'html.parser')
    # Remove XBRL inline tags
    for tag in soup.find_all(True):
        if ':' in tag.name:
            tag.decompose()
    text = soup.get_text(separator='\n')
    lines = text.split('\n')
    clean = []
    for line in lines:
        s = line.strip()
        if not s or len(s) < 3:
            continue
        # Skip XBRL metadata lines
        if s.startswith('http://') or s.startswith('false') or s.startswith('true'):
            continue
        if 'xbrli:' in s or 'us-gaap/' in s or ':pure' in s or 'iso4217' in s:
            continue
        clean.append(s)
    return clean

def _find_section(lines, heading_patterns, stop_patterns, max_chars=12000):
    """Find a filing section by its heading, validating real content follows."""
    candidates = []
    for i, line in enumerate(lines):
        s = line.strip()
        for pat in heading_patterns:
            if re.match(pat, s, re.IGNORECASE):
                # Verify substantial text follows (not a ToC entry)
                text_after = ''
                for j in range(1, min(20, len(lines) - i)):
                    text_after += lines[i + j].strip() + ' '
                if len(text_after) > 200:
                    candidates.append((i + 1, len(text_after)))
                break
    if not candidates:
        return ""
    # Pick the candidate with the most text after it
    best_idx = max(candidates, key=lambda x: x[1])[0]
    collected = []
    total_chars = 0
    for i in range(best_idx, len(lines)):
        s = lines[i].strip()
        for spat in stop_patterns:
            if re.match(spat, s, re.IGNORECASE) and len(s) < 100:
                return '\n'.join(collected)
        collected.append(s)
        total_chars += len(s)
        if total_chars > max_chars:
            break
    return '\n'.join(collected)

def _extract_points(text, keywords, limit=5, exclude_phrases=None):
    """Extract meaningful sentences containing keywords, filtering boilerplate."""
    if not text:
        return []
    sentences = re.split(r'(?<=[.!?])\s+', text)
    results = []
    seen = set()
    skip_phrases = [
        'check mark', 'check one', 'accelerated filer', 'emerging growth',
        'exchange act', 'rule 12b', 'form 10-k', 'form 10-q', 'section 13',
        'table of contents', 'page number', 'incorporated by reference',
        'not applicable', 'see note', '☐', '☑', '☒', '¨'
    ]
    if exclude_phrases:
        skip_phrases.extend(exclude_phrases)
    for sent in sentences:
        sent = sent.strip()
        if len(sent) < 50 or len(sent) > 500:
            continue
        lower = sent.lower()
        if any(bp in lower for bp in skip_phrases):
            continue
        # Skip lines that are mostly numbers/tables
        alpha_ratio = sum(c.isalpha() for c in sent) / max(len(sent), 1)
        if alpha_ratio < 0.5:
            continue
        if any(re.search(r'\b' + kw + r'\b', sent, re.IGNORECASE) for kw in keywords):
            if lower not in seen:
                seen.add(lower)
                results.append(sent)
        if len(results) >= limit:
            break
    return results

def extract_comprehensive_insights(symbol):
    """Scrape and parse the latest SEC filing (10-K or 10-Q) using section-aware extraction."""
    try:
        ticker = yf.Ticker(symbol)
        filings = ticker.sec_filings
        if not filings:
            return None

        # Find the latest 10-K or 10-Q
        latest_filing = None
        for f in filings:
            if f['type'] in ['10-K', '10-Q']:
                latest_filing = f
                break
        if not latest_filing:
            return None

        url = latest_filing.get('exhibits', {}).get(latest_filing['type']) or latest_filing.get('edgarUrl')
        if not url:
            return None

        lines = _get_clean_filing_lines(url)
        if not lines:
            return None

        # --- Extract key sections ---
        # Business (Item 1)
        business_text = _find_section(
            lines,
            [r'^BUSINESS\s*OVERVIEW', r'^OUR\s*BUSINESS', r'^GENERAL\s*$',
             r'^BUSINESS\s*$', r'^BUSINESS\s+DESCRIPTION'],
            [r'^RISK\s+FACTORS', r'^PROPERTIES', r'^LEGAL\s+PROCEEDINGS',
             r'^UNRESOLVED', r'^CYBERSECURITY'],
            max_chars=12000
        )

        # Risk Factors (Item 1A)
        risk_text = _find_section(
            lines,
            [r'^RISK\s+FACTORS\.?$', r'^STRATEGIC\s+RISKS'],
            [r'^UNRESOLVED\s+STAFF', r'^PROPERTIES', r'^CYBERSECURITY',
             r'^LEGAL\s+PROCEEDINGS', r'^MINE\s+SAFETY',
             r"^MANAGEMENT.S\s+DISCUSSION"],
            max_chars=15000
        )

        # MD&A (Item 7) — try multiple heading patterns
        mda_text = _find_section(
            lines,
            [r'^CONSOLIDATED\s+RESULTS', r'^OVERVIEW\s*$',
             r'^RESULTS\s+OF\s+OPERATIONS', r'^SEGMENT\s+OPERATIONS',
             r'^EXECUTIVE\s+SUMMARY'],
            [r'^QUANTITATIVE', r'^FINANCIAL\s+STATEMENTS',
             r'^CHANGES\s+IN', r'^CONTROLS', r'^CRITICAL\s+ACCOUNTING'],
            max_chars=15000
        )

        combined = business_text + '\n' + mda_text
        # Full text for tariff search (tariff mentions can appear anywhere)
        full_text = '\n'.join(lines)

        # Negative-sentiment phrases to exclude from opportunities
        neg_exclude = [
            'adversely', 'negatively impact', 'negatively affect',
            'decline', 'could affect', 'may affect',
            'uncertain', 'threat', 'challenged', 'headwind',
            'disruption', 'decrease our revenue', 'increase our costs',
            'could also adversely', 'nonexistent', 'compliance costs',
            'material adverse', 'regulations or changes', 'subject to risk',
            'penalties', 'litigation', 'loss of', 'damage to',
            'bad actors', 'social engineering', 'cybersecurity',
            'data breach', 'ransomware', 'vulnerability'
        ]

        # --- Extract insights from the correct sections ---
        insights = {
            "opportunities": _extract_points(
                combined,
                ['growth', 'opportunity', 'expansion', 'innovation',
                 'strategic', 'new product', 'new market', 'demand',
                 'ramp', 'increase', 'momentum', 'invest', 'capacity',
                 'backlog', 'order book', 'delivered', 'revenue grew',
                 'profit grew', 'margin improvement'],
                limit=5,
                exclude_phrases=neg_exclude
            ),
            "risks": _extract_points(
                risk_text,
                ['could adversely', 'may adversely', 'uncertainty',
                 'challenge', 'decline', 'volatility', 'material adverse',
                 'disruption', 'failure', 'negatively impact'],
                limit=5
            ),
            "tariff_impact": _extract_points(
                full_text,
                ['tariff', 'trade policy', 'import duty',
                 'trade restriction', 'trade war', 'customs duty'],
                limit=5
            ),
            "customers": _extract_points(
                combined,
                ['customer', 'client', 'contract', 'backlog',
                 'order', 'airline', 'defense', 'government'],
                limit=5
            ),
            "one_time": _extract_points(
                combined + '\n' + risk_text,
                ['one-time', 'non-recurring', 'impairment',
                 'restructuring', 'settlement', 'divestiture',
                 'write-off', 'gain on sale', 'separation'],
                limit=5
            )
        }
        return insights
    except Exception as e:
        print(f"Error extracting SEC insights for {symbol}: {e}")
        return None

@app.route('/api/stock_details/<symbol>')
def get_stock_details(symbol):
    """Fetch comprehensive details for a specific stock."""
    symbol = symbol.upper()
    try:
        ticker = yf.Ticker(symbol)
        
        # 1. Financials
        info = ticker.info
        financials = ticker.financials
        quarterly_financials = ticker.quarterly_financials
        
        # Calculate margins and growth
        revenue = info.get('totalRevenue')
        net_income = info.get('netIncomeToCommon')
        profit_margin = info.get('profitMargins')
        
        # 2. News
        formatted_news = []
        try:
            news_raw = ticker.news
            if news_raw:
                formatted_news = parse_yf_news(news_raw[:5])
        except Exception as ne:
            print(f"Error parsing news for {symbol}: {ne}")
        
        # 3. SEC 'Insights' (Dynamic extraction from actual filings)
        business_summary = info.get('longBusinessSummary', "N/A")
        dividend_yield = info.get('dividendYield')
        forward_pe = info.get('forwardPE')
        trailing_pe = info.get('trailingPE')
        
        sec_insights = extract_comprehensive_insights(symbol)
        
        # Calculate valuation score
        valuation_score = "Fair Value"
        if forward_pe and trailing_pe:
            if forward_pe < trailing_pe * 0.8:
                valuation_score = "Undervalued"
            elif forward_pe > trailing_pe * 1.2:
                valuation_score = "Overvalued"

        # Fallback to defaults or simulated logic if scraping fails
        if not sec_insights:
            insights = {
                "opportunities": [f"Expansion in {info.get('sector', 'its sector')} and recovery in margins."],
                "threats": ["Increasing competition and potential macro headwinds."],
                "revenue_drivers": ["Growth in core services and volume increases."],
                "tariff_impact": ["Subject to global trade policies but manageable through regional supply chains."],
                "customers": ["Diverse customer base across multiple geographic regions."],
                "one_time": ["N/A - No major one-time events identified in summary."],
                "valuation": valuation_score
            }
        else:
            sec_insights["valuation"] = valuation_score
            # Map labels for consistency
            insights = {
                "opportunities": sec_insights["opportunities"] if sec_insights["opportunities"] else ["No specific growth opportunities identified in latest filing."],
                "threats": sec_insights["risks"] if sec_insights["risks"] else ["No highlighted risks found in primary text blocks."],
                "revenue_drivers": sec_insights["customers"][:2] + sec_insights["opportunities"][:1], # Blend some drivers
                "tariff_impact": sec_insights["tariff_impact"] if sec_insights["tariff_impact"] else ["No significant mention of tariff impact in the latest filing."],
                "customers": sec_insights["customers"] if sec_insights["customers"] else ["Publicly traded and serving global markets."],
                "one_time": sec_insights["one_time"] if sec_insights["one_time"] else ["No major one-time events mentioned in recent results."],
                "valuation": valuation_score
            }

        # Format financials for the chart
        fin_summary = []
        if financials is not None and not financials.empty:
            try:
                dates = financials.columns[:4] # Last 4 years
                for d in dates:
                    # Check if requested indices exist in financials
                    rev = financials.loc['Total Revenue', d] if 'Total Revenue' in financials.index and d in financials.columns else 0
                    ni = financials.loc['Net Income', d] if 'Net Income' in financials.index and d in financials.columns else 0
                    
                    fin_summary.append({
                        "date": d.strftime('%Y') if hasattr(d, 'strftime') else str(d),
                        "revenue": float(rev) if pd.notnull(rev) else 0,
                        "net_income": float(ni) if pd.notnull(ni) else 0
                    })
            except Exception as fe:
                print(f"Error formatting financials for {symbol}: {fe}")

        return jsonify({
            "ticker": symbol,
            "company_name": info.get('longName', symbol),
            "summary": business_summary,
            "metrics": {
                "market_cap": info.get('marketCap'),
                "pe_ratio": info.get('trailingPE'),
                "forward_pe": forward_pe,
                "profit_margin": profit_margin,
                "revenue": revenue,
                "net_income": net_income,
                "dividend_yield": dividend_yield
            },
            "financials": fin_summary,
            "news": formatted_news,
            "insights": insights
        })
    except Exception as e:
        print(f"Error fetching stock details for {symbol}: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/global_news')
@app.route('/api/news/market')
def get_global_news():
    """Fetch global market news."""
    try:
        # Use SPY news as a proxy for global market news (^GSPC often returns empty)
        mkt = yf.Ticker("SPY")
        news_raw = mkt.news
        if not news_raw:
            return jsonify([])
        return jsonify(parse_yf_news(news_raw[:10]))
    except Exception as e:
        print(f"Error fetching global news: {e}")
        return jsonify([]), 200 # Return empty list rather than 500 for better UI stability

@app.route('/api/earnings_calendar')
def api_earnings_calendar():
    """Return stocks sorted by their upcoming earnings date."""
    data = load_sp500_data()
    if not data:
        return jsonify([])
    
    # Filter for stocks with earnings date and sort them
    earnings_stocks = []
    for s in data:
        # We might need to normalize the date format
        edate = s.get('EarningsDate')
        if edate:
            earnings_stocks.append(s)
    
    # Simple sort by date string (YYYY-MM-DD or similar)
    # If EarningsDate is an epoch or different format, we'd handle it here
    def get_date_val(x):
        d = x.get('EarningsDate')
        if isinstance(d, (int, float)):
            return d
        return str(d)

    earnings_stocks.sort(key=get_date_val)
    
    return jsonify(sanitize_data(earnings_stocks))

if __name__ == '__main__':
    app.run(debug=True)
