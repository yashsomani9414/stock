import pandas as pd
import datetime
import json
import os
import requests
import time
import yfinance as yf
from bs4 import BeautifulSoup
from zoneinfo import ZoneInfo

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
    
def get_market_regime():
    """Determine if market is BULLISH or BEARISH based on SPY vs 200D MA."""
    try:
        print("Fetching Market Regime (SPY)...")
        spy = yf.download("SPY", period="1y", progress=False)
        if spy.empty: return "BULLISH"
        if isinstance(spy.columns, pd.MultiIndex):
            spy.columns = spy.columns.get_level_values(0)
        close = spy['Close'].dropna()
        ma200 = close.tail(200).mean()
        curr = close.iloc[-1]
        regime = "BULLISH" if curr >= ma200 else "BEARISH"
        print(f"Market Regime: {regime} (SPY: {curr:.2f} vs 200MA: {ma200:.2f})")
        return regime
    except Exception as e:
        print(f"Error fetching regime: {e}")
        return "BULLISH"

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
        # Ensure Trend Strength is numeric before averaging
        trend_series = pd.to_numeric(group['Trend Strength'], errors='coerce')
        avg_trend = trend_series.mean()
        
        avg_ret_1d = group.get('1D Return', pd.Series([None])).mean()
        avg_ret_5d = group.get('5D Return', pd.Series([None])).mean()
        avg_ret_1m = group.get('1M Return', pd.Series([None])).mean()
        avg_ret_6m = group.get('6M Return', pd.Series([None])).mean()

        # Decision Breakdown
        decisions = group['Trade Decision'].value_counts().to_dict()
        breakdown = {
            "Strong Buy": decisions.get("Strong Buy", 0),
            "Buy (Small)": decisions.get("Buy (Small)", 0),
            "Hold": decisions.get("Hold", 0),
            "Reduce": decisions.get("Reduce", 0),
            "Sell": decisions.get("Sell", 0),
            "Rejected": decisions.get("Rejected – Universal Filter", 0)
        }

        # --- Aggregate Sector Decision Logic ---
        total_valid = sum([v for k, v in breakdown.items() if k != "Rejected"])
        sector_decision = "Hold"
        
        if total_valid > 0:
            pct_sb = (breakdown["Strong Buy"] / total_valid) * 100
            pct_buy_combined = ((breakdown["Strong Buy"] + breakdown["Buy (Small)"]) / total_valid) * 100
            pct_sell_combined = ((breakdown["Sell"] + breakdown["Reduce"]) / total_valid) * 100
            pct_sell_only = (breakdown["Sell"] / total_valid) * 100
            
            if pct_sb > 10 or pct_buy_combined > 50:
                sector_decision = "Strong Buy"
            elif pct_buy_combined > 30:
                sector_decision = "Buy"
            elif pct_sell_combined > 40 or pct_sell_only > 25:
                sector_decision = "Sell"

        sectors.append({
            "Sector": sector_name,
            "Market Cap": total_mcap,
            "Weighted P/E": round(weighted_pe, 2) if weighted_pe and not pd.isna(weighted_pe) else None,
            "Avg 50D MA": round(avg_50d, 2) if avg_50d and not pd.isna(avg_50d) else None,
            "Avg 200D MA": round(avg_200d, 2) if avg_200d and not pd.isna(avg_200d) else None,
            "Avg Trend Strength": round(avg_trend, 2) if avg_trend is not None and not pd.isna(avg_trend) else None,
            "Avg 1D Return": round(avg_ret_1d, 2) if avg_ret_1d is not None and not pd.isna(avg_ret_1d) else None,
            "Avg 5D Return": round(avg_ret_5d, 2) if avg_ret_5d is not None and not pd.isna(avg_ret_5d) else None,
            "Avg 1M Return": round(avg_ret_1m, 2) if avg_ret_1m is not None and not pd.isna(avg_ret_1m) else None,
            "Avg 6M Return": round(avg_ret_6m, 2) if avg_ret_6m is not None and not pd.isna(avg_ret_6m) else None,
            "Stock Count": len(group),
            "Decision Breakdown": breakdown,
            "Sector Decision": sector_decision
        })
    return sectors

def get_all_potential_tickers():
    """Scrape S&P 500, 400, and 600 tickers from Wikipedia."""
    indices = [
        {"name": "S&P 500", "url": "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies", "id": "constituents"},
        {"name": "S&P 400", "url": "https://en.wikipedia.org/wiki/List_of_S%26P_400_companies", "id": "constituents"}
    ]
    
    all_tickers = set()
    headers = {"User-Agent": "Mozilla/5.0"}
    
    for index in indices:
        try:
            print(f"Fetching tickers for {index['name']}...")
            response = requests.get(index['url'], headers=headers, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            table = soup.find('table', {'id': index['id']})
            if not table:
                continue
            
            for row in table.find_all('tr')[1:]:
                cells = row.find_all('td')
                if len(cells) > 0:
                    ticker = cells[0].text.strip().replace('.', '-')
                    all_tickers.add(ticker)
            time.sleep(1) # Be nice to Wikipedia
        except Exception as e:
            print(f"Error fetching {index['name']} tickers: {e}")
            
    return list(all_tickers)

def extract_earnings_date(ticker_obj, info):
    """Extract next earnings date."""
    for key in ['earningsTimestampStart', 'earningsTimestamp', 'earningsCallTimestampStart']:
        ts = info.get(key)
        if ts:
            try:
                if isinstance(ts, list): ts = ts[0]
                return datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d')
            except: continue
    try:
        cal = ticker_obj.calendar
        if cal and 'Earnings Date' in cal:
            edates = cal['Earnings Date']
            if edates and isinstance(edates, list) and len(edates) > 0:
                return edates[0].strftime('%Y-%m-%d')
    except: pass
    return None

def calculate_rsi(series, period=14):
    """Calculate Relative Strength Index (RSI)."""
    if len(series) < period + 1:
        return None
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs)).iloc[-1]

def get_batch_stock_info(symbols, delay=5.0):
    """Fetch fundamental data for a batch of tickers."""
    batch_results = []
    tickers_obj = yf.Tickers(" ".join(symbols))
    for symbol in symbols:
        try:
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
            print(f"Error info {symbol}: {e}")
            batch_results.append({"Ticker": symbol, "Name": "N/A", "Sector": "N/A", "Industry": "N/A"})
    time.sleep(delay)
    return batch_results

def calculate_score(row, sector_pe_medians, sector_vol_medians, history=None, market_regime="BULLISH"):
    """
    Weighted-Average Scoring Engine (V6)
    =====================================
    Each dimension scores 0–100 independently, then combined via weights.
    No single factor can dominate beyond its allocated weight.
    
    Weights: Trend 30% | Momentum 25% | Valuation 20% | Volume 15% | Safety 10%
    """
    try:
        prev_score = history.get('Score') if history else None
        prev_low = history.get('ConsecutiveLowDays', 0) if history else 0
        
        price = row.get('Price') or 0
        ma50 = row.get('50D MA') or 0
        ma200 = row.get('200D MA') or 0
        trend_strength = row.get('Trend Strength') or 0
        ret1d = row.get('1D Return') or 0
        ret5d = row.get('5D Return') or 0
        ret1m = row.get('1M Return') or 0
        ret6m = row.get('6M Return') or 0
        rsi = row.get('RSI')
        dist_from_ma50 = row.get('DistFromMA50') or 0
        vol1d_ratio = 1 + (row.get('Vol Change 1D') or 0) / 100
        vol5d_ratio = 1 + (row.get('Vol Change 5D') or 0) / 100
        mcap = row.get('Market Cap') or 0
        pe = row.get('P/E Ratio')
        vol6m_raw = row.get('6M Volatility')
        median_pe = sector_pe_medians.get(row.get('Sector'))
        median_vol = sector_vol_medians.get(row.get('Sector'))

        # ═══════════════════════════════════════════════════════════════
        # A) TREND SUB-SCORE (0–100)
        # ═══════════════════════════════════════════════════════════════
        trend_score = 0
        if price > 0 and ma200 > 0:
            pct_above_200 = ((price / ma200) - 1) * 100
            
            # Trend Score: scaled points for being above/near 200MA
            if pct_above_200 >= 0:
                trend_score += min(30, pct_above_200 * 3)  # Full at +10%
            else:
                # Downtrend granularity: Penalty for being deep below 200MA
                # -1% -> 27 pts, -5% -> 15 pts, -10% -> 0 pts
                trend_score += max(0, 30 + (pct_above_200 * 6)) 
            
            # Golden cross: MA50 > MA200
            if ma50 > ma200:
                trend_score += 25
            
            # Price above 50MA
            if price > ma50:
                trend_score += 20
            elif price > ma200:
                trend_score += 5 # Near-term consolidating but above 200MA
            
            # Trend strength (scaled)
            if trend_strength > 0:
                trend_score += min(25, trend_strength * 5)  # Full at +5%

        # ═══════════════════════════════════════════════════════════════
        # B) MOMENTUM SUB-SCORE (0–100)
        # ═══════════════════════════════════════════════════════════════
        mom_score = 0
        
        # Individual return contributions (V7: More realistic thresholds)
        mom_score += min(25, max(0, ret6m * 1.25))   # Full at +20%
        mom_score += min(25, max(0, ret1m * 2.5))    # Full at +10%
        mom_score += min(25, max(0, ret5d * 5.0))    # Full at +5%
        
        # Acceleration bonus: annualized rates for fair comparison
        ann_5d =  ret5d * 52    # 5-day return annualized
        ann_1m =  ret1m * 12    # 1-month return annualized
        ann_6m =  ret6m * 2     # 6-month return annualized
        if ann_5d > ann_1m > ann_6m and ret5d > 0.5:
            mom_score += 25  # True acceleration
        
        # Short-term weakness penalty
        if ret5d < -2 and ret1m > 0:
            mom_score = max(0, mom_score - 15)

        # ═══════════════════════════════════════════════════════════════
        # C) VALUATION SUB-SCORE (0–100)
        # ═══════════════════════════════════════════════════════════════
        val_score = 50  # Start neutral
        
        # P/E relative to sector (up to ±30 pts)
        if pe and median_pe and median_pe > 0:
            pe_ratio = pe / median_pe
            if pe_ratio < 0.7:
                val_score += 30       # Value
            elif pe_ratio < 1.0:
                val_score += (1.0 - pe_ratio) * 100
            elif pe_ratio > 2.0:
                val_score -= 30       # Expensive
            elif pe_ratio > 1.2:
                val_score -= min(25, (pe_ratio - 1.2) * 31)

        # Volatility relative to sector (up to ±20 pts)
        if vol6m_raw and median_vol and median_vol > 0:
            vol_ratio = vol6m_raw / median_vol
            if vol_ratio < 0.8:
                val_score += 20       # Stable
            elif vol_ratio > 1.5:
                val_score -= 20       # High absolute risk
        
        val_score = max(0, min(100, val_score))

        # ═══════════════════════════════════════════════════════════════
        # D) VOLUME SUB-SCORE (0–100)
        # ═══════════════════════════════════════════════════════════════
        vol_score = 40  # Baseline
        
        vol_score += min(30, max(0, (vol5d_ratio - 1.0) * 150))  # Full at 1.2x
        vol_score += min(20, max(0, (vol1d_ratio - 1.0) * 40))   # Full at 1.5x
        
        if ret1d > 0.5 and vol1d_ratio > 1.1:
            vol_score += 10 # Accumulation
        
        if ret5d < -3 and vol5d_ratio >= 1.3:
            vol_score = max(0, vol_score - 40) # Distribution
        
        vol_score = max(0, min(100, vol_score))

        # ═══════════════════════════════════════════════════════════════
        # E) SAFETY SUB-SCORE (0–100)
        # ═══════════════════════════════════════════════════════════════
        safety_score = 70  # Start healthy
        
        if rsi:
            if rsi > 75: safety_score -= 40
            elif rsi > 70: safety_score -= 20
            elif rsi < 35: safety_score += 20
        
        if dist_from_ma50 > 18:  # Widened to 18% for Strong Buy candidates
            safety_score -= 40
        elif dist_from_ma50 > 12:
            safety_score -= 15
        
        safety_score = max(0, min(100, safety_score))

        # ═══════════════════════════════════════════════════════════════
        # WEIGHTED AVERAGE COMPOSITE SCORE
        # ═══════════════════════════════════════════════════════════════
        WEIGHTS = {
            'trend': 0.30,
            'momentum': 0.25,
            'valuation': 0.20,
            'volume': 0.15,
            'safety': 0.10,
        }
        
        composite = (
            trend_score * WEIGHTS['trend'] +
            mom_score * WEIGHTS['momentum'] +
            val_score * WEIGHTS['valuation'] +
            vol_score * WEIGHTS['volume'] +
            safety_score * WEIGHTS['safety']
        )
        
        final_points = min(100, round(max(0, composite)))
        new_low = prev_low + 1 if final_points < 40 else 0

        # ═══════════════════════════════════════════════════════════════
        # V4: RISK-ADJUSTED POSITION SIZING
        # ═══════════════════════════════════════════════════════════════
        vol6m = vol6m_raw or 25
        rec_weight = round(min(5.0, max(1.0, 75 / vol6m)), 1)

        # V4: TRAILING STOP-LOSS TRACKING
        curr_price = price
        prev_highest = (history.get('HighestPrice') or 0) if history else 0
        highest_price = max(curr_price, prev_highest)
        trailing_stop = round(highest_price * 0.90, 2) if highest_price > 0 else 0

        # ═══════════════════════════════════════════════════════════════
        # DECISION LOGIC
        # ═══════════════════════════════════════════════════════════════
        avg_vol_20d = row.get('Volume') / vol1d_ratio if row.get('Volume') and row.get('Vol Change 1D') is not None else 0
        if mcap < 2e9 or avg_vol_20d < 500000 or price < 10:
            return final_points, "Rejected – Universal Filter", new_low, highest_price, trailing_stop, rec_weight

        # SELL: Use 5D metrics to reduce whipsaw
        sell_signals = sum([
            price < ma200,
            ret5d < -7 and vol5d_ratio >= 1.2, # Significant breakdown
            trend_strength < -3,
            new_low >= 5 # Increased from 3
        ])
        if sell_signals >= 2:
            return final_points, "Sell", new_low, highest_price, trailing_stop, rec_weight

        # EARNINGS BLACKOUT
        edate_str = row.get('EarningsDate')
        edate_dist = 999
        if edate_str:
            try:
                pt_today = datetime.datetime.now(ZoneInfo("America/Los_Angeles")).date()
                edate_dist = (datetime.datetime.strptime(edate_str, '%Y-%m-%d').date() - pt_today).days
                if -1 <= edate_dist <= 7:
                    return final_points, "Hold", new_low, highest_price, trailing_stop, rec_weight
            except: pass

        # V4: EXIT FOR PROFIT
        if history and history.get('Trade Decision') in ("Strong Buy", "Buy (Small)"):
            if curr_price < trailing_stop and curr_price > 0:
                return final_points, "Sell (Profit-Lock)", new_low, highest_price, trailing_stop, rec_weight

        # V4: GLOBAL CIRCUIT BREAKER + BUY DECISIONS
        decision = "Hold"
        # V7: Momentum floors to stop falling knives and crash buys
        if final_points >= 70 and price > ma50 and price > ma200 and ret1m > -2 and ret5d > -7:
            if (rsi and rsi < 75) and dist_from_ma50 < 22 and edate_dist > 7:
                decision = "Strong Buy"
        elif final_points >= 55 and price > ma50 and price > ma200 and ret1m > -4 and ret5d > -10:
            if (rsi and rsi < 80) and dist_from_ma50 < 25 and edate_dist > 5:
                decision = "Buy (Small)"

        # Apply Global Circuit Breaker Downgrade
        if market_regime == "BEARISH" and decision in ("Strong Buy", "Buy (Small)"):
            decision = "Hold (Market Risk)"

        # REDUCE
        if decision == "Hold":
            if final_points < 35 and ret5d < -3 and ret1m < -3:
                decision = "Reduce"
            elif prev_score and prev_score >= 65 and final_points < 45:
                decision = "Reduce"

        return final_points, decision, new_low, highest_price, trailing_stop, rec_weight
    except Exception as e:
        print(f"Error scoring: {e}")
        return 0, "ERROR", 0, 0, 0, 1.5

def fetch_and_save():
    print("Starting fetch...")
    regime = get_market_regime()
    tickers = get_all_potential_tickers()
    if not tickers: return
    print(f"Total potential tickers: {len(tickers)}")
    
    ma_rows = []
    batch_size = 10
    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i+batch_size]
        success = False
        for attempt in range(3):
            try:
                data = yf.download(batch, period="1y", group_by='ticker', progress=False)
                if not data.empty:
                    success = True
                    break
            except Exception as e:
                print(f"Download error for batch {i} (attempt {attempt+1}): {e}")
                time.sleep(20)
        
        if success:
            for symbol in batch:
                try:
                    hist = data[symbol] if len(batch) > 1 else data
                    close, volume = hist['Close'].dropna(), hist['Volume'].dropna()
                    if len(close) < 200: continue
                    
                    ma50, ma200 = close.tail(50).mean(), close.tail(200).mean()
                    rsi = calculate_rsi(close)
                    curr_price = close.iloc[-1]
                    dist_ma50 = ((curr_price / ma50) - 1) * 100 if ma50 > 0 else 0
                    
                    ret1d = (close.iloc[-1]/close.iloc[-2]-1)*100 if len(close)>=2 else 0
                    ret5d = (close.iloc[-1]/close.iloc[-6]-1)*100 if len(close)>=6 else 0
                    ret1m = (close.iloc[-1]/close.iloc[-21]-1)*100 if len(close)>=21 else 0
                    ret6m = (close.iloc[-1]/close.iloc[-126]-1)*100 if len(close)>=126 else 0
                    vol6m = close.tail(126).pct_change().std()*(252**0.5)*100 if len(close)>=126 else None
                    
                    avg_v20 = float(volume.tail(20).mean())
                    curr_v = float(volume.iloc[-1])
                    avg_v5 = float(volume.tail(5).mean())
                    
                    ma_rows.append({
                        "Symbol": symbol, 
                        "50D MA": round(ma50, 2), 
                        "200D MA": round(ma200, 2),
                        "RSI": round(rsi, 2) if rsi is not None else None,
                        "DistFromMA50": round(dist_ma50, 2),
                        "1D Return": round(ret1d, 2), "5D Return": round(ret5d, 2),
                        "1M Return": round(ret1m, 2), "6M Return": round(ret6m, 2),
                        "6M Volatility": round(vol6m, 2) if vol6m else None,
                        "Volume": int(curr_v), "Vol Change 1D": round((curr_v/avg_v20-1)*100, 2) if avg_v20>0 else 0,
                        "Vol Change 5D": round((avg_v5/avg_v20-1)*100, 2) if avg_v20>0 else 0
                    })
                except: continue
        time.sleep(5)

    ma_df = pd.DataFrame(ma_rows)
    info_rows = []
    symbols = ma_df["Symbol"].tolist()
    for i in range(0, len(symbols), 50):
        info_rows.extend(get_batch_stock_info(symbols[i:i+50]))
    
    # Filter by Market Cap > $5B
    final_df = ma_df.merge(pd.DataFrame(info_rows), left_on="Symbol", right_on="Ticker", how="left").drop(columns=["Ticker"])
    
    # Filter: Market Cap > 5 Billion
    # Some stocks might have None for Market Cap if info fetch failed
    count_before = len(final_df)
    final_df = final_df[final_df['Market Cap'] > 5e9]
    count_after = len(final_df)
    print(f"Filtered from {count_before} to {count_after} stocks with Market Cap > $5B.")
    
    final_df['Trend Strength'] = final_df.apply(lambda r: round((r['50D MA']/r['200D MA']-1)*100, 2) if r['200D MA'] else 0, axis=1)
    
    existing = []
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE, 'r') as f: existing = json.load(f)
    hist_map = {s['Symbol']: s for s in existing if 'Symbol' in s}
    
    pe_med = final_df.groupby('Sector')['P/E Ratio'].median().to_dict()
    vol_med = final_df.groupby('Sector')['6M Volatility'].median().to_dict()
    
    # Calculate scores and decisions for each stock (V4)
    results = final_df.apply(lambda r: calculate_score(r, pe_med, vol_med, hist_map.get(r['Symbol']), regime), axis=1)
    
    final_df['Score'] = [r[0] for r in results]
    final_df['Trade Decision'] = [r[1] for r in results]
    final_df['ConsecutiveLowDays'] = [r[2] for r in results]
    final_df['HighestPrice'] = [r[3] for r in results]
    final_df['TrailingStop'] = [r[4] for r in results]
    final_df['RecWeight'] = [r[5] for r in results]
    final_df['MarketRegime'] = regime
    
    # Get current time in Pacific Time
    pacific_time = datetime.datetime.now(ZoneInfo("America/Los_Angeles"))
    final_df['LastUpdated'] = pacific_time.strftime("%Y-%m-%d %H:%M:%S")
    
    output = final_df.where(pd.notnull(final_df), None).to_dict(orient='records')
    with open(DATA_FILE, 'w') as f: json.dump(output, f, indent=2)
    print(f"Done. Saved {len(output)} stocks.")

if __name__ == "__main__":
    fetch_and_save()
