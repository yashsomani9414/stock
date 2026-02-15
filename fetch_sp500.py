import yfinance as yf
import pandas as pd
import json
import time
import datetime
import requests

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
        table = pd.read_html(response.text)
        df = table[0]
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
    symbol = ticker_obj['Symbol']
    name = ticker_obj['Name']
    sector = ticker_obj.get('Sector', 'N/A')
    industry = ticker_obj.get('Industry', 'N/A')

    try:
        stock = yf.Ticker(symbol)
        history = stock.history(period="210d")
        if history.empty or len(history) < 50:
            return None

        close = history["Close"]
        current_price = close.iloc[-1]
        ma_50d = close.tail(50).mean()
        ma_200d = close.tail(200).mean() if len(close) >= 200 else None
        trend_strength = ((ma_50d / ma_200d) - 1) if ma_200d else None

        try:
            pe_ratio = stock.info.get("trailingPE")
            market_cap = stock.info.get("marketCap")
        except:
            pe_ratio, market_cap = None, None

        return {
            "Symbol": symbol,
            "Name": name,
            "Price": round(current_price, 2),
            "50D MA": round(ma_50d, 2),
            "200D MA": round(ma_200d, 2) if ma_200d else None,
            "Trend Strength": round(trend_strength * 100, 2) if trend_strength else None,
            "P/E Ratio": round(pe_ratio, 2) if pe_ratio else None,
            "Market Cap": market_cap,
            "Sector": sector,
            "Industry": industry,
            "LastUpdated": datetime.date.today().isoformat()
        }
    except Exception as e:
        print(f"{symbol}: No price data found. Reason: {e}")
        return None

def fetch_all():
    tickers = get_sp500_tickers()
    print(f"Fetching {len(tickers)} tickers...")
    all_data = []

    batch_size = 5
    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i+batch_size]
        for t in batch:
            data = get_stock_data(t)
            if data:
                all_data.append(data)
            time.sleep(0.25)  # rate limit delay
        print(f"Fetched batch {i//batch_size + 1}")

    with open(DATA_FILE, 'w') as f:
        json.dump(all_data, f, indent=2)
    print(f"Saved {len(all_data)} stocks to {DATA_FILE}")

if __name__ == "__main__":
    fetch_all()
