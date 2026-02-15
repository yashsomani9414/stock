import yfinance as yf
import time
import random

tickers = ["AAPL", "MSFT", "GOOG", "AMZN", "TSLA"]  # Test with major stocks
print(f"Testing {len(tickers)} stocks...")

for symbol in tickers:
    print(f"Fetching {symbol}...")
    try:
        stock = yf.Ticker(symbol)
        hist = stock.history(period="5d")
        if hist.empty:
            print(f"❌ {symbol}: Empty history")
        else:
            print(f"✅ {symbol}: Fetched {len(hist)} days. Price: {hist['Close'].iloc[-1]}")
    except Exception as e:
        print(f"❌ {symbol}: Error {e}")
    
    time.sleep(1)
