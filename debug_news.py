import yfinance as yf
import json

print("Fetching news for AAPL...")
try:
    aapl = yf.Ticker("AAPL")
    news = aapl.news
    print(json.dumps(news, indent=2))
except Exception as e:
    print(f"Error fetching AAPL news: {e}")

print("\nFetching news for Market (^GSPC)...")
try:
    mkt = yf.Ticker("^GSPC")
    news = mkt.news
    print(json.dumps(news, indent=2))
except Exception as e:
    print(f"Error fetching Market news: {e}")
