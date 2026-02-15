import yfinance as yf

print("Testing yfinance for AAPL...")
try:
    stock = yf.Ticker("AAPL")
    hist = stock.history(period="5d")
    print(f"History:\n{hist}")
    if hist.empty:
        print("History is empty.")
    else:
        print("History fetched successfully.")
except Exception as e:
    print(f"Error: {e}")
